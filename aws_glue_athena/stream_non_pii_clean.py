import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, col, when, to_timestamp, lower, regexp_replace, lit, trim
from pyspark.sql.types import StringType

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# S3 requester pays configuration
print("Setting S3 requester pays configuration")
sc._jsc.hadoopConfiguration().set("fs.s3.requester.pays.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3.requester.pays.mode", "requester")
sc._jsc.hadoopConfiguration().set("fs.s3a.requester.pays.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3n.requester.pays.enabled", "true")

# Get job arguments
args_list = sys.argv
resolved_args = {}
if '--target_bucket' in args_list:
    resolved_args = getResolvedOptions(args_list, ['JOB_NAME', 'target_bucket'])
else:
    resolved_args = getResolvedOptions(args_list, ['JOB_NAME'])
    # Default target bucket if not provided
    resolved_args['target_bucket'] = 'peak-fitness-processed-data-0503'

job_name = resolved_args['JOB_NAME']
target_bucket = resolved_args['target_bucket']
job.init(job_name, resolved_args)

bucket_path = f"s3://{target_bucket}"
# Process only non-pii data
input_path_base = f"{bucket_path}/peak-fitness-stream/non-pii" # Renamed to avoid conflict with local var in function
# Output to non-pii directory
output_path_base = f"{bucket_path}/cleaned-data/non-pii" # Renamed

# --- Incremental Load: Processed folder tracking ---
log_bucket_name = target_bucket # Using the same bucket for logs
log_key = "etl_logs/non_pii_processed_folders.txt" # Log file for tracking processed non-pii folders
s3_client = boto3.client('s3')

processed_folders = set()
try:
    print(f"Loading processed folder list from s3://{log_bucket_name}/{log_key}")
    response = s3_client.get_object(Bucket=log_bucket_name, Key=log_key)
    processed_folders = set(response['Body'].read().decode('utf-8').splitlines())
    print(f"Found {len(processed_folders)} previously processed folders.")
except s3_client.exceptions.NoSuchKey:
    print(f"No processed folder list found at s3://{log_bucket_name}/{log_key}, assuming first run or new log.")
except Exception as e:
    print(f"Error loading processed folder list from s3://{log_bucket_name}/{log_key}: {str(e)}. Proceeding as if no folders were processed.")
    # Depending on requirements, you might want to raise e here to stop the job if log loading is critical

newly_processed_folders = []
# --- End Incremental Load Setup ---

# Cleaning and transformation logic
def clean_and_enhance_data(df, data_type_base, date_str):
    """
    Cleans and enhances the input DataFrame.

    Args:
        df (DataFrame): Input Spark DataFrame.
        data_type_base (str): Base type of the data (e.g., 'events', 'schedule').
        date_str (str): Date string (YYYYMMDD) extracted from the input path.

    Returns:
        DataFrame: Cleaned and enhanced Spark DataFrame.
    """
    try:
        initial_count = df.count()
        print(f"Initial record count for {data_type_base} ({date_str}): {initial_count}")

        # Remove duplicates and rows where all values are null
        df = df.dropDuplicates()
        df = df.na.drop(how="all")

        # Standardize string columns: lowercase, trim, and fill nulls/empties with 'unknown'
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in string_cols:
            df = df.withColumn(col_name, lower(trim(col(col_name)))) # Add trim here as well
            df = df.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ''), "unknown").otherwise(col(col_name))) # Handle empty strings too

        # Type-specific processing logic
        if data_type_base == "events" and "event_ts" in df.columns:
            print(f"Before conversion - event_ts data type: {df.schema['event_ts'].dataType}")
            print("Attempting to convert event_ts to timestamp type...")
            # Explicitly cast to string first for safety before conversion
            df = df.withColumn("event_ts_str", col("event_ts").cast(StringType()))
            # Convert to timestamp using the expected format
            df = df.withColumn("event_ts",
                               to_timestamp(col("event_ts_str"), "yyyy-MM-dd'T'HH:mm:ss"))
            df = df.drop("event_ts_str") # Drop the intermediate string column
            df = df.withColumn("event_date", col("event_ts").cast("date"))
            print(f"After conversion - event_ts data type: {df.schema['event_ts'].dataType}")
            print("Sample event_ts values after conversion:")
            df.select("event_ts").where(col("event_ts").isNotNull()).show(5, False)
            print("Sample event_ts values that failed conversion (became null):")
            df.select("event_ts").where(col("event_ts").isNull()).limit(5).show(5, False)

        elif data_type_base == "schedule":
            if "start_time" in df.columns:
                print("Converting start_time to timestamp type")
                df = df.withColumn("start_time", to_timestamp(col("start_time")))
            if "end_time" in df.columns:
                print("Converting end_time to timestamp type")
                df = df.withColumn("end_time", to_timestamp(col("end_time")))

        elif data_type_base == "users": 
            if "registration_date" in df.columns:
                print("Converting registration_date to timestamp type")
                df = df.withColumn("registration_date", to_timestamp(col("registration_date")))
            if "last_login" in df.columns:
                print("Converting last_login to timestamp type")
                df = df.withColumn("last_login", to_timestamp(col("last_login")))

        df = df.withColumn("processed_timestamp", current_timestamp())

        year_val, month_val, day_val = 2025, 1, 1 
        if len(date_str) == 8 and date_str.isdigit():
            year_val = int(date_str[0:4])
            month_val = int(date_str[4:6])
            day_val = int(date_str[6:8])
            print(f"Extracted date from folder name: year={year_val}, month={month_val}, day={day_val}")
        else:
             print(f"Warning: Could not parse date from folder name part '{date_str}'. Using default date 2025-01-01 for partitioning.")

        df = df.withColumn("year", lit(year_val))
        df = df.withColumn("month", lit(month_val))
        df = df.withColumn("day", lit(day_val))

        if "duration" in df.columns:
             df = df.withColumn("duration", when(col("duration").isNull() | (col("duration") < 0), 0).otherwise(col("duration")))
        if "email" in df.columns:
             pass # email was already lowercased and trimmed
        if "phone_number" in df.columns:
             df = df.withColumn("phone_number", regexp_replace(col("phone_number"), "[^0-9]", ""))

        final_count = df.count()
        print(f"Final record count after cleaning: {final_count}")
        removed_count = initial_count - final_count
        if removed_count > 0:
            print(f"Removed {removed_count} records during cleaning (duplicates/all nulls)")
        return df
    except Exception as e:
        print(f"Error during data cleaning and enhancement for {data_type_base} ({date_str}): {str(e)}")
        raise e

# Main function to process new non-pii data incrementally
def process_incremental_non_pii_data():
    """
    Processes new non-pii data found in the input path incrementally,
    skipping already processed folders based on a log file.
    """
    print(f"Starting incremental processing of non-pii data from: {input_path_base}")
    # s3_client is global
    bucket_name = bucket_path.replace("s3://", "") # bucket_path is global
    # base_input_prefix_for_discovery refers to the part after bucket name, used for S3 listing
    base_input_prefix_for_discovery = input_path_base.replace(f"s3://{bucket_name}/", "")
    if not base_input_prefix_for_discovery.endswith('/'):
        base_input_prefix_for_discovery += '/'

    base_type_dirs = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=base_input_prefix_for_discovery, Delimiter='/')

    print(f"Looking for base type directories under prefix: {base_input_prefix_for_discovery}")
    try:
        for page in pages:
            if 'CommonPrefixes' in page:
                for prefix_info in page.get('CommonPrefixes', []):
                    full_prefix = prefix_info.get('Prefix')
                    if full_prefix:
                        dir_name = full_prefix[len(base_input_prefix_for_discovery):].strip('/')
                        if dir_name:
                             base_type_dirs.add(dir_name)
    except Exception as e:
        print(f"Error listing base directories in s3://{bucket_name}/{base_input_prefix_for_discovery}: {e}")
        raise 

    if not base_type_dirs:
        print(f"No base type directories found under s3://{bucket_name}/{base_input_prefix_for_discovery}. Exiting.")
        return

    print(f"Discovered base type directories: {base_type_dirs}")

    for base_dir_name in base_type_dirs:
        current_base_type_prefix_for_discovery = f"{base_input_prefix_for_discovery}{base_dir_name}/"
        print(f"\n--- Processing base directory: {base_dir_name} ---")
        print(f"Looking for date folders under prefix: {current_base_type_prefix_for_discovery}")

        date_folders_to_process = set()
        pages_date_folders = paginator.paginate(Bucket=bucket_name, Prefix=current_base_type_prefix_for_discovery, Delimiter='/')
        try:
             for page in pages_date_folders:
                 if 'CommonPrefixes' in page:
                     for prefix_info in page.get('CommonPrefixes', []):
                         full_prefix = prefix_info.get('Prefix')
                         if full_prefix:
                             date_folder_name = full_prefix[len(current_base_type_prefix_for_discovery):].strip('/')
                             if date_folder_name:
                                 date_folders_to_process.add(date_folder_name)
        except Exception as e:
             print(f"Error listing date directories in s3://{bucket_name}/{current_base_type_prefix_for_discovery}: {e}")
             print(f"Skipping base directory {base_dir_name} due to listing error.")
             continue

        if not date_folders_to_process:
            print(f"No date-specific folders found under s3://{bucket_name}/{current_base_type_prefix_for_discovery}")
            continue

        print(f"Found date folders for '{base_dir_name}': {date_folders_to_process}")

        for date_folder in date_folders_to_process:
            folder_log_identifier = f"{base_dir_name}/{date_folder}" # e.g. "events/events_20230101"
            
            if folder_log_identifier in processed_folders:
                print(f"Skipping already processed folder: {folder_log_identifier}")
                continue

            parts = date_folder.split('_')
            if len(parts) < 2 or not parts[1].isdigit() or len(parts[1]) != 8:
                print(f"Skipping folder '{date_folder}' in '{base_dir_name}': Not 'type_YYYYMMDD'.")
                continue
            
            data_type_from_folder = parts[0]
            if data_type_from_folder != base_dir_name:
                 print(f"Warning: Folder name type '{data_type_from_folder}' in dir '{base_dir_name}' mismatch. Using dir name '{base_dir_name}'.")
            
            data_type_base_for_cleaning = base_dir_name # Consistent type for cleaning
            date_str = parts[1]

            # Path to read data, e.g., s3://bucket/prefix/events/events_20250101/
            current_input_s3_path = f"s3://{bucket_name}/{current_base_type_prefix_for_discovery}{date_folder}"
            # Output path for this base type, e.g., s3://bucket/cleaned-prefix/non-pii/events
            # Partitions (year, month, day) will be subdirs of this.
            current_output_s3_path = f"{output_path_base}/{base_dir_name}"

            print(f"\nProcessing new data: type '{data_type_base_for_cleaning}', date '{date_str}' from: {current_input_s3_path}")
            try:
                df = spark.read.parquet(current_input_s3_path)
                if df.rdd.isEmpty():
                    print(f"Skipping {current_input_s3_path} - empty dataset")
                    # Still mark as processed if we want to avoid re-checking empty datasets constantly
                    # Or decide based on requirements: if empty means "try again later", don't log.
                    # For now, let's log it as "checked" to avoid re-listing and re-checking.
                    newly_processed_folders.append(folder_log_identifier)
                    print(f"Folder {folder_log_identifier} (empty) marked as processed.")
                    continue

                print(f"Cleaning data for {date_folder}...")
                cleaned_df = clean_and_enhance_data(df, data_type_base_for_cleaning, date_str)

                print("Schema before writing:")
                cleaned_df.printSchema()

                print(f"Writing cleaned data to {current_output_s3_path} (Mode: append, Partitioning: year, month, day)")
                cleaned_df.write \
                    .mode("append") \
                    .partitionBy("year", "month", "day") \
                    .parquet(current_output_s3_path)

                print(f"Successfully processed and wrote data for {folder_log_identifier} to {current_output_s3_path}.")
                newly_processed_folders.append(folder_log_identifier)

            except Exception as e:
                print(f"⚠️ Error processing {current_input_s3_path}: {str(e)}")
                # Decide if a failed folder should be added to log to prevent retries, or left out.
                # For now, not adding to log on failure, so it might be retried next run.
                continue 

# Execute ETL logic
try:
    # spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") # Not for "append"
    print("Job processing mode is 'append'.")

    process_incremental_non_pii_data()

    if newly_processed_folders:
        print(f"Updating processed folders log with {len(newly_processed_folders)} new folder(s): {newly_processed_folders}")
        all_folders_to_log = processed_folders.union(set(newly_processed_folders))
        try:
            s3_client.put_object(
                Bucket=log_bucket_name,
                Key=log_key,
                Body='\n'.join(sorted(list(all_folders_to_log))).encode('utf-8'),
                ContentType='text/plain'
            )
            print(f"Successfully updated processed folders log: s3://{log_bucket_name}/{log_key}")
        except Exception as e:
            print(f"CRITICAL: Error writing updated processed folders log to s3://{log_bucket_name}/{log_key}: {str(e)}")
            # This is a critical error; consider failing the job if the log can't be updated.
            # raise e # Optionally re-raise to fail the Glue job
    else:
        print("No new folders were processed in this run.")

    print("\n✅ Incremental non-pii data processing run completed!")
    job.commit()
except Exception as e:
    print(f"❌ Error in the main incremental non-pii data processing job: {str(e)}")
    raise e
