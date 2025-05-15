import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import current_timestamp, col, when, to_timestamp, lower, regexp_replace, year, month, dayofmonth, lit, trim
from pyspark.sql.types import StringType
from datetime import datetime

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
if '--target_bucket' in sys.argv:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_bucket'])
else:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    args['target_bucket'] = 'peak-fitness-processed-data-0503'

job_name = args['JOB_NAME']
target_bucket = args['target_bucket']
job.init(job_name, args)

bucket_path = f"s3://{target_bucket}"
input_path = f"{bucket_path}/peak-fitness-stream"
output_path = f"{bucket_path}/cleaned-data"

# Processed file tracking
log_bucket = target_bucket
log_key = "etl_logs/cleaned_files_list.txt"
s3_client = boto3.client('s3')

# Load existing processed files
processed_files = set()
try:
    print(f"Loading cleaned file list from s3://{log_bucket}/{log_key}")
    response = s3_client.get_object(Bucket=log_bucket, Key=log_key)
    processed_files = set(response['Body'].read().decode('utf-8').splitlines())
except s3_client.exceptions.NoSuchKey:
    print("No cleaned file list found, assuming first run.")
except Exception as e:
    print(f"Error loading cleaned list: {e}")

newly_processed = []

# Cleaning and transformation logic
def clean_and_enhance_data(df, data_type_base, date_str):
    try:
        initial_count = df.count()
        print(f"Initial record count: {initial_count}")
        for col_name in df.columns:
            print(f"  - {col_name}")

        df = df.dropDuplicates()
        df = df.na.drop(how="all")

        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in string_cols:
            df = df.withColumn(col_name, lower(col(col_name)))
            df = df.withColumn(col_name, when(col(col_name).isNull(), "unknown").otherwise(col(col_name)))

        if data_type_base == "events" and "event_ts" in df.columns:
            df = df.withColumn("event_ts", to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss"))
            df = df.withColumn("event_date", col("event_ts").cast("date"))

        if data_type_base == "schedule":
            if "start_time" in df.columns:
                df = df.withColumn("start_time", to_timestamp(col("start_time")))
            if "end_time" in df.columns:
                df = df.withColumn("end_time", to_timestamp(col("end_time")))

        if data_type_base == "users":
            if "registration_date" in df.columns:
                df = df.withColumn("registration_date", to_timestamp(col("registration_date")))
            if "last_login" in df.columns:
                df = df.withColumn("last_login", to_timestamp(col("last_login")))

        df = df.withColumn("processed_timestamp", current_timestamp())

        year_val = int(date_str[0:4])
        month_val = int(date_str[4:6])
        day_val = int(date_str[6:8])
        print(f"Extracted date from folder name: year={year_val}, month={month_val}, day={day_val}")

        df = df.withColumn("year", lit(year_val))
        df = df.withColumn("month", lit(month_val))
        df = df.withColumn("day", lit(day_val))

        if "duration" in df.columns:
            df = df.withColumn("duration", when(col("duration") < 0, 0).otherwise(col("duration")))

        if "email" in df.columns:
            df = df.withColumn("email", lower(trim(col("email"))))

        if "phone_number" in df.columns:
            df = df.withColumn("phone_number", regexp_replace(col("phone_number"), "[^0-9]", ""))

        final_count = df.count()
        print(f"Final record count after cleaning: {final_count}")
        print(f"Removed {initial_count - final_count} records during cleaning")

        return df
    except Exception as e:
        print(f"Error during data cleaning and enhancement: {str(e)}")
        raise e

# Loop through each category and file
def process_stream_data():
    process_data_category("pii")

def process_data_category(category):
    print(f"Processing {category} stream data...")
    s3_resource = boto3.resource('s3')
    bucket_name = bucket_path.replace("s3://", "")
    category_path = f"{input_path}/{category}"
    prefix = category_path.replace(f"s3://{bucket_name}/", "")
    bucket = s3_resource.Bucket(bucket_name)

    subdirs = set()
    for obj in bucket.objects.filter(Prefix=prefix):
        parts = obj.key.split('/')
        if len(parts) > 2:
            subdirs.add(parts[2])

    for data_type in subdirs:
        data_type_base = data_type.split('_')[0] if '_' in data_type else data_type
        print(f"Processing {category}/{data_type} data...")
        data_type_path = f"{category_path}/{data_type}"
        prefix = data_type_path.replace(f"s3://{bucket_name}/", "")
        date_dirs = set()

        for obj in bucket.objects.filter(Prefix=prefix):
            parts = obj.key.split('/')
            if len(parts) > 3:
                date_dirs.add(parts[3])

        for date_dir in date_dirs:
            input_date_path = f"{data_type_path}/{date_dir}"
            output_date_path = f"{output_path}/{category}/{data_type}"
            date_str = date_dir.split("_")[1] if "_" in date_dir else ""

            # Generate unique path ID
            input_relative_path = f"{category}/{data_type}/{date_dir}"
            if input_relative_path in processed_files:
                print(f"Skipping already cleaned file: {input_relative_path}")
                continue

            print(f"Processing {input_date_path}...")
            try:
                df = spark.read.parquet(input_date_path)
                cleaned_df = clean_and_enhance_data(df, data_type_base, date_str or "20250501")
                cleaned_df.write.mode("append").partitionBy("year", "month").parquet(output_date_path)
                newly_processed.append(input_relative_path)
                print(f"Successfully processed {input_date_path}.")
            except Exception as e:
                print(f"Error processing {input_date_path}: {str(e)}")

# Run the ETL logic
try:
    process_stream_data()

    if newly_processed:
        combined = processed_files.union(newly_processed)
        print(f"Updating cleaned file list with {len(newly_processed)} new files")
        s3_client.put_object(
            Bucket=log_bucket,
            Key=log_key,
            Body='\n'.join(sorted(combined)).encode('utf-8'),
            ContentType='text/plain'
        )

    print("Schema addition and data cleaning job completed successfully!")
    job.commit()
except Exception as e:
    print(f"Error in schema addition and data cleaning job: {str(e)}")
    raise e