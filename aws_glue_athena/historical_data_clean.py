import sys
import traceback
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, current_timestamp, to_timestamp, regexp_replace, trim, lower
from pyspark.sql.types import *
from datetime import datetime

# Define logging functions first to ensure they're available
def log_info(message):
    """Log information messages with timestamp"""
    print(f"[INFO] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def log_error(message, exception=None):
    """Log error messages with timestamp and exception details"""
    print(f"[ERROR] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")
    if exception:
        print(f"[ERROR] Exception details: {str(exception)}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")

def test_s3_access(spark, path, description):
    """Test access to an S3 path and log the result - fixed version"""
    try:
        log_info(f"Testing access to {description} at path: {path}")
        # Try to list files using boto3 - simpler and more reliable
        s3 = boto3.client('s3')
        
        # Extract bucket and prefix from s3 path
        path_parts = path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        # List objects in the path
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        
        if 'Contents' in response:
            log_info(f"Successfully accessed {description}. Found files in path.")
            for item in response.get('Contents', [])[:3]:
                log_info(f"  - {item['Key']}")
            return True
        else:
            log_info(f"Path exists but no files found in {description}.")
            return True
    except Exception as e:
        log_error(f"Failed to access {description} at path: {path}", e)
        return False

def write_data_safely(spark, dataframe, path, format="parquet", description="data"):
    """Safely write data to S3 with overwrite mode - fixed version"""
    log_info(f"Starting to write {description} to {path}")
    
    try:
        # Simple write with overwrite mode
        dataframe.write.mode("overwrite").format(format).save(path)
        log_info(f"Successfully wrote {description} to {path}")
        return True
    except Exception as e:
        log_error(f"Failed to write {description} to {path}", e)
        return False

def clean_historical_schedule(spark, bucket_path):
    """Clean historical schedule data"""
    log_info("Processing historical schedule data...")
    input_path = f"{bucket_path}/peak-fitness-historical/non-pii/mindbody_schedule"
    output_path = f"{bucket_path}/cleaned-data/historical/schedule"
    
    try:
        log_info(f"Reading historical schedule data from {input_path}")
        df = spark.read.parquet(input_path)
        log_info(f"Successfully read historical schedule data: {df.count()} rows")
        
        # Show sample data and schema
        log_info("Sample of historical schedule data:")
        df.show(5, truncate=False)
        log_info("Schema of historical schedule data:")
        df.printSchema()
        
        # Clean data
        if not df.rdd.isEmpty():
            log_info("Cleaning historical schedule data")
            
            # Drop duplicates based on session_id
            df = df.dropDuplicates(["session_id"])
            
            # Fill nulls with appropriate values
            df = df.na.fill({
                "class_code": "UNKNOWN",
                "class_name": "UNKNOWN",
                "instructor_id": -1,
                "instructor_name": "UNKNOWN",
                "loc_id": "UNKNOWN",
                "city": "UNKNOWN",
                "location_name": "UNKNOWN",
                "duration_mins": 0
            })
            
            # Additional cleaning
            df = df.withColumn("class_name", trim(df["class_name"]))
            df = df.withColumn("instructor_name", trim(df["instructor_name"]))
            df = df.withColumn("location_name", trim(df["location_name"]))
            
            # Write cleaned data
            write_success = write_data_safely(
                spark, 
                df, 
                output_path, 
                "parquet", 
                "cleaned historical schedule data"
            )
            
            if write_success:
                log_info("Historical schedule data processed and saved successfully")
            else:
                log_error("Failed to save historical schedule data")
        else:
            log_info("No historical schedule data to process")
    except Exception as e:
        log_error("Error processing historical schedule data", e)

def clean_user_snapshot(spark, bucket_path):
    """Clean user snapshot data"""
    log_info("Processing user snapshot data...")
    input_path = f"{bucket_path}/peak-fitness-historical/pii/mindbody_user_snapshot"
    output_path = f"{bucket_path}/cleaned-data/historical/user_snapshot"
    
    try:
        log_info(f"Reading user snapshot data from {input_path}")
        df = spark.read.parquet(input_path)
        log_info(f"Successfully read user snapshot data: {df.count()} rows")
        
        # Show sample data and schema
        log_info("Sample of user snapshot data:")
        df.show(5, truncate=False)
        log_info("Schema of user snapshot data:")
        df.printSchema()
        
        # Clean data
        if not df.rdd.isEmpty():
            log_info("Cleaning user snapshot data")
            
            # Drop duplicates based on user_id
            df = df.dropDuplicates(["user_id"])
            
            # Fill nulls with appropriate values
            df = df.na.fill({
                "first_name": "UNKNOWN",
                "last_name": "UNKNOWN",
                "email": "UNKNOWN",
                "phone_number": "UNKNOWN",
                "preferred_location_id": "UNKNOWN"
            })
            
            # Standardize email addresses (lowercase)
            df = df.withColumn("email", lower(df["email"]))
            
            # Standardize phone numbers (remove non-numeric characters)
            df = df.withColumn("phone_number", 
                              regexp_replace(df["phone_number"], "[^0-9]", ""))
            
            # Write cleaned data
            write_success = write_data_safely(
                spark, 
                df, 
                output_path, 
                "parquet", 
                "cleaned user snapshot data"
            )
            
            if write_success:
                log_info("User snapshot data processed and saved successfully")
            else:
                log_error("Failed to save user snapshot data")
        else:
            log_info("No user snapshot data to process")
    except Exception as e:
        log_error("Error processing user snapshot data", e)

def clean_class_attendance(spark, bucket_path):
    """Clean class attendance data"""
    log_info("Processing class attendance data...")
    input_path = f"{bucket_path}/peak-fitness-historical/pii/class_attendance"
    output_path = f"{bucket_path}/cleaned-data/historical/class_attendance"
    
    try:
        log_info(f"Reading class attendance data from {input_path}")
        df = spark.read.parquet(input_path)
        log_info(f"Successfully read class attendance data: {df.count()} rows")
        
        # Show sample data and schema
        log_info("Sample of class attendance data:")
        df.show(5, truncate=False)
        log_info("Schema of class attendance data:")
        df.printSchema()
        
        # Clean data
        if not df.rdd.isEmpty():
            log_info("Cleaning class attendance data")
            
            # Drop duplicates
            df = df.dropDuplicates()
            
            # Fill nulls with UNKNOWN
            df = df.na.fill("UNKNOWN")
            
            # Add a processed_timestamp column
            df = df.withColumn("processed_timestamp", current_timestamp())
            
            # Write cleaned data
            write_success = write_data_safely(
                spark, 
                df, 
                output_path, 
                "parquet", 
                "cleaned class attendance data"
            )
            
            if write_success:
                log_info("Class attendance data processed and saved successfully")
            else:
                log_error("Failed to save class attendance data")
        else:
            log_info("No class attendance data to process")
    except Exception as e:
        log_error("Error processing class attendance data", e)

def verify_cleaned_data(spark, bucket_path):
    """Verify that cleaned data exists and print record counts"""
    log_info("Verifying cleaned data directories and files...")
    
    cleaned_path = f"{bucket_path}/cleaned-data"
    
    # Use boto3 to list subdirectories reliably
    try:
        s3 = boto3.client('s3')
        path_parts = cleaned_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        # Add trailing slash to prefix if not present
        if prefix and not prefix.endswith('/'):
            prefix = prefix + '/'
            
        # List all objects with this prefix
        paginator = s3.get_paginator('list_objects_v2')
        result_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        # Track directories found
        directories_found = []
        
        for result in result_iterator:
            if 'CommonPrefixes' in result:
                for common_prefix in result['CommonPrefixes']:
                    dir_prefix = common_prefix['Prefix']
                    dir_name = dir_prefix[len(prefix):].rstrip('/')
                    directories_found.append(dir_prefix)
                    log_info(f"Found directory: {dir_name}/")
        
        if not directories_found:
            log_info("No cleaned data directories found. Data processing may have failed.")
            return
            
        # Check each expected output directory - only historical data paths
        data_paths = [
            f"{bucket_path}/cleaned-data/historical/schedule",
            f"{bucket_path}/cleaned-data/historical/user_snapshot",
            f"{bucket_path}/cleaned-data/historical/class_attendance"
        ]
        
        for path in data_paths:
            try:
                # Try to read the parquet data
                df = spark.read.option("pathGlobFilter", "*.parquet").parquet(path)
                count = df.count()
                log_info(f"Successfully verified {path}: {count} records")
            except Exception as e:
                log_info(f"Could not read data from {path}: {str(e)}")
                
    except Exception as e:
        log_error(f"Error verifying cleaned data", e)

def cleanup_test_data(bucket_path):
    """Optional: Clean up test data directory after successful run"""
    test_path = f"{bucket_path}/test-write-verification"
    log_info(f"Cleaning up test data at {test_path}")
    
    try:
        s3 = boto3.resource('s3')
        path_parts = test_path.replace("s3://", "").split("/", 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        bucket = s3.Bucket(bucket_name)
        objects_to_delete = []
        
        # List all objects with the test prefix
        for obj in bucket.objects.filter(Prefix=prefix):
            objects_to_delete.append({'Key': obj.key})
            
        if objects_to_delete:
            s3.meta.client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            log_info(f"Deleted {len(objects_to_delete)} test objects")
        else:
            log_info("No test data found to clean up")
            
    except Exception as e:
        log_error(f"Error cleaning up test data", e)

try:
    # Initialize Glue context
    log_info("Initializing Spark and Glue context")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Set allowed Spark configurations
    log_info("Setting allowed Spark configurations")
    spark.conf.set("spark.sql.broadcastTimeout", "1200")
    
    # Set S3 requester pays configuration - all necessary configurations
    log_info("Setting S3 requester pays configuration")
    sc._jsc.hadoopConfiguration().set("fs.s3.requester.pays.enabled", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3.requester.pays.mode", "requester")
    sc._jsc.hadoopConfiguration().set("fs.s3a.requester.pays.enabled", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3n.requester.pays.enabled", "true")
    
    # Get job parameters
    if '--target_bucket' in sys.argv:
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_bucket'])
    else:
        # Use default value
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        args['target_bucket'] = 'peak-fitness-processed-data-0503'
    
    job_name = args['JOB_NAME']
    target_bucket = args['target_bucket']
    
    # Initialize job
    job.init(job_name, args)
    
    log_info("Starting data cleaning job...")
    log_info(f"Target bucket: {target_bucket}")
    
    # S3 path with proper prefix
    bucket_path = f"s3://{target_bucket}"
    
    # Display AWS identity information (optional but helpful for debugging)
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        log_info(f"Running as: {identity['Arn']}")
        log_info(f"Account ID: {identity['Account']}")
    except Exception as e:
        log_error("Failed to get identity information", e)
    
    # Test access to target bucket
    access_ok = test_s3_access(spark, bucket_path, "target bucket")
    
    if not access_ok:
        log_error(f"Cannot access target bucket: {target_bucket}. Please check permissions.")
        # Continue anyway to see if operations succeed
        log_info("Will attempt to continue processing despite access test failure")
    
    # Create a small test write to verify write permissions
    log_info("Performing test write to target bucket")
    test_data = [("test1", 1), ("test2", 2), ("test3", 3)]
    test_df = spark.createDataFrame(test_data, ["name", "value"])
    test_write_path = f"{bucket_path}/test-write-verification/"
    
    write_test_success = write_data_safely(spark, test_df, test_write_path, "parquet", "test data")
    
    if not write_test_success:
        log_error("Test write failed. Check target bucket permissions and configuration.")
        # Continue anyway to see if other operations succeed
        log_info("Will attempt to continue with data cleaning despite write test failure")
    
    # Clean historical data only
    clean_historical_schedule(spark, bucket_path)
    clean_user_snapshot(spark, bucket_path)
    clean_class_attendance(spark, bucket_path)
    
    # Verify cleaned data after all processing
    verify_cleaned_data(spark, bucket_path)
    
    # Optional: Clean up test data
    if write_test_success:
        cleanup_test_data(bucket_path)
    
    log_info("Historical data cleaning job completed!")
    job.commit()

except Exception as e:
    log_error("Unhandled exception in data cleaning job", e)
    raise e