import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.functions import current_timestamp

def log_info(message):
    """Log information messages with timestamp"""
    print(f"[INFO] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Set up S3 requester pays configuration
log_info("Setting S3 requester pays configuration")
sc._jsc.hadoopConfiguration().set("fs.s3a.requester.pays.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
sc._jsc.hadoopConfiguration().set("fs.s3.requester.pays.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3.requester.pays.mode", "requester")

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define bucket information
source_bucket = "peak-fitness-data-raw"
target_bucket = "peak-fitness-processed-data-0503"

log_info("=== Starting ETL Process - Only Processing New Stream Files ===")

# Create S3 client
s3_client = boto3.client('s3')

# Only list files in stream/ folder
log_info(f"Listing files under: peak-fitness-stream/")
paginator = s3_client.get_paginator('list_objects_v2')
pages = paginator.paginate(
    Bucket=source_bucket,
    Prefix='peak-fitness-stream/',
    RequestPayer='requester'
)

# Process each file
for page in pages:
    if 'Contents' in page:
        for item in page['Contents']:
            source_key = item['Key']
            file_size = item['Size']
            
            # Skip empty files or directories
            if file_size == 0 or source_key.endswith('/'):
                log_info(f"Skipping empty or directory: {source_key}")
                continue

            log_info(f"\n--- Processing file: {source_key} (Size: {file_size} bytes) ---")
            
            try:
                # File extension
                file_extension = source_key.split('.')[-1].lower() if '.' in source_key else ""
                
                # Build S3 source and target path
                source_path = f"s3a://{source_bucket}/{source_key}"
                target_prefix = '/'.join(source_key.split('/')[:-1]) if '/' in source_key else ''
                file_name = source_key.split('/')[-1].split('.')[0]
                target_key_check = f"{target_prefix}/{file_name}/"
                target_path = f"s3a://{target_bucket}/{target_key_check}"

                # Skip if already exists in target bucket
                response = s3_client.list_objects_v2(
                    Bucket=target_bucket,
                    Prefix=target_key_check,
                    RequestPayer='requester'
                )
                if 'Contents' in response:
                    log_info(f"Target already exists: {target_key_check}, skipping.")
                    continue

                # Read data by file type
                if file_extension in ['csv', 'txt']:
                    log_info(f"Reading CSV data from: {source_path}")
                    try:
                        df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
                    except:
                        log_info("Retrying without header")
                        df = spark.read.option("header", "false").option("inferSchema", "true").csv(source_path)
                elif file_extension in ['json', 'jsonl']:
                    log_info(f"Reading JSON data from: {source_path}")
                    df = spark.read.json(source_path)
                elif file_extension in ['parquet']:
                    log_info(f"Reading Parquet data from: {source_path}")
                    df = spark.read.parquet(source_path)
                elif file_extension in ['orc']:
                    log_info(f"Reading ORC data from: {source_path}")
                    df = spark.read.orc(source_path)
                else:
                    log_info(f"Unsupported format: {file_extension}, skipping.")
                    continue

                # Show schema
                log_info("Schema:")
                df.printSchema()
                
                row_count = df.count()
                log_info(f"Read {row_count} rows")

                # Add timestamp
                transformed_df = df.withColumn("processed_time", current_timestamp())

                # Write as Parquet
                log_info(f"Writing to: {target_path}")
                transformed_df.write.mode("overwrite").parquet(target_path)
                log_info(f"Write completed to {target_path}")

                # Verify
                verification = spark.read.parquet(target_path)
                verification_count = verification.count()
                log_info(f"Verification count: {verification_count}")
                if verification_count != row_count:
                    log_info(f"WARNING: Row count mismatch! Original: {row_count}, Written: {verification_count}")

            except Exception as e:
                log_info(f"Error processing {source_key}: {str(e)}")
                import traceback
                log_info(f"Traceback: {traceback.format_exc()}")
                continue

log_info("=== ETL Process Completed ===")
job.commit()
