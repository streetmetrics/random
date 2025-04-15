import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, to_timestamp, current_timestamp, struct, to_date, from_unixtime
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField, DateType
import h3
import yaml
from tqdm import tqdm
import os

# Parameters
MARKETS = ['001F-COL-0CA2','9D37-NEW-3259','CCC2-BOS-25AA']
PREFIX = "2024/01/01/"

# MARKETS = None
# PREFIX = None

H3_RESOLUTION_BASE = 15


os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
os.environ["SPARK_HOME"] = "/Users/sunaybhat/Documents/spark-3.5.4-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/Users/sunaybhat/miniconda3/envs/s3_h3_env/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/sunaybhat/miniconda3/envs/s3_h3_env/bin/python"

CONFIG_PATH = '/Users/sunaybhat/Documents/GitHub/DS_config.yaml'
with open(CONFIG_PATH, 'r') as yamlfile: config = yaml.load(yamlfile, Loader=yaml.FullLoader)

spark = SparkSession.builder \
    .appName("S3 H3 Indexation") \
    .config("spark.hadoop.fs.s3a.access.key", config['AWS']['KEYs']['Access']) \
    .config("spark.hadoop.fs.s3a.secret.key", config['AWS']['KEYs']['Secret']) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    .getOrCreate()

# S3 Configuration
SOURCE_BUCKET = "near-market-data"
TARGET_BUCKET = "azira-backfill-snowflake"

S3_CLIENT = boto3.client(
        's3',
        region_name=config['AWS']['Region'],
        aws_access_key_id=config['AWS']['KEYs']['Access'],
        aws_secret_access_key=config['AWS']['KEYs']['Secret']    )


# Define schema based on Snowflake procedure
schema = StructType([
    StructField("device_id", StringType()),
    StructField("unix_ts", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("datasource_id", IntegerType()),
    StructField("accuracy", IntegerType()),
    StructField("device_ip", StringType()),
    StructField("user_agent", StringType()),
    StructField("publisher_id", IntegerType()),
    StructField("make", StringType()),
    StructField("os", StringType()),
    StructField("os_version", StringType()),
    StructField("categories", StringType()),
    StructField("country_code", StringType()),
    StructField("market_hash", StringType())
])

def get_market_prefixes(s3_client, bucket_name, base_prefix="USA/"):
    """Get all market prefixes using bucket collection delimiter"""
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=base_prefix,
        Delimiter="/"
    )
    
    # Extract just the market paths like "USA/001F-COL-0CA2/"
    market_prefixes = [prefix['Prefix'].split('/')[1] for prefix in response.get('CommonPrefixes', [])]
    return market_prefixes


def transform_and_save_files(input_files, output_path, schema=schema, force_copy=True):
    """
    Transform a list of Near Market data files and save to specified location
    
    Args:
        input_files (list): List of S3 file paths to process
        output_path (str): S3 path to save processed data
        schema (StructType): Schema for the input data
        force_copy (bool): Whether to overwrite existing data
    """

    # Define H3 indexing functions
    h3_udf = udf(lambda lat, lon: h3.geo_to_h3(lat, lon, H3_RESOLUTION_BASE), StringType())
    h3_parent_udf = udf(lambda h3_index, res: h3.h3_to_parent(h3_index, res), StringType())

    if not input_files:
        print("No files provided to process")
        return
    
    try:
        # Read all files into a Spark DataFrame
        df = spark.read.csv(
            input_files,
            sep='\t',
            schema=schema,
            inferSchema=False
        )
        
        # Add H3 indices and other required columns
        df = df.withColumn("h3i_15", h3_udf(col("lat"), col("lon"))) \
            .withColumn("h3i_11", h3_parent_udf(col("h3i_15"), lit(11))) \
            .withColumn("h3i_9", h3_parent_udf(col("h3i_15"), lit(9))) \
            .withColumn("h3i_7", h3_parent_udf(col("h3i_15"), lit(7))) \
            .withColumn("h3i_5", h3_parent_udf(col("h3i_15"), lit(5))) \
            .withColumn("core_date", from_unixtime('unix_ts').cast(DateType())) \
            .withColumn("timestamp", to_timestamp(col("unix_ts"))) \
            .withColumn("device_geo", struct(col("lon"), col("lat"))) \

        
        # Select and reorder columns to match Snowflake structure
        df = df.select(
            "core_date","device_id","timestamp",
            "unix_ts","lat","lon",
            "device_geo","datasource_id",
            "accuracy","device_ip",
            "user_agent","publisher_id",
            "make","os","os_version",
            "categories","country_code","market_hash",
            "h3i_15","h3i_11","h3i_9","h3i_7","h3i_5"
        )
        
        # Write processed data
        write_mode = "overwrite" if force_copy else "append"
        df.coalesce(1).write.mode(write_mode) \
                .option("compression", "gzip") \
                .parquet(output_path)
        
        # print(f"Successfully processed {len(input_files)} files and saved to {output_path}")
        return True
        
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        return False


def main():

    # Get all market prefixes
    if MARKETS:
        markets = MARKETS
    else:
        markets = get_market_prefixes(S3_CLIENT, SOURCE_BUCKET)

    if PREFIX:
        for market in tqdm(markets):  # Loop through markets
            files = [f"s3a://near-market-data/USA/{market}/{PREFIX}part-{str(i).zfill(5)}.gz" for i in range(50)]
            output = f"s3a://azira-backfill-snowflake/USA/{market}/{PREFIX.replace('/', '_')}"
            success = transform_and_save_files(files, output)
            print(files)
        if success:
            print(f"Successfully processed and saved data to azira-backfill-snowflake")
        else:
            print("Failed to process files.")

    else:
        for month in tqdm(range(1, 13)):  # Loop through months
            for day in tqdm(range(1, 32)):  # Loop through days
                for market in tqdm(markets):  # Loop through markets
                    formatted_day = str(day).zfill(2)
                    formatted_month = str(month).zfill(2)
                    current_prefix = f"2024/{formatted_month}/{formatted_day}"
                    files = [f"s3a://near-market-data/USA/{market}/2024/{current_prefix}/part-{str(i).zfill(5)}.gz" for i in range(50)]
                    output = f"s3a://azira-backfill-snowflake/USA/2024/{market}_{current_prefix.replace('/', '_')}"
                    success = transform_and_save_files(files, output)
        if success:
            print(f"Successfully processed and saved data to azira-backfill-snowflake")
        else:
            print("Failed to process files.")

if __name__ == "__main__":
    main()