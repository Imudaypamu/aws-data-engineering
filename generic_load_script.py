import sys
import os
import tempfile
import boto3
import gzip
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


def unzip_gz_from_s3(s3_client, bucket, key, local_path):
    """
    Download a gzipped file from S3 and unzip it locally.
    """
    gz_path = local_path + ".gz"
    print(f"Downloading s3://{bucket}/{key} to {gz_path} ...")
    s3_client.download_file(bucket, key, gz_path)

    print(f"Unzipping {gz_path} to {local_path} ...")
    with gzip.open(gz_path, 'rb') as f_in, open(local_path, 'wb') as f_out:
        f_out.write(f_in.read())


def read_json_config(s3_client, config_bucket, source_system, table_name):
    """
    Download and parse the JSON config file from S3.
    """
    config_key = f"config/{source_system}/{table_name}_config.json"
    print(f"Fetching config from s3://{config_bucket}/{config_key} ...")
    obj = s3_client.get_object(Bucket=config_bucket, Key=config_key)
    config_content = obj['Body'].read().decode('utf-8')
    config = json.loads(config_content)
    return config


def start_spark():
    """
    Initialize a SparkSession with Delta Lake support.
    """
    builder = SparkSession.builder.appName("GenericLoader")
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def main():
    # Expect exactly 4 arguments
    if len(sys.argv) != 5:
        print("Usage: generic_loader.py <inbound_folder> <source_system> <table_name> <gz_file_key>")
        sys.exit(1)

    inbound_folder = sys.argv[1]
    source_system = sys.argv[2]
    table_name = sys.argv[3]
    gz_file_key = sys.argv[4]

    # Buckets from environment variables or defaults
    LANDING_BUCKET = os.getenv("LANDING_BUCKET", "your-landing-bucket")
    CONFIG_BUCKET = os.getenv("CONFIG_BUCKET", "your-config-bucket")

    s3 = boto3.client("s3")

    # Assume gz_file_key includes S3 key relative to LANDING_BUCKET
    inbound_bucket = LANDING_BUCKET
    inbound_key = gz_file_key

    # Build s3 paths for landing and curated locations
    landing_location = f"s3://{LANDING_BUCKET}/{source_system}_raw.db/raw_/{table_name}/"
    curated_location = f"s3://{LANDING_BUCKET}/{source_system}_raw.db/{table_name}/"

    print(f"Inbound bucket/key: s3://{inbound_bucket}/{inbound_key}")
    print(f"Landing location: {landing_location}")
    print(f"Curated location: {curated_location}")

    # Temporary dir for download and unzip
    with tempfile.TemporaryDirectory() as tmpdir:
        local_unzipped_path = os.path.join(tmpdir, f"{table_name}.csv")

        # Download and unzip gzipped CSV
        unzip_gz_from_s3(s3, inbound_bucket, inbound_key, local_unzipped_path)

        # Start Spark session
        spark = start_spark()

        # Read JSON config from S3
        config = read_json_config(s3, CONFIG_BUCKET, source_system, table_name)

        # Extract config parameters
        curated_load_strategy = config.get("curated_load_strategy", "append").lower()
        landing_load_strategy = config.get("landing_load_strategy", "append").lower()
        primary_key = config.get("primary_key", [])
        sort_column = config.get("sort_column", [])
        curated_script = config.get("curated_script", None)  # Optional, unused here but kept
        curated_table_name = config.get("curated_table_name", table_name)  # Unused here, but can be used elsewhere
        select_columns = config.get("select_columns", None)
        delimiter = config.get("delimiter", ",")

        print(f"Curated Load Strategy: {curated_load_strategy}")
        print(f"Landing Load Strategy: {landing_load_strategy}")
        print(f"Primary Key: {primary_key}")
        print(f"Sort Column: {sort_column}")
        print(f"Curated Script (optional): {curated_script}")
        print(f"Curated Table Name: {curated_table_name}")
        print(f"Select Columns: {select_columns}")
        print(f"CSV Delimiter: '{delimiter}'")

        # Read CSV data into DataFrame
        df = spark.read.option("header", True).option("delimiter", delimiter).csv(local_unzipped_path)

        # Select only specified columns if provided
        if select_columns:
            df = df.select(*select_columns)

        # Add ingestion date partition column from filename if possible (extract date from filename)
        match = re.search(r'(\d{8})', gz_file_key)
        if match:
            dt_raw = match.group(1)
            dt_formatted = f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:]}"
            df = df.withColumn("dt", lit(dt_formatted))
        else:
            df = df.withColumn("dt", lit("unknown"))

        # Write to landing location (Delta format) with partitioning on dt
        print("Writing to landing location...")
        landing_mode = "append" if landing_load_strategy == "append" else "overwrite"
        df.write.format("delta") \
            .mode(landing_mode) \
            .partitionBy("dt") \
            .save(landing_location)

        # Write to curated location based on load strategy
        print("Writing to curated location...")
        if curated_load_strategy == "append":
            df.write.format("delta") \
                .mode("append") \
                .partitionBy("dt") \
                .save(curated_location)

        elif curated_load_strategy == "truncate":
            df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("dt") \
                .save(curated_location)

        elif curated_load_strategy == "upsert":
            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

            if DeltaTable.isDeltaTable(spark, curated_location):
                delta_table = DeltaTable.forPath(spark, curated_location)
                if not primary_key:
                    raise ValueError("Primary key required for upsert strategy")

                merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in primary_key] + ["target.dt = source.dt"])
                delta_table.alias("target").merge(
                    df.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            else:
                # Table doesn't exist yet, write as overwrite partitioned by dt
                df.write.format("delta") \
                    .mode("overwrite") \
                    .partitionBy("dt") \
                    .save(curated_location)

        else:
            raise ValueError(f"Unknown curated load strategy: {curated_load_strategy}")

        print("Data load completed successfully.")

        spark.stop()


if __name__ == "__main__":
    main()
