import sys
import os
import json
import logging
import traceback
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from delta import *
from delta.tables import DeltaTable
from pyspark import SparkConf


# Set up logging
def setup_logger(log_file_path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file_path)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


# Load config JSON directly from S3 using boto3
def load_config_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read().decode('utf-8')
    config = json.loads(content)
    return config


# Build Spark schema from dict with string types
def build_schema_from_dict(schema_dict):
    type_map = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        # Add more type mappings here if needed
    }
    fields = []
    for col_name, type_str in schema_dict.items():
        spark_type = type_map.get(type_str)
        if not spark_type:
            raise ValueError(f"Unsupported data type: {type_str} for column {col_name}")
        fields.append(StructField(col_name, spark_type, True))
    return StructType(fields)


# Start Spark session with Delta support and S3 (s3a)
def start_spark():
    conf = SparkConf()
    conf.setAppName("GenericLoader")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.sql.catalogImplementation", "hive")

    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    conf.set("spark.sql.warehouse.dir", os.environ.get("WAREHOUSE_BUCKET", "s3a://default-warehouse-bucket/"))
    conf.set("spark.sql.shuffle.partitions", "200")

    return SparkSession.builder.config(conf=conf).getOrCreate()


# Load data to curated with strategy (append, overwrite, upsert)
def load_to_curated(df, curated_location, curated_load_strategy, primary_key=None):
    if curated_load_strategy == "append":
        df.write.format("parquet") \
            .mode("append") \
            .partitionBy("dt") \
            .save(curated_location)
        print("Curated data appended successfully")

    elif curated_load_strategy == "overwrite":
        df.write.format("parquet") \
            .mode("overwrite") \
            .partitionBy("dt") \
            .save(curated_location)
        print("Curated data overwritten successfully")

    elif curated_load_strategy == "upsert":
        if not primary_key:
            raise ValueError("Primary key is required for upsert strategy")

        if DeltaTable.isDeltaTable(spark, curated_location):
            delta_table = DeltaTable.forPath(spark, curated_location)
            merge_condition = " AND ".join(
                [f"target.`{k}` = source.`{k}`" for k in primary_key] + ["target.dt = source.dt"]
            )
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print("Curated data upserted successfully")
        else:
            df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("dt") \
                .save(curated_location)
            print("New delta table created and data inserted")

    else:
        raise ValueError(f"Unknown curated load strategy: {curated_load_strategy}")


def main():
    global spark
    logger = None
    spark = None

    try:
        if len(sys.argv) != 5:
            print("Usage: load_script.py <inbound_folder> <source_system> <table_name> <file_path>")
            sys.exit(1)

        inbound_folder = sys.argv[1]
        source_system = sys.argv[2]
        table_name = sys.argv[3]
        file_path = sys.argv[4]

        # Setup logger
        tmp_log_dir = "/tmp/PySpark"
        os.makedirs(tmp_log_dir, exist_ok=True)
        tmp_log_file = f"{tmp_log_dir}/{table_name}_pyspark_processing.log"
        logger = setup_logger(tmp_log_file)
        logger.info(f"Starting processing for source_system={source_system}, table_name={table_name}, file={file_path}")

        # Helper to convert s3:// to s3a:// for Spark paths
        def s3a_path(s3_path):
            if s3_path.startswith("s3://"):
                return "s3a://" + s3_path[5:]
            return s3_path

        LANDING_PATH = s3a_path(os.environ.get("LANDING_BUCKET", "s3a://default-landing-bucket/"))
        CONFIG_PATH = os.environ.get("CONFIG_BUCKET", "s3://default-config-bucket/")  # Keep s3:// for boto3
        CURATED_PATH = s3a_path(os.environ.get("CURATED_BUCKET", "s3a://default-curated-bucket/"))
        full_s3_path = f"{LANDING_PATH.rstrip('/')}/{file_key.lstrip('/')}"
        spark_file_path = s3a_path(full_s3_path)
        

        # Load config from S3 using boto3
        config_s3_path = f"{CONFIG_PATH}/{source_system}/{table_name}/{table_name}_config.json"

        if not config_s3_path.startswith("s3://"):
            raise ValueError("CONFIG_BUCKET environment variable must start with s3://")

        path_parts = config_s3_path[5:].split("/", 1)
        bucket_name = path_parts[0]
        key = path_parts[1]

        logger.info(f"Loading config from s3://{bucket_name}/{key}")
        config = load_config_from_s3(bucket_name, key)

        # Extract config parameters
        schema_dict = config.get("schema", None)
        if not schema_dict:
            raise ValueError("Schema must be defined in the config file")

        schema = build_schema_from_dict(schema_dict)

        curated_load_strategy = config.get("curated_load_strategy", "append").lower()
        landing_load_strategy = config.get("landing_load_strategy", "append").lower()
        primary_key = config.get("primary_key", [])
        delimiter = config.get("delimiter", ",")
        select_columns = config.get("select_columns", None)
        sort_columns = config.get("sort_column", [])

        logger.info(f"Config loaded: curated_load_strategy={curated_load_strategy}, primary_key={primary_key}")

        curated_location = f"{CURATED_PATH}/{source_system}/{table_name}/"
        landing_location = f"{LANDING_PATH}/{source_system}/{table_name}/"

        # Start Spark session
        spark = start_spark()

        # Read CSV with schema and delimiter
        logger.info(f"Reading CSV file from {spark_file_path} with schema")
        df = spark.read.option("header", True).option("delimiter", delimiter).schema(schema).csv(spark_file_path)

        if select_columns:
            logger.info(f"Selecting columns: {select_columns}")
            df = df.select(*select_columns)

        if sort_columns:
            logger.info(f"Sorting by columns: {sort_columns}")
            df = df.orderBy(*sort_columns)

        # Write to landing zone based on landing_load_strategy
        logger.info(f"Writing data to landing zone: {landing_location} with strategy {landing_load_strategy}")
        if landing_load_strategy == "append":
            df.write.mode("append").parquet(landing_location)
        elif landing_load_strategy == "overwrite":
            df.write.mode("overwrite").parquet(landing_location)
        else:
            logger.warning(f"Unknown landing_load_strategy: {landing_load_strategy}. Skipping landing write.")

        # Extract partition date from file name
        date_partition = file_path.split("/")[-1].split("_")[-1].split(".")[0]
        dt_formatted = f"{date_partition[:4]}-{date_partition[4:6]}-{date_partition[6:]}"
        logger.info(f"Extracted date partition: {dt_formatted}")

        # Add metadata columns
        df = df.withColumn("dt", lit(dt_formatted))
        df = df.withColumn("updt_nm", lit("EMR-PySpark")) \
               .withColumn("cret_nm", lit("EMR-PySpark")) \
               .withColumn("cret_ts", current_timestamp()) \
               .withColumn("updt_ts", current_timestamp())

        # Load to curated zone using strategy
        load_to_curated(df, curated_location, curated_load_strategy, primary_key)

        logger.info("Processing completed successfully.")

    except Exception as e:
        if logger:
            logger.error(f"Error occurred: {str(e)}")
            logger.error(traceback.format_exc())
        else:
            print("Error occurred:", str(e))
            traceback.print_exc()
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            if logger:
                logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
