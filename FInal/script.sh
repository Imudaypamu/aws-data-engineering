#!/bin/bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <s3_object_key>"
  exit 1
fi

S3_OBJECT_KEY=$1    # e.g., inbound/sap/customers/customers_20240628.csv

# Parse S3 object key
IFS='/' read -r -a parts <<< "$S3_OBJECT_KEY"
INBOUND_FOLDER=${parts[0]}
SOURCE_SYSTEM=${parts[1]}
TABLE_NAME=${parts[2]}

echo "S3 Object Key: $S3_OBJECT_KEY"
echo "Inbound Folder: $INBOUND_FOLDER"
echo "Source System: $SOURCE_SYSTEM"
echo "Table Name: $TABLE_NAME"


# Define buckets and paths
CONFIG_BUCKET="s3://lil-pract-dev-config"
CONFIG_S3_PATH="$CONFIG_BUCKET/$SOURCE_SYSTEM/$TABLE_NAME/${TABLE_NAME}_config.json"
LOCAL_CONFIG="/tmp/${TABLE_NAME}_config.json"

SCRIPTS_BUCKET="s3://lil-pract-dev-scripts/scripts"
LOCAL_SCRIPT_DIR="/tmp/pyspark"
mkdir -p "$LOCAL_SCRIPT_DIR"


# Download configuration JSON
echo "Downloading config file from $CONFIG_S3_PATH..."
aws s3 cp "$CONFIG_S3_PATH" "$LOCAL_CONFIG"

if [ $? -ne 0 ]; then
  echo "Failed to download config.json from $CONFIG_S3_PATH"
  exit 2
fi


# Extract the PySpark script filename using jq
CURATED_SCRIPT=$(jq -r '.curated_script' "$LOCAL_CONFIG")
if [ -z "$CURATED_SCRIPT" ] || [ "$CURATED_SCRIPT" == "null" ]; then
  echo "curated_script not found or empty in config.json"
  exit 3
fi

echo "Curated script from config: $CURATED_SCRIPT"

# Download PySpark script
SCRIPT_S3_PATH="$SCRIPTS_BUCKET/pyspark/$CURATED_SCRIPT"
LOCAL_SCRIPT="$LOCAL_SCRIPT_DIR/$CURATED_SCRIPT"

echo "Downloading PySpark script from $SCRIPT_S3_PATH to $LOCAL_SCRIPT..."
aws s3 cp "$SCRIPT_S3_PATH" "$LOCAL_SCRIPT"

if [ $? -ne 0 ]; then
  echo "Failed to download PySpark script from $SCRIPT_S3_PATH"
  exit 4
fi


# Export required environment variables for buckets
export LANDING_BUCKET="s3://lil-pract-dev-landing"
export CONFIG_BUCKET="s3://lil-pract-dev-config"
export CURATED_BUCKET="s3://lil-pract-dev-datalake/warehouse"
export LOG_BUCKET="s3://lil-pract-dev-logging"
export WAREHOUSE_BUCKET="s3a://lil-pract-dev-datalake/warehouse"

echo "Environment variables for S3 buckets set."


# Run PySpark script using spark-submit
echo "Running PySpark script with spark-submit..."
spark-submit "$LOCAL_SCRIPT" "$INBOUND_FOLDER" "$SOURCE_SYSTEM" "$TABLE_NAME" "$S3_OBJECT_KEY"

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  echo "spark-submit failed with exit code $EXIT_CODE"
  exit 5
fi

echo "PySpark job executed successfully."

exit 0
