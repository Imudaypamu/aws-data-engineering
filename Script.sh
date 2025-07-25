#!/bin/bash

# Check if correct number of arguments is passed
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <s3_object_key>"
  exit 1
fi

# Input S3 object key (passed as an argument)
S3_OBJECT_KEY=$1  # Example: inbound/sap/customers/customers_20240628.gz

# Parse the components of the S3 object key (i.e., extract folder and file details)
IFS='/' read -r -a parts <<< "$S3_OBJECT_KEY"
INBOUND_FOLDER=${parts[0]}      # inbound
SOURCE_SYSTEM=${parts[1]}       # sap
TABLE_NAME=${parts[2]}          # customers

echo "S3 Object Key: $S3_OBJECT_KEY"
echo "Inbound Folder: $INBOUND_FOLDER"
echo "Source System: $SOURCE_SYSTEM"
echo "Table Name: $TABLE_NAME"

# Download config.json from S3 config bucket (adjust your path accordingly)
CONFIG_BUCKET="s3://ted-config"
CONFIG_S3_PATH="$CONFIG_BUCKET/$SOURCE_SYSTEM/$TABLE_NAME/${TABLE_NAME}_config.json"
LOCAL_CONFIG="/tmp/${TABLE_NAME}_config.json"

# Download the config file
echo "Downloading config file from $CONFIG_S3_PATH..."
aws s3 cp "$CONFIG_S3_PATH" "$LOCAL_CONFIG"
if [ $? -ne 0 ]; then
  echo "Failed to download config.json from $CONFIG_S3_PATH"
  exit 2
fi

# Extract curated_script name using jq
CURATED_SCRIPT=$(jq -r '.curated_script' "$LOCAL_CONFIG")
if [ -z "$CURATED_SCRIPT" ] || [ "$CURATED_SCRIPT" == "null" ]; then
  echo "curated_script not found or empty in config.json"
  exit 3
fi

echo "Curated script from config: $CURATED_SCRIPT"

# Define your S3 path for the Python script
SCRIPTS_BUCKET="s3://ted-scripts/scripts"
SCRIPT_S3_PATH="$SCRIPTS_BUCKET/pyspark/$CURATED_SCRIPT"

# Download the Python script
LOCAL_SCRIPT="/tmp/pyspark/$CURATED_SCRIPT"
echo "Downloading python script from $SCRIPT_S3_PATH to $LOCAL_SCRIPT..."
aws s3 cp "$SCRIPT_S3_PATH" "$LOCAL_SCRIPT"
if [ $? -ne 0 ]; then
  echo "Failed to download python script from S3."
  exit 4
fi

# Export necessary environment variables (Optional)
export LANDING_BUCKET="s3://ted-landing"
export CONFIG_BUCKET="s3://ted-config"
export CURATED_BUCKET="s3://ted-datalake/warehouse"
export LOG_BUCKET="s3://ted-logging1"

# Run Python script with extracted params (without spark-submit)
echo "Running Python script with extracted parameters..."
python3 "$LOCAL_SCRIPT" "$INBOUND_FOLDER" "$SOURCE_SYSTEM" "$TABLE_NAME" "$S3_OBJECT_KEY"

echo "Python script executed successfully."
