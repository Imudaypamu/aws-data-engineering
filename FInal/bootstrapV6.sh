#!/bin/bash

set -euo pipefail

echo "Running bootstrap actions..."

# === Config ===
SYNC_SCRIPT_PATH="/home/hadoop/sync_wrapper.sh"
S3_LOG_BUCKET="s3://lil-pract-dev-logging/bootstrap"
LOG_FILE="/tmp/bootstrap-actions.log"
mkdir -p /tmp

trap 'echo "Bootstrap failed on line $LINENO. Uploading log to S3..." >&2; aws s3 cp "$LOG_FILE" "$S3_LOG_BUCKET/bootstrap-failed-$(date +%Y%m%d-%H%M%S).log" || echo "S3 upload failed"' ERR

exec > >(tee "$LOG_FILE" | logger -t bootstrap-actions) 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') - Updating yum and installing dependencies..."

# Install necessary dependencies
sudo yum update -y
sudo yum install -y gcc-c++ python3-devel java-1.8.0-openjdk-devel wget unzip git

# === Install Python 3.8 ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Installing Python 3.8..."
sudo yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel
cd /usr/src
sudo wget https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz
sudo tar xzf Python-3.8.10.tgz
cd Python-3.8.10
sudo ./configure --enable-optimizations
sudo make altinstall

# Update alternatives and set python3.8 as default
sudo alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.8 1
sudo alternatives --set python3 /usr/local/bin/python3.8

# Verify installation
python3 --version

# === Install Python Packages ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Installing Python packages..."
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install pandas pyarrow boto3 s3fs numpy pyspark delta-spark

# === Set JAVA_HOME for Spark ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Setting JAVA_HOME for Spark..."
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0" | sudo tee /etc/profile.d/java_home.sh

# === Install Apache Iceberg ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Downloading and configuring Apache Iceberg..."
JAR_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar"
sudo mkdir -p /usr/lib/iceberg
cd /usr/lib/iceberg
sudo wget "$JAR_URL"

# === Configure Spark session defaults for Iceberg ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Configuring Iceberg Spark session defaults..."
sudo mkdir -p /etc/spark/conf
sudo tee -a /etc/spark/conf/spark-defaults.conf > /dev/null <<EOF
spark.sql.catalog.hadoop_prod=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type=hadoop
spark.sql.catalog.hadoop_prod.warehouse=s3://lil-pract-dev-datalake/warehouse/
EOF

# === Download Wrapper and Python Scripts from S3 ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Downloading wrapper and python scripts from S3..."
WRAPPER_DIR="/home/hadoop/wrapper"
SCRIPTS_S3_PATH="s3://lil-pract-dev-scripts/scripts/"
mkdir -p "$WRAPPER_DIR"
aws s3 cp --recursive "${SCRIPTS_S3_PATH}" "$WRAPPER_DIR/"

# === Set permissions for the wrapper files ===
sudo chmod u+x "$WRAPPER_DIR"/*.sh


# === Add Wrapper to system PATH ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Adding $WRAPPER_DIR to system PATH..."
echo "export PATH=\$PATH:$WRAPPER_DIR" | sudo tee /etc/profile.d/custom_path.sh
sudo chmod +x /etc/profile.d/custom_path.sh

# === Bootstrap Completion ===
echo "$(date '+%Y-%m-%d %H:%M:%S') - Bootstrap complete. Uploading success log to S3..."
sleep 2
aws s3 cp "$LOG_FILE" "$S3_LOG_BUCKET/bootstrap-success-$(date +%Y%m%d-%H%M%S).log" || echo "S3 upload failed"
