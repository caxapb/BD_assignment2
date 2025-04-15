#!/bin/bash

echo "prepare_data.sh has started..."
. .venv/bin/activate
PARQUET_FILE="/app/data/a.parquet"

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

echo "load parquet file"
hdfs dfs -put -f "$PARQUET_FILE" / 

echo "prepare_data1.py is runnings"
spark-submit /app/prepare_data.py

echo "Putting data to hdfs"
hdfs dfs -mkdir /data/
hdfs dfs -put /app/data/*.txt /data/
hdfs dfs -ls /data
echo "done data preparation!"

echo "Starting RDD!"
spark-submit /app/prepare_data2.py
hdfs dfs -ls /index/data