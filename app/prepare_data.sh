#!/bin/bash

echo "prepare_data.sh has started..."
. .venv/bin/activate
PARQUET_FILE="/app/data/test.parquet"

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

echo "putting data to hdfs..."
hdfs dfs -put -f "$PARQUET_FILE" / && \
    spark-submit /app/prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -mkdir /data/ && /
    hdfs dfs -put /app/data/*.txt /data/ && \
    hdfs dfs -ls /data && \
    echo "done data preparation!" && \
    echo "Starting RDD!" && \
    spark-submit /app/prepare_data_v2.py && \
    hdfs dfs -ls /index/data