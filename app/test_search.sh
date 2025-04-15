#!/bin/bash

# spark-submit \
#   --master yarn \
#   --deploy-mode client \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
#   --conf spark.cassandra.connection.host=cassandra-server \
#   test_query.py

spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  --conf spark.cassandra.connection.host=cassandra-server \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.sql.catalogImplementation=in-memory \
  test_query.py