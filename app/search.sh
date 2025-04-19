# #!/bin/bash

echo "Starting Searching"

# chack if any parameter (query) was passed. If not - exit with code 1
if [ -z "$1" ]; then
    echo "You need to type the query!!!"
    exit 1
fi

QUERY="$1"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  --conf spark.cassandra.connection.host=cassandra-server \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.sql.catalogImplementation=in-memory \
  query.py "$QUERY"
  