# #!/bin/bash

# # Check if query is provided
# if [ -z "$1" ]; then
#     echo "Usage: ./search.sh 'your query here'"
#     exit 1
# fi

# QUERY="$1"

# # Run PySpark application on YARN cluster
# spark-submit \
#     --master yarn \
#     --deploy-mode cluster \
#     --conf spark.cassandra.connection.host=cassandra-server \
#     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
#     ./query.py "$QUERY"

#!/bin/bash

echo "Starting Cassandra connection test..."

if [ -z "$1" ]; then
    echo "Usage: ./search.sh 'your query here'"
    exit 1
fi

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv test_query.py  $1

