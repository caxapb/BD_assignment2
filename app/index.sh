#!/bin/bash
INPUT="/index/data"
cd /app/mapreduce/
mapred streaming \
    -files mapper1.py,reducer1.py \
    -mapper 'python3 mapper1.py' \
    -reducer 'python3 reducer1.py' \
    -input $INPUT \
    -output /tmp/index_output

echo "Data is indexed, the reducer output is saved in /tmp/index_output/part-00000"
cd ..
spark-submit /app/app.py
echo "Data is loaded into Cassandra"