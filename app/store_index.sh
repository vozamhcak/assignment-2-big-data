#!/bin/bash
set -euo pipefail

echo "Store the index to Cassandra/ScyllaDB tables"

source .venv/bin/activate
export CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra-server}"

echo "Creating Cassandra schema..."
python3 app.py

echo "Checking HDFS index path..."
hdfs dfs -test -e /indexer/index/part-00000

echo "Copying index file from HDFS..."
rm -f /tmp/index_part_00000
hdfs dfs -get -f /indexer/index/part-00000 /tmp/index_part_00000

echo "Loading index into Cassandra..."
python3 store_index.py

echo "store_index.sh done"