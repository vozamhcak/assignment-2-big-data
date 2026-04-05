#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

INPUT_PATH="${1:-/input/data}"

echo "Creating index from HDFS input: $INPUT_PATH"

hdfs dfs -test -e "$INPUT_PATH"

echo "Cleaning old index folders..."
hdfs dfs -rm -r -f /indexer || true
hdfs dfs -rm -r -f /tmp/indexer || true

hdfs dfs -mkdir -p /indexer

echo "Running Hadoop Streaming job..."
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming"*.jar \
  -D mapreduce.job.name="search-engine-index" \
  -D mapreduce.job.reduces=1 \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output /indexer/index

echo "Checking index output..."
hdfs dfs -ls /indexer/index
hdfs dfs -cat /indexer/index/part-00000 | head -5 || true

echo "create_index.sh done"