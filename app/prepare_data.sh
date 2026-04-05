#!/bin/bash
set -e

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "Cleaning old HDFS folders..."
hdfs dfs -rm -r -f /data || true
hdfs dfs -rm -r -f /input/data || true

echo "Creating /data in HDFS ..."
hdfs dfs -mkdir -p /data

echo "Collecting file list..."
mapfile -d '' files < <(find data -type f -name "*.txt" -print0)
total=${#files[@]}

echo "Uploading txt files to HDFS ... total: $total"
ok_count=0
skip_count=0
current=0

for file in "${files[@]}"; do
  current=$((current + 1))
  echo "[$current/$total] $file"

  if hdfs dfs -put "$file" /data/ 2>/dev/null; then
    ok_count=$((ok_count + 1))
  else
    echo "skip $file"
    skip_count=$((skip_count + 1))
  fi
done

echo "Uploaded: $ok_count"
echo "Skipped: $skip_count"

echo "Run Spark preparation..."
spark-submit prepare_data.py

echo "Check HDFS folders:"
hdfs dfs -ls /data | head
hdfs dfs -ls /input/data

echo "Preview prepared data:"
hdfs dfs -cat /input/data/part-* | head -3 || true

echo "done data preparation!"