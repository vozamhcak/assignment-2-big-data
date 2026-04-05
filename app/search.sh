#!/bin/bash
set -euo pipefail

QUERY="${*:-}"

if [ -z "$QUERY" ]; then
  echo 'Usage: bash search.sh "your query"'
  exit 1
fi

echo "This script will search documents using BM25"

source .venv/bin/activate

export CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra-server}"
export PYSPARK_DRIVER_PYTHON=$(which python3)

spark-submit \
  --master local[*] \
  query.py "$QUERY"