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

printf "%s" "$QUERY" | spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives .venv.tar.gz#.venv \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.executorEnv.CASSANDRA_HOST="$CASSANDRA_HOST" \
  --conf spark.yarn.appMasterEnv.CASSANDRA_HOST="$CASSANDRA_HOST" \
  --conf spark.yarn.maxAppAttempts=1 \
  query.py