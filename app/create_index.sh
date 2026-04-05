#!/bin/bash
set -euo pipefail

cd /app

service ssh restart || true

echo "Starting Hadoop services..."
bash start-services.sh

echo "Recreating Python virtual environment..."
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

echo "Packing virtual environment for Spark on YARN..."
rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

export CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra-server}"

wait_for_hdfs() {
  echo "Waiting for HDFS..."
  for _ in $(seq 1 60); do
    if hdfs dfs -ls / >/dev/null 2>&1; then
      echo "HDFS is ready"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: HDFS is not ready"
  return 1
}

wait_for_cassandra() {
  echo "Waiting for Cassandra..."
  for _ in $(seq 1 90); do
    if python3 - <<'PY'
import os
from cassandra.cluster import Cluster

host = os.environ.get("CASSANDRA_HOST", "cassandra-server")
try:
    cluster = Cluster([host])
    session = cluster.connect()
    session.execute("SELECT release_version FROM system.local")
    cluster.shutdown()
    raise SystemExit(0)
except Exception:
    raise SystemExit(1)
PY
    then
      echo "Cassandra is ready"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: Cassandra is not ready"
  return 1
}

wait_for_hdfs
wait_for_cassandra

TXT_COUNT=$(find data -type f -name "*.txt" | wc -l | tr -d ' ')

if [ "$TXT_COUNT" -eq 0 ] && [ -f /app/a.parquet ]; then
  echo "No local txt files found, extracting 1000 docs from /app/a.parquet ..."
  python3 extract_from_parquet.py /app/a.parquet 1000 data
  TXT_COUNT=$(find data -type f -name "*.txt" | wc -l | tr -d ' ')
fi

if [ "$TXT_COUNT" -gt 0 ]; then
  echo "Found $TXT_COUNT txt documents. Running demo pipeline..."
  bash prepare_data.sh
  CASSANDRA_HOST="$CASSANDRA_HOST" bash index.sh

  echo
  echo "Demo query 1:"
  CASSANDRA_HOST="$CASSANDRA_HOST" bash search.sh "history time" || true

  echo
  echo "Demo query 2:"
  CASSANDRA_HOST="$CASSANDRA_HOST" bash search.sh "christmas carol" || true
else
  echo "No txt documents found in /app/data and no /app/a.parquet provided."
  echo "Environment is ready for manual run."
fi

tail -f /dev/null