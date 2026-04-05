#!/bin/bash
set -euo pipefail

# Keep only the real worker in Hadoop workers/slaves lists.
WORKERS_FILE="$HADOOP_HOME/etc/hadoop/workers"
SLAVES_FILE="$HADOOP_HOME/etc/hadoop/slaves"

cat > "$WORKERS_FILE" <<'EOF'
cluster-slave-1
EOF

if [ -f "$SLAVES_FILE" ]; then
  cat > "$SLAVES_FILE" <<'EOF'
cluster-slave-1
EOF
fi

# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Start YARN
$HADOOP_HOME/sbin/start-yarn.sh

# Start MapReduce history server
mapred --daemon start historyserver || true

# Wait a bit until HDFS becomes responsive
for _ in $(seq 1 30); do
  if hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Diagnostics
jps -lm || true
hdfs dfsadmin -report || true
hdfs dfsadmin -safemode leave || true

# Prepare HDFS folders for Spark on YARN
hdfs dfs -mkdir -p /apps/spark/jars || true
hdfs dfs -chmod 744 /apps/spark/jars || true
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/ || true
hdfs dfs -chmod -R 755 /apps/spark/jars || true

# Root home in HDFS
hdfs dfs -mkdir -p /user/root || true

scala -version || true
jps -lm || true