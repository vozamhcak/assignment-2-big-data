````markdown
# big-data-assignment2

Simple search engine using Hadoop MapReduce, Cassandra, and Spark RDD.

## Prerequisites

- Docker
- Docker Compose

## How to run

### 1. Start everything

```bash
docker compose up
````

This starts:

* `cluster-master`
* `cluster-slave-1`
* `cassandra-server`

The master container runs `app/app.sh` automatically.

## 2. Dataset options

There are two supported ways to provide documents.

### Option A: Local `.txt` files

Put files into:

```text
app/data/
```

Each file must follow this format:

```text
<doc_id>_<doc_title>.txt
```

### Option B: Parquet file

Put one parquet file at:

```text
app/a.parquet
```

Then `app.sh` can extract 1000 documents from it using PySpark.

## Manual commands inside the master container

Open a shell:

```bash
docker exec -it cluster-master bash
cd /app
source .venv/bin/activate
```

### Extract 1000 documents from parquet

```bash
python3 extract_from_parquet.py /app/a.parquet 1000 data
```

### Prepare input in HDFS

```bash
bash prepare_data.sh
```

### Build index in HDFS

```bash
bash create_index.sh
```

### Store index in Cassandra

```bash
CASSANDRA_HOST=cassandra-server bash store_index.sh
```

### Run the full indexing pipeline

```bash
CASSANDRA_HOST=cassandra-server bash index.sh
```

### Search

```bash
CASSANDRA_HOST=cassandra-server bash search.sh "history time"
```

## Project files

Main required files are in `app/`:

* `app.sh`
* `start-services.sh`
* `prepare_data.py`
* `prepare_data.sh`
* `create_index.sh`
* `store_index.sh`
* `index.sh`
* `query.py`
* `search.sh`
* `app.py`
* `store_index.py`
* `mapreduce/mapper1.py`
* `mapreduce/reducer1.py`
* `extract_from_parquet.py`
* `requirements.txt`

```
```
