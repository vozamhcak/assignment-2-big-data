## app folder

This folder contains all scripts and source code required to run the simple search engine.

### data
Stores local plain-text documents in the format `<doc_id>_<doc_title>.txt`.

### mapreduce
Stores Hadoop Streaming scripts:
- `mapper1.py`
- `reducer1.py`

### app.py
Creates Cassandra keyspace and tables for:
- inverted index
- vocabulary
- documents
- collection statistics

### app.sh
Main entrypoint executed by the master container. It:
- starts Hadoop services
- creates Python virtual environment
- installs dependencies
- packages the environment for Spark on YARN
- optionally prepares data, indexes, and runs demo queries

### create_index.sh
Runs Hadoop MapReduce pipeline(s) and stores:
- inverted index in `/indexer/index`
- vocabulary in `/indexer/vocabulary`
- collection stats in `/indexer/stats`

### extract_from_parquet.py
Extracts at least 1000 documents from a parquet file using PySpark and writes them as `.txt` files into `data/`.

### index.sh
Runs:
1. `create_index.sh`
2. `store_index.sh`

### prepare_data.py
Reads all text files from HDFS `/data`, transforms them into:
`<doc_id>\t<doc_title>\t<doc_text>`
and writes one-partition output to `/input/data`.

### prepare_data.sh
Uploads local `.txt` files from `data/` to HDFS `/data` and runs `prepare_data.py`.

### query.py
PySpark application that reads a query from stdin, loads index data from Cassandra, computes BM25, and prints top results.

### requirements.txt
Python dependencies required by the project.

### search.sh
Runs `query.py` on YARN in distributed mode.

### start-services.sh
Starts HDFS, YARN, and MapReduce history server.

### store_index.py
Loads index data from local file copied from HDFS into Cassandra tables.

### store_index.sh
Creates Cassandra schema, copies the HDFS index locally, and loads it into Cassandra.