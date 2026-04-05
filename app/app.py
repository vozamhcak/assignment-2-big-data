import os
from cassandra.cluster import Cluster

KEYSPACE = "search_engine"

CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
"""

CREATE_INVERTED_INDEX = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.inverted_index (
    term text PRIMARY KEY,
    df int,
    postings text
)
"""

CREATE_VOCABULARY = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.vocabulary (
    term text PRIMARY KEY,
    df int
)
"""

CREATE_DOCUMENTS = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.documents (
    doc_id text PRIMARY KEY,
    title text,
    doc_len int
)
"""

CREATE_COLLECTION_STATS = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.collection_stats (
    stat_key text PRIMARY KEY,
    stat_value double
)
"""


def main():
    cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra-server")

    cluster = Cluster([cassandra_host])
    session = cluster.connect()

    session.execute(CREATE_KEYSPACE)
    session.set_keyspace(KEYSPACE)

    session.execute(CREATE_INVERTED_INDEX)
    session.execute(CREATE_VOCABULARY)
    session.execute(CREATE_DOCUMENTS)
    session.execute(CREATE_COLLECTION_STATS)

    print("Cassandra schema is ready")

    cluster.shutdown()


if __name__ == "__main__":
    main()