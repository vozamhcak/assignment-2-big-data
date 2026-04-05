import os
from cassandra.cluster import Cluster

KEYSPACE = "search_engine"
INDEX_FILE = "/tmp/index_part_00000"


def parse_postings(postings_str: str):
    result = []
    if not postings_str:
        return result

    for item in postings_str.split(";"):
        parts = item.split("|", 3)
        if len(parts) != 4:
            continue

        doc_id, doc_title, tf, doc_len = parts

        try:
            tf = int(tf)
            doc_len = int(doc_len)
        except ValueError:
            continue

        result.append((doc_id, doc_title, tf, doc_len))

    return result


def main():
    cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra-server")

    cluster = Cluster([cassandra_host])
    session = cluster.connect(KEYSPACE)

    # Clean old data to keep the index consistent between reruns
    for table in ("inverted_index", "vocabulary", "documents", "collection_stats"):
        session.execute(f"TRUNCATE {table}")

    insert_index = session.prepare(
        "INSERT INTO inverted_index (term, df, postings) VALUES (?, ?, ?)"
    )
    insert_vocab = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    insert_doc = session.prepare(
        "INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)"
    )
    insert_stat = session.prepare(
        "INSERT INTO collection_stats (stat_key, stat_value) VALUES (?, ?)"
    )

    docs = {}
    inserted_terms = 0

    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t", 2)
            if len(parts) != 3:
                continue

            term, df, postings = parts

            try:
                df = int(df)
            except ValueError:
                continue

            session.execute(insert_index, (term, df, postings))
            session.execute(insert_vocab, (term, df))
            inserted_terms += 1

            for doc_id, doc_title, _, doc_len in parse_postings(postings):
                docs[doc_id] = (doc_title, doc_len)

    for doc_id, (doc_title, doc_len) in docs.items():
        session.execute(insert_doc, (doc_id, doc_title, doc_len))

    doc_count = len(docs)
    avg_doc_len = (
        sum(doc_len for _, doc_len in docs.values()) / doc_count
        if doc_count > 0 else 0.0
    )

    session.execute(insert_stat, ("doc_count", float(doc_count)))
    session.execute(insert_stat, ("avg_doc_len", float(avg_doc_len)))

    print(f"Inserted terms: {inserted_terms}")
    print(f"Inserted documents: {doc_count}")
    print(f"Average document length: {avg_doc_len:.6f}")

    cluster.shutdown()


if __name__ == "__main__":
    main()