import math
import os
import re
import sys
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

KEYSPACE = "search_engine"
TABLE = "inverted_index"

K1 = 1.5
B = 0.75

TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


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


def bm25(tf, df, doc_len, avgdl, n_docs, k1=K1, b=B):
    if tf <= 0 or df <= 0 or n_docs <= 0 or avgdl <= 0:
        return 0.0

    idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)
    denom = tf + k1 * (1 - b + b * (doc_len / avgdl))
    return idf * ((tf * (k1 + 1)) / denom)


def read_query():
    argv_query = " ".join(sys.argv[1:]).strip()
    if argv_query:
        return argv_query
    return ""


def load_collection_stats(session):
    rows = session.execute(
        "SELECT stat_key, stat_value FROM collection_stats"
    )
    stats = {row.stat_key: row.stat_value for row in rows}

    n_docs = int(stats.get("doc_count", 0))
    avgdl = float(stats.get("avg_doc_len", 0.0))
    return n_docs, avgdl


def main():
    query_text = read_query()
    if not query_text:
        print("Empty query")
        return

    query_terms = tokenize(query_text)
    if not query_terms:
        print("Empty query after tokenization")
        return

    cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra-server")

    spark = SparkSession.builder.appName("bm25-search").getOrCreate()
    sc = spark.sparkContext

    cluster = Cluster([cassandra_host])
    session = cluster.connect(KEYSPACE)

    n_docs, avgdl = load_collection_stats(session)

    if n_docs == 0 or avgdl == 0:
        print("No indexed documents found")
        cluster.shutdown()
        spark.stop()
        return

    unique_terms = sorted(set(query_terms))
    placeholders = ", ".join(["%s"] * len(unique_terms))

    cql = f"SELECT term, df, postings FROM {TABLE} WHERE term IN ({placeholders})"
    rows = session.execute(cql, tuple(unique_terms))

    posting_rows = []
    for row in rows:
        postings = parse_postings(row.postings)
        for doc_id, doc_title, tf, doc_len in postings:
            score = bm25(
                tf=tf,
                df=row.df,
                doc_len=doc_len,
                avgdl=avgdl,
                n_docs=n_docs,
            )
            posting_rows.append((doc_id, (doc_title, score)))

    if not posting_rows:
        print("No results")
        cluster.shutdown()
        spark.stop()
        return

    rdd = sc.parallelize(posting_rows)

    top_docs = (
        rdd.reduceByKey(lambda a, b: (a[0], a[1] + b[1]))
        .map(lambda x: (x[0], x[1][0], x[1][1]))
        .sortBy(lambda x: -x[2])
        .take(10)
    )

    for rank, (doc_id, doc_title, _) in enumerate(top_docs, start=1):
        print(f"{rank}\t{doc_id}\t{doc_title}")

    cluster.shutdown()
    spark.stop()


if __name__ == "__main__":
    main()