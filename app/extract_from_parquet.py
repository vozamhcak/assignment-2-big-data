#!/usr/bin/env python3
import os
import sys
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim


def safe_title(title: str) -> str:
    cleaned = sanitize_filename(str(title)).replace(" ", "_")
    cleaned = cleaned.strip("._")
    return cleaned or "untitled"


def main():
    parquet_path = sys.argv[1] if len(sys.argv) > 1 else "/app/a.parquet"
    n_docs = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "data"

    spark = (
        SparkSession.builder
        .appName("extract-parquet-docs")
        .master("local[*]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

    df = (
        spark.read.parquet(parquet_path)
        .select("id", "title", "text")
        .where(col("id").isNotNull())
        .where(col("title").isNotNull())
        .where(col("text").isNotNull())
        .where(length(trim(col("text"))) > 0)
        .limit(n_docs)
    )

    os.makedirs(output_dir, exist_ok=True)

    for name in os.listdir(output_dir):
        if name.endswith(".txt"):
            os.remove(os.path.join(output_dir, name))

    created = 0
    for row in df.toLocalIterator():
        doc_id = str(row["id"])
        title = safe_title(row["title"])
        text = str(row["text"])

        filename = f"{doc_id}_{title}.txt"
        path = os.path.join(output_dir, filename)

        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

        created += 1

    spark.stop()
    print(f"Created {created} documents in {output_dir}")


if __name__ == "__main__":
    main()