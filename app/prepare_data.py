from pyspark.sql import SparkSession
import os
import re

spark = SparkSession.builder \
    .appName("prepare txt data") \
    .getOrCreate()

sc = spark.sparkContext

input_path = "/data/*.txt"
output_path = "/input/data"

def parse_file(item):
    path, content = item
    filename = os.path.basename(path)

    if not filename.endswith(".txt"):
        return None

    name = filename[:-4]
    match = re.match(r"^([^_]+)_(.+)$", name)
    if not match:
        return None

    doc_id = match.group(1)
    doc_title = match.group(2)

    text = content.replace("\t", " ").replace("\n", " ").strip()
    if not text:
        return None

    return f"{doc_id}\t{doc_title}\t{text}"

rdd = sc.wholeTextFiles(input_path).map(parse_file).filter(lambda x: x is not None)
rdd.coalesce(1).saveAsTextFile(output_path)

spark.stop()