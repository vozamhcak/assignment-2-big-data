#!/usr/bin/env python3
import sys

current_term = None
postings = []

def flush(term, postings_list):
    if term is None or not postings_list:
        return

    df = len(postings_list)
    postings_str = ";".join(postings_list)
    print(f"{term}\t{df}\t{postings_str}")

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 4)
    if len(parts) != 5:
        continue

    term, doc_id, doc_title, doc_len, tf = parts
    posting = f"{doc_id}|{doc_title}|{tf}|{doc_len}"

    if current_term is None:
        current_term = term

    if term != current_term:
        flush(current_term, postings)
        current_term = term
        postings = []

    postings.append(posting)

flush(current_term, postings)