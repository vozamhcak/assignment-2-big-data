#!/usr/bin/env python3
import sys
import re
from collections import Counter

TOKEN_RE = re.compile(r"[a-z0-9]+")

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, doc_title, doc_text = parts

    text = doc_text.lower()
    tokens = TOKEN_RE.findall(text)

    if not tokens:
        continue

    doc_len = len(tokens)
    tf_counter = Counter(tokens)

    for term, tf in tf_counter.items():
        # key = term
        # value = doc_id, doc_title, doc_len, tf
        print(f"{term}\t{doc_id}\t{doc_title}\t{doc_len}\t{tf}")