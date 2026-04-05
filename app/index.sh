#!/bin/bash
set -e

echo "Run full indexing pipeline"

bash create_index.sh
bash store_index.sh

echo "index.sh done"