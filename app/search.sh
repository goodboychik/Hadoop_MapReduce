#!/bin/bash
echo "Searching for documents matching query: $1"

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Python of the executor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

# Submit the Spark job with the query
spark-submit --master yarn \
    --archives /app/.venv.tar.gz#.venv \
    query.py "$@"