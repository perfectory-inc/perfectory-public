#!/usr/bin/env bash
# Runs generate.py inside the pinned Spark image (see its docstring for the docker command).
set -euo pipefail
export HOME=/tmp
py4j=(/opt/spark/python/lib/py4j-*-src.zip)
export PYTHONPATH="/opt/spark/python:${py4j[0]}"
export PYSPARK_SUBMIT_ARGS="--jars /jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar pyspark-shell"
exec python3 "$(dirname "$0")/generate.py"
