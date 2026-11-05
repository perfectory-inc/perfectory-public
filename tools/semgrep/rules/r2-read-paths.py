# Fixture for tools/semgrep/rules/r2-read-paths.yml (`opengrep scan --test`). Not executed.
# ruleid: python-reads-r2-through-spark
import boto3
# ruleid: python-reads-r2-through-spark
from botocore.config import Config
# ruleid: python-reads-r2-through-spark
import s3fs
# ruleid: python-reads-r2-through-spark
from pyarrow import fs
# ruleid: python-reads-r2-through-spark
import fsspec
# ok: python-reads-r2-through-spark
import pyarrow

# ruleid: r2-read-settings-live-in-the-lakehouse-engine
builder.config("spark.hadoop.fs.s3a.retry.limit", "3")
# ruleid: r2-read-settings-live-in-the-lakehouse-engine
builder.config(f"spark.sql.catalog.{catalog}.s3.retry.num-retries", "3")
# ruleid: r2-read-settings-live-in-the-lakehouse-engine
builder.config(f"spark.sql.catalog.{catalog}.http-client.type", "apache")
# ok: r2-read-settings-live-in-the-lakehouse-engine
builder.config("spark.sql.shuffle.partitions", "64")
