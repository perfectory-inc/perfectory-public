# Fixture for tools/semgrep/rules/r2-read-paths.yml (`opengrep scan --test`). Not executed.
# ok: python-reads-r2-through-spark
import boto3
# ruleid: python-reads-r2-through-spark
import s3fs
# ruleid: python-reads-r2-through-spark
from pyarrow import fs
# ruleid: python-reads-r2-through-spark
import fsspec
# ok: python-reads-r2-through-spark
import pyarrow

session = boto3.session.Session()
# ruleid: python-reads-r2-through-spark
s3 = boto3.client("s3", endpoint_url="https://r2.invalid")
# ruleid: python-reads-r2-through-spark
bucket = boto3.resource("s3").Bucket("fixture")
# ruleid: python-reads-r2-through-spark
objects = session.client("s3")
# ruleid: python-reads-r2-through-spark
objects = session.client(service_name="s3", region_name="auto")
# ok: python-reads-r2-through-spark
alarms = boto3.client("sns")
# ok: python-reads-r2-through-spark
secrets = session.client("secretsmanager", region_name="ap-northeast-2")

# ruleid: r2-read-settings-live-in-the-lakehouse-engine
builder.config("spark.hadoop.fs.s3a.retry.limit", "3")
# ruleid: r2-read-settings-live-in-the-lakehouse-engine
builder.config(f"spark.sql.catalog.{catalog}.s3.retry.num-retries", "3")
# ruleid: r2-read-settings-live-in-the-lakehouse-engine
builder.config(f"spark.sql.catalog.{catalog}.http-client.type", "apache")
# ok: r2-read-settings-live-in-the-lakehouse-engine
builder.config("spark.sql.shuffle.partitions", "64")
