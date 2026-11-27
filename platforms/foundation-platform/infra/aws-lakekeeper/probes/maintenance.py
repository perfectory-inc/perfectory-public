"""T11b: Iceberg's stock maintenance procedures in a stock Spark container, on Lakekeeper, with vended credentials only.

spark-submit maintenance.py <table> <dropped table>. Run by the maintenance probe (maintenance.tf) as the operator.
The procedures are upstream's own (Iceberg 1.12.0 for Spark 3.5): expire_snapshots, remove_orphan_files with
prefix listing (the table's FileIO lists its own prefix; Spark has no S3 key of its own), and DROP TABLE ... PURGE
sent to the catalog (rest-catalog-purge, so Lakekeeper's soft deletion applies). Prints one JSON line per result,
never a credential. spark.testing is set in this dev runner only: remove_orphan_files otherwise refuses an
older_than within the last 24 hours, and the planted files are minutes old.
"""

import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

table, dropped = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("w109-maintenance").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def emit(**fields):
    print(json.dumps({"maintenance": True, **fields}, sort_keys=True, default=str), flush=True)


def run(label, sql):
    try:
        result = [row.asDict() for row in spark.sql(sql).collect()]
        emit(step=label, ok=True, rows=result[:50], count=len(result))
        return result
    except Exception as error:  # the type and the first line are the evidence
        emit(step=label, ok=False, error=f"{type(error).__name__}: {str(error).splitlines()[0][:300]}")
        return None


now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
run("refs before", f"SELECT name, type, snapshot_id FROM lk.{table}.refs")
run("snapshots before", f"SELECT snapshot_id, parent_id, operation FROM lk.{table}.snapshots ORDER BY committed_at")
run("expire_snapshots", f"CALL lk.system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '{now}', retain_last => 1)")
run("remove_orphan_files", f"CALL lk.system.remove_orphan_files(table => '{table}', older_than => TIMESTAMP '{now}', prefix_listing => true)")
run("refs after", f"SELECT name, type, snapshot_id FROM lk.{table}.refs")
run("snapshots after", f"SELECT snapshot_id, parent_id, operation FROM lk.{table}.snapshots ORDER BY committed_at")
run("drop with purge through the catalog", f"DROP TABLE lk.{dropped} PURGE")
spark.stop()
