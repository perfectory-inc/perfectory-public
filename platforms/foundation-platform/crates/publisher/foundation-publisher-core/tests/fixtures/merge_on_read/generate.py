#!/usr/bin/env python3
"""Writes the merge-on-read parity fixture the Rust snapshot scan is held to (root ADR-0344).

Two Iceberg tables take the same writes through Spark and the pinned Iceberg runtime
(infra/lakehouse/contracts/lakehouse-engine.contract.json): three range-ordered appends, then the
incremental Gold `MERGE` (gold_incremental.merge_sql's shape: update, delete, insert) twice. One
table is copy-on-write, the other merge-on-read, so the second carries real position delete files
and delete manifests. `expected.json` records Spark's own read of each (they must be equal), where
each table's current manifest list is, and the short name under `files/` of each object Spark wrote;
the Rust test reads the committed files and must return the same rows, PNU for PNU. Synthetic PNUs only (the reserved 99999 region).

Run from the repository root in the pinned Spark image (compose.lakehouse.yml `spark`), with the
engine contract's Iceberg runtime jar in /jars:

    docker run --rm -v "$PWD:/repo" -v "$HOME/.ivy2/jars:/jars:ro" -w /repo --entrypoint bash \
      apache/spark:3.5.6-scala2.12-java17-python3-ubuntu \
      platforms/foundation-platform/crates/publisher/foundation-publisher-core/tests/fixtures/merge_on_read/run-in-spark.sh
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
WAREHOUSE = "/tmp/merge-on-read-fixture"
TABLES = {"copy_on_write": "copy-on-write", "merge_on_read": "merge-on-read"}


def pnu(main: int) -> str:
    return f"9999910100{1}{main:04d}0000"


def rows(mains: range, generation: str) -> list[tuple[str, str, int]]:
    return [(pnu(main), f"{generation}-{main}", main) for main in mains]


def main() -> None:
    from pyspark.sql import SparkSession

    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    spark = (SparkSession.builder.master("local[1,8]")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.fixture", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.fixture.type", "hadoop")
             .config("spark.sql.catalog.fixture.warehouse", f"file:{WAREHOUSE}")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    expected = {"tables": {}}
    for name, mode in TABLES.items():
        table = f"fixture.gold.{name}"
        spark.sql(f"""CREATE TABLE {table} (pnu string NOT NULL, row_digest string, value bigint) USING iceberg
                      TBLPROPERTIES ('format-version' = '2', 'write.distribution-mode' = 'range',
                                     'write.merge.distribution-mode' = 'range',
                                     'write.merge.mode' = '{mode}', 'write.update.mode' = '{mode}',
                                     'write.delete.mode' = '{mode}')""")
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY pnu")
        # Three appends: three data files, each one PNU range.
        for start in (1, 21, 41):
            spark.createDataFrame(rows(range(start, start + 20), "base"), "pnu string, row_digest string, value bigint") \
                .writeTo(table).append()
        # Two incremental merges, as gold_incremental.merge_sql writes them. The second deletes and
        # updates rows the first wrote (a delete file on a file a merge added) and deletes again in
        # a file that already carries deletes.
        for generation, source in (
            ("first", [*[(pnu(m), f"first-{m}", m, False) for m in (2, 3, 25, 44)],
                       *[(pnu(m), None, None, True) for m in (5, 30, 59)],
                       *[(pnu(m), f"first-{m}", m, False) for m in (70, 71, 72, 73)]]),
            ("second", [*[(pnu(m), f"second-{m}", m, False) for m in (3, 71)],
                        *[(pnu(m), None, None, True) for m in (7, 72)]]),
        ):
            spark.createDataFrame(source, "pnu string, row_digest string, value bigint, _delete boolean") \
                .createOrReplaceTempView("source")
            spark.sql(f"""MERGE INTO {table} t USING source s ON t.pnu = s.pnu
                          WHEN MATCHED AND s._delete THEN DELETE
                          WHEN MATCHED AND NOT s._delete THEN UPDATE SET t.row_digest = s.row_digest, t.value = s.value
                          WHEN NOT MATCHED AND NOT s._delete THEN INSERT (pnu, row_digest, value)
                          VALUES (s.pnu, s.row_digest, s.value)""")
        read = [[row.pnu, row.row_digest, row.value] for row in spark.table(table).orderBy("pnu").collect()]
        snapshot = spark.sql(f"SELECT snapshot_id, manifest_list FROM {table}.snapshots "
                             "ORDER BY committed_at DESC LIMIT 1").first()
        deletes = spark.sql(f"SELECT count(*) AS n FROM {table}.delete_files").first().n
        expected["tables"][name] = {"snapshot_id": snapshot.snapshot_id, "manifest_list": snapshot.manifest_list,
                                    "position_delete_files": deletes, "rows": read}
    tables = expected["tables"]
    if tables["copy_on_write"]["rows"] != tables["merge_on_read"]["rows"]:
        raise SystemExit("Spark reads the two tables differently; the fixture proves nothing")
    if tables["copy_on_write"]["position_delete_files"] or not tables["merge_on_read"]["position_delete_files"]:
        raise SystemExit("only the merge-on-read table may carry delete files, and it must")

    # Short names: Iceberg's own (UUIDs, snapshot ids) pass Windows' 260-character path limit here.
    target = HERE / "files"
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir()
    written = sorted(path for path in Path(WAREHOUSE).rglob("*") if path.suffix in (".avro", ".parquet"))
    expected["files"] = {}
    for index, path in enumerate(written):
        shutil.copyfile(path, target / f"{index:02d}{path.suffix}")
        expected["files"][f"file:{path}"] = f"{index:02d}{path.suffix}"
    for name in TABLES:
        for row in spark.sql(f"SELECT file_path FROM fixture.gold.{name}.all_files").collect():
            if row.file_path not in expected["files"]:
                raise SystemExit(f"{row.file_path} is not under the names the fixture maps")
    spark.stop()
    (HERE / "expected.json").write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
