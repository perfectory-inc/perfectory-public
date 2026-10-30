"""Every comparing Silver lane writes about its changed keys, not its table (root ADR-0275, ADR-0311).

For each lane contract that names `compare.content_columns`, a synthetic release of 40 keys is
loaded, then a next release changes 3 keys, drops 1 and adds 1. `release_changes` must hand back
exactly 3 + 1 rows plus 1 removal marker — not 40 — and the table's current rows
(`platform_contracts.current_rows`) over both loads must equal the next release row for row, as an
overwrite would have left it. The same release is then loaded again with one more change and one
more key gone, as a re-collected file is (ADR-0328): its rows and its marker must outrank the first
load's. Rows are synthetic (PNUs start 99999).

Skipped where pyspark is missing; the CI spark-runtime lane runs it.
"""
import importlib.util
import json
import sys
import unittest
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

LANES_DIR = Path(__file__).resolve().parents[2] / "contracts"
KEYS, CHANGED, DROPPED, ADDED = 40, (3, 7, 11), 20, 99


def comparing_lanes():
    lanes = {}
    for path in sorted(LANES_DIR.glob("*-source-objects.json")):
        block = json.loads(path.read_text(encoding="utf-8")).get("silver_refresh") or {}
        if "compare" in block:
            lanes[block["table"]] = block["compare"]["content_columns"]
    return lanes


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def value(column, kind, n, vintage):
    """A synthetic value for key number `n`; a content column carries the vintage only when changed."""
    if column == "pnu":
        return f"99999101001{n:04d}0000"
    if kind == "string":
        return f"{column}-{n}-{vintage}"
    if kind in ("int", "long"):
        return n
    if kind == "double":
        return float(n)
    if kind == "array<string>":
        return [f"{column}-{n}-{vintage}"]
    if kind == "binary":  # a WKB geometry (root ADR-0362)
        return bytearray(f"{column}-{n}-{vintage}".encode())
    if kind == "date":
        return date(2099, 1, 1)
    if kind == "timestamp":
        return datetime(2099, int(vintage[4:6]) if vintage[:8].isdigit() else 1, 1, tzinfo=timezone.utc)
    raise AssertionError(f"no synthetic {kind}")


def hashable(row):
    """A row as a tuple a Counter can count: an array column (`raw_columns`) as a tuple."""
    return tuple(tuple(v) if isinstance(v, list) else v for v in row)


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class ComparingLanesWriteTheirChangesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-release-compare-lanes")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def release(self, contract, content, keys, vintage, changed=(), tag=None):
        from platform_contracts import column_names, create_table_columns_sql

        kinds = {c["name"]: c["logical_type"] for c in contract["columns"]}
        # The column a changed key changes: a text column, since a PNU is synthetic and fixed per key.
        first_content = next(c for c in content if c != "pnu" and kinds[c] == "string")
        rows = []
        for n in keys:
            row = {}
            for column in column_names(contract):
                if column == "row_state":
                    row[column] = None
                elif column == "source_snapshot_id":
                    row[column] = f"synthetic-lane:{vintage}"
                elif column == "source_record_id":
                    row[column] = f"bronze/{vintage}.zip"
                elif column == "data_reference_date":
                    row[column] = vintage  # changes on every row of every release, and is not content
                elif column == "vintage":
                    row[column] = vintage[:6]  # the release's version, which orders a key's versions
                elif column == first_content:
                    row[column] = value(column, kinds[column], n, (tag or vintage) if n in changed else "same")
                else:
                    row[column] = value(column, kinds[column], n, "same" if column != "ingested_at_utc" else vintage)
            rows.append(tuple(row[c] for c in column_names(contract)))
        schema = " ".join(create_table_columns_sql(contract, indent=0).split())
        return self.spark.createDataFrame(rows, schema)

    def test_every_comparing_lane_appends_its_changed_keys_and_keeps_its_current_rows(self):
        from pyspark.sql import functions as F

        from platform_contracts import REMOVED, column_names, current_row_latest, current_rows, load_lakehouse_contract
        from silver_release_compare import commit_stamp, compare_rule, release_changes, release_edition

        lanes = comparing_lanes()
        self.assertGreaterEqual(len(lanes), 5)
        for table, content in lanes.items():
            with self.subTest(table=table):
                contract = load_lakehouse_contract(table)
                key, removal = compare_rule(contract, content)
                columns = column_names(contract)
                old_keys = list(range(KEYS))
                new_keys = [n for n in old_keys if n != DROPPED] + [ADDED]
                june = self.release(contract, content, old_keys, "20990601")
                september = self.release(contract, content, new_keys, "20990901", changed=CHANGED)

                latest = current_row_latest(contract)

                def compare(release, current, at):
                    instant = F.lit(at).cast("timestamp")
                    stamp, _ = commit_stamp(latest, release_edition(release, latest), instant)
                    return release_changes(release, current, key=key, content=content, removal=removal,
                                           source_snapshot_id="synthetic-lane:20990901", ingested_at=instant,
                                           columns=columns, stamp=stamp)

                changes = compare(september, current_rows(june, contract), datetime(2099, 9, 2, tzinfo=timezone.utc))

                markers = changes.where(F.col(removal) == REMOVED).count()
                self.assertEqual((changes.count() - markers, markers), (len(CHANGED) + 1, 1),
                                 "the changed and added keys and one marker, not the table")
                after = current_rows(june.unionByName(changes), contract)
                compared = [*key, *content]
                self.assertEqual(Counter(hashable(r) for r in after.select(*compared).collect()),
                                 Counter(hashable(r) for r in september.select(*compared).collect()),
                                 "current rows equal the release an overwrite would have written")

                # The same release again, one more key changed and one more gone (ADR-0328): before,
                # a land lane's rows tied on the release id and the re-load's marker was dropped.
                again = self.release(contract, content, [n for n in new_keys if n != 0], "20990901",
                                     changed=CHANGED, tag="again")
                table = june.unionByName(changes)
                reloaded = compare(again, current_rows(table, contract), datetime(2099, 9, 3, tzinfo=timezone.utc))
                self.assertEqual(reloaded.where(F.col(removal) == REMOVED).count(), 1)
                after = current_rows(table.unionByName(reloaded), contract)
                self.assertEqual(Counter(hashable(r) for r in after.select(*compared).collect()),
                                 Counter(hashable(r) for r in again.select(*compared).collect()),
                                 "a release loaded again outranks its first load, its marker included")


if __name__ == "__main__":
    unittest.main()
