"""The media reconciler (scripts/ops/media_reconcile.py, root ADR-0387) against synthetic listings.

2026-10-26: the onbid collector hit its 40-minute limit with its Bronze commit at the end of the run,
so photos it had stored landed with no line naming them, and nothing compared R2 with the index.
These tests plant an orphan and a missing object in a fake listing and pin that the reconciler finds
both, leaves an orphan inside its grace alone, keeps what earlier runs' index reads named, reads the
listing line by line, and that the job, its unit and its alerts are wired. Hashes are of synthetic
strings.

2026-11-16..18: the listing (188,000 objects) loaded as one JSON document held the run at its memory
limit until its 10 minutes ran out, every run; and the index was read through Trino, which is being
retired. The listing is now JSON Lines read as it comes, and the index delta is the publisher's.
"""

import hashlib
import json
import pathlib
import re
import sys
import tempfile
import tracemalloc
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
PLATFORM = ORCHESTRATION.parent
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import media_reconcile  # noqa: E402

PREFIX = "media/display/sha256/"
TABLES = ["silver.onbid_real_estate_item_photo", "silver.court_auction_item_photo",
          "silver.industrial_complex_notice_attachment", "silver.court_auction_appraisal_report",
          "silver.industrial_land_sale_notice_attachment"]
TABLE = "+".join(TABLES)
NOW = 1_800_000_000.0  # a synthetic instant


def sha(name):
    return hashlib.sha256(name.encode()).hexdigest()


def key(h, fmt):
    return f"{PREFIX}{h}.{fmt}"


def listed(object_key, hours_ago):
    from datetime import datetime, timezone
    at = datetime.fromtimestamp(NOW - hours_ago * 3600, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"key": object_key, "size_bytes": 100, "last_modified": at}


def header(count=None, prefix=PREFIX):
    head = {"prefix": prefix, "list_request_count": 1}
    if count is not None:
        head["object_count"] = count
    return head


def lines(*objects):
    """The listing file list-lakehouse-objects writes: a header line, then one line per object."""
    return "".join(json.dumps(line) + "\n" for line in [header(len(objects)), *objects])


def delta(keys, files=None):
    return {"index_table": TABLE, "keys": keys, "read_files": files or {TABLES[0]: ["s3://f/1.parquet"]},
            "tables": {}}


class Reconcile(unittest.TestCase):
    def check(self, referenced, *objects):
        return media_reconcile.reconcile(referenced, header(len(objects)), iter(objects), PREFIX, 48, NOW)

    def test_an_orphan_and_a_missing_object_are_found(self):
        kept, lost, stray = sha("synthetic-kept"), sha("synthetic-lost"), sha("synthetic-stray")
        result = self.check({key(kept, "jpg"), key(lost, "pdf")},
                            listed(key(kept, "jpg"), 100), listed(key(stray, "png"), 100))
        self.assertEqual(result["missing"], [key(lost, "pdf")])
        self.assertEqual([o["key"] for o in result["orphans"]], [key(stray, "png")])
        self.assertEqual((result["missing_count"], result["orphan_count"], result["orphan_bytes"]), (1, 1, 100))

    def test_agreeing_sets_report_nothing(self):
        kept = sha("synthetic-kept")
        result = self.check({key(kept, "jpg")}, listed(key(kept, "jpg"), 100))
        self.assertEqual((result["missing_count"], result["orphan_count"], result["listed_objects"]), (0, 0, 1))

    def test_an_unreferenced_object_inside_the_grace_is_not_an_orphan_yet(self):
        young = sha("synthetic-young")
        result = self.check(set(), listed(key(young, "jpg"), 47))
        self.assertEqual((result["orphan_count"], result["young_unreferenced_count"]), (0, 1))

    def test_a_key_of_another_format_is_not_the_indexed_one(self):
        # The key is <hash>.<format>: the same hash under another extension is not what the page asks.
        h = sha("synthetic-format")
        result = self.check({key(h, "jpg")}, listed(key(h, "png"), 100))
        self.assertEqual((result["missing_count"], result["orphan_count"]), (1, 1))

    def test_a_listing_of_another_prefix_a_short_listing_or_a_row_naming_no_key_is_refused(self):
        with self.assertRaises(ValueError):
            media_reconcile.reconcile(set(), header(0, "onbid/"), iter([]), PREFIX, 48, NOW)
        # A listing cut short (a full disk, a killed writer) is not a listing with fewer objects.
        with self.assertRaises(ValueError):
            media_reconcile.reconcile(set(), header(2), iter([listed(key(sha("a"), "jpg"), 100)]), PREFIX, 48, NOW)
        with self.assertRaises(ValueError):
            media_reconcile.key_of(PREFIX, "not-a-hash", "jpg")


class Run(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self.tmp_dir.name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def run_once(self, index_delta, listing_text, now):
        (self.tmp / "delta.json").write_text(json.dumps(index_delta), encoding="utf-8")
        (self.tmp / "listing.jsonl").write_text(listing_text, encoding="utf-8")
        return media_reconcile.run(self.tmp, self.tmp / "delta.json", self.tmp / "listing.jsonl",
                                   self.tmp / "reports", self.tmp / "m.prom", now=now)

    def gauges(self):
        return (self.tmp / "m.prom").read_text(encoding="utf-8")

    def test_a_missing_object_reaches_the_gauge(self):
        lost = sha("synthetic-lost")
        self.run_once(delta([[lost, "jpg"]]), lines(), NOW)
        self.assertIn(f'foundation_media_reconcile_missing_objects{{prefix="{PREFIX}"}} 1', self.gauges())

    def test_a_run_keeps_what_earlier_runs_named_and_the_files_its_delta_read(self):
        # Partial (root ADR-0314): the second run's delta holds only the new file's key, and the key the
        # first run read is still referenced, so it is neither missing nor an orphan.
        old, new = sha("synthetic-old"), sha("synthetic-new")
        both = lines(listed(key(old, "jpg"), 100), listed(key(new, "jpg"), 100))
        first = self.run_once(delta([[old, "jpg"]], {TABLES[0]: ["s3://f/1.parquet"]}), both, NOW)
        self.assertEqual((first["missing_count"], first["orphan_count"]), (0, 1))
        self.assertEqual(media_reconcile.load_state(self.tmp)["read_files"], {TABLES[0]: ["s3://f/1.parquet"]})
        second = self.run_once(delta([[new, "jpg"]], {TABLES[0]: ["s3://f/1.parquet", "s3://f/2.parquet"]}),
                               both, NOW + 86400)
        self.assertEqual((second["index_keys_read"], second["referenced_keys"]), (1, 2))
        self.assertEqual((second["missing_count"], second["orphan_count"]), (0, 0))
        self.assertEqual(media_reconcile.load_state(self.tmp)["read_files"][TABLES[0]],
                         ["s3://f/1.parquet", "s3://f/2.parquet"])
        self.assertEqual(len(list((self.tmp / "reports").glob("*.json"))), 2)  # one per run, none replaced
        self.assertIn(f'foundation_media_reconcile_index_rows_read{{prefix="{PREFIX}"}} 1', self.gauges())

    def test_a_state_from_the_trino_watermark_keeps_its_keys_and_reads_every_file_once(self):
        kept = sha("synthetic-kept")
        (self.tmp / "referenced.json").write_text(json.dumps(
            {"index_table": TABLE, "watermark": "2026-11-15T00:00:00Z", "keys": [key(kept, "jpg")]}), encoding="utf-8")
        state = media_reconcile.load_state(self.tmp)
        self.assertEqual((state["read_files"], "watermark" in state), ({}, False))
        result = self.run_once(delta([]), lines(listed(key(kept, "jpg"), 100)), NOW)
        self.assertEqual((result["referenced_keys"], result["missing_count"]), (1, 0))

    def test_the_comparison_holds_the_reference_set_not_the_listing(self):
        # 2026-11-15: 132,338 referenced keys, 187,986 listed objects. Scaled to a tenth, the run's
        # Python heap stays near the reference set's size; loading the listing whole held ~144 MiB at
        # full scale (measured 2026-11-18 on the old code with 140,000 keys and 200,000 objects).
        objects = [listed(key(sha(f"synthetic-{i}"), "jpg"), 100) for i in range(20_000)]
        referenced = [o["key"] for o in objects[:14_000]]
        (self.tmp / "referenced.json").write_text(json.dumps({"index_table": TABLE, "keys": referenced}),
                                                  encoding="utf-8")
        listing_text = lines(*objects)
        tracemalloc.start()
        try:
            result = self.run_once(delta([]), listing_text, NOW)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        self.assertEqual((result["listed_objects"], result["orphan_count"]), (20_000, 6_000))
        self.assertLess(peak, 12 * 2**20)


class Wiring(unittest.TestCase):
    def test_the_contract_names_the_prefix_and_the_index(self):
        config = media_reconcile.settings()
        self.assertEqual((config["prefix"], config["index_table"]), (PREFIX, TABLE))

    def test_the_index_is_read_by_the_publisher_not_trino(self):
        script = (PLATFORM / "scripts/ops/media-reconcile.sh").read_text(encoding="utf-8")
        self.assertIn('"${PUBLISHER_BIN}" media-index-delta', script)
        for text in (script, (PLATFORM / "scripts/ops/media_reconcile.py").read_text(encoding="utf-8")):
            self.assertNotIn("docker", text)
            self.assertNotIn("trino --", text)

    def test_the_job_runs_the_script_and_each_alert_reads_a_gauge_it_writes(self):
        jobs = json.loads((ORCHESTRATION / "jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
        (job,) = [j for j in jobs if j["id"] == "media_reconcile"]
        self.assertEqual(job["scope"], "new-objects")
        unit = (PLATFORM / "infra/systemd" / job["systemd_service"]).read_text(encoding="utf-8")
        self.assertIn("ExecStart=/opt/foundation-platform/current/scripts/ops/media-reconcile.sh", unit)
        written = set(re.findall(r'\("([a-z_]+)", "', (PLATFORM / "scripts/ops/media_reconcile.py").read_text(encoding="utf-8")))
        rules = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")
        read = set(re.findall(r"foundation_media_reconcile_([a-z_]+)", rules))
        self.assertEqual(read, {"missing_objects", "orphan_objects", "timestamp_seconds"})
        self.assertLessEqual(read, written)


if __name__ == "__main__":
    unittest.main()
