"""The media reconciler (scripts/ops/media_reconcile.py, root ADR-0387) against synthetic listings.

2026-10-26: the onbid collector hit its 40-minute limit with its Bronze commit at the end of the run,
so photos it had stored landed with no line naming them, and nothing compared R2 with the index.
These tests plant an orphan and a missing object in a fake listing and pin that the reconciler finds
both, leaves an orphan inside its grace alone, reads the index only since its last run, and that the
job, its unit and its alerts are wired. Hashes are of synthetic strings.
"""

import hashlib
import json
import pathlib
import re
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
PLATFORM = ORCHESTRATION.parent
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import media_reconcile  # noqa: E402

PREFIX = "onbid/media/sha256/"
TABLES = ["silver.onbid_real_estate_item_photo", "silver.court_auction_item_photo",
          "silver.industrial_complex_notice_attachment", "silver.court_auction_appraisal_report"]
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


def listing(*objects):
    return {"prefix": PREFIX, "list_request_count": 1, "objects": list(objects)}


def delta(keys, newest, since=None):
    return {"index_table": TABLE, "since": since, "keys": keys, "newest": newest}


class Reconcile(unittest.TestCase):
    def test_an_orphan_and_a_missing_object_are_found(self):
        kept, lost, stray = sha("synthetic-kept"), sha("synthetic-lost"), sha("synthetic-stray")
        result = media_reconcile.reconcile({key(kept, "jpg"), key(lost, "pdf")},
                                           listing(listed(key(kept, "jpg"), 100), listed(key(stray, "png"), 100)),
                                           PREFIX, 48, NOW)
        self.assertEqual(result["missing"], [key(lost, "pdf")])
        self.assertEqual([o["key"] for o in result["orphans"]], [key(stray, "png")])
        self.assertEqual((result["missing_count"], result["orphan_count"], result["orphan_bytes"]), (1, 1, 100))

    def test_agreeing_sets_report_nothing(self):
        kept = sha("synthetic-kept")
        result = media_reconcile.reconcile({key(kept, "jpg")}, listing(listed(key(kept, "jpg"), 100)), PREFIX, 48, NOW)
        self.assertEqual((result["missing_count"], result["orphan_count"]), (0, 0))

    def test_an_unreferenced_object_inside_the_grace_is_not_an_orphan_yet(self):
        young = sha("synthetic-young")
        result = media_reconcile.reconcile(set(), listing(listed(key(young, "jpg"), 47)), PREFIX, 48, NOW)
        self.assertEqual((result["orphan_count"], result["young_unreferenced_count"]), (0, 1))

    def test_a_key_of_another_format_is_not_the_indexed_one(self):
        # The key is <hash>.<format>: the same hash under another extension is not what the page asks.
        h = sha("synthetic-format")
        result = media_reconcile.reconcile({key(h, "jpg")}, listing(listed(key(h, "png"), 100)), PREFIX, 48, NOW)
        self.assertEqual((result["missing_count"], result["orphan_count"]), (1, 1))

    def test_a_listing_of_another_prefix_or_a_row_naming_no_key_is_refused(self):
        with self.assertRaises(ValueError):
            media_reconcile.reconcile(set(), {"prefix": "onbid/", "objects": []}, PREFIX, 48, NOW)
        with self.assertRaises(ValueError):
            media_reconcile.key_of(PREFIX, "not-a-hash", "jpg")

    def test_the_index_is_read_since_the_watermark_less_the_overlap(self):
        self.assertNotIn("WHERE", media_reconcile.index_sql(TABLES, None))  # the first run reads it whole
        self.assertIn("WHERE ingested_at_utc >= TIMESTAMP '2026-01-01 22:00:00 UTC'",
                      media_reconcile.index_sql(TABLES, "2026-01-02T00:00:00.000000Z"))
        with self.assertRaises(ValueError):
            media_reconcile.index_sql(["silver.x; select 1"], None)

    # 2026-11-06 (root ADR-0429): the court photo index joins the onbid one; until its first Silver
    # load the table does not exist, and the reconciler must not fail every day for it.
    def test_every_index_naming_the_store_is_read_and_a_table_not_there_yet_names_nothing(self):
        sql = media_reconcile.index_sql(TABLES, None)
        for table in TABLES:
            self.assertIn(f"FROM {table}", sql)
        self.assertEqual(media_reconcile.existing(TABLES, {TABLES[0]}), [TABLES[0]])


class Run(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self.tmp_dir.name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def run_once(self, index_delta, objects, now):
        (self.tmp / "delta.json").write_text(json.dumps(index_delta), encoding="utf-8")
        (self.tmp / "listing.json").write_text(json.dumps(objects), encoding="utf-8")
        return media_reconcile.run(self.tmp, self.tmp / "delta.json", self.tmp / "listing.json",
                                   self.tmp / "reports", self.tmp / "m.prom", now=now)

    def gauges(self):
        return (self.tmp / "m.prom").read_text(encoding="utf-8")

    def test_a_missing_object_reaches_the_gauge(self):
        lost = sha("synthetic-lost")
        self.run_once(delta([[lost, "jpg"]], "2026-01-01T00:00:00.000000Z"), listing(), NOW)
        self.assertIn(f'foundation_media_reconcile_missing_objects{{prefix="{PREFIX}"}} 1', self.gauges())

    def test_a_run_reads_the_index_since_its_last_and_keeps_what_it_named(self):
        # Partial (root ADR-0314): the second run's delta holds only the new row, and the key the first
        # run read is still referenced, so it is neither missing nor an orphan.
        old, new = sha("synthetic-old"), sha("synthetic-new")
        both = listing(listed(key(old, "jpg"), 100), listed(key(new, "jpg"), 100))
        first = self.run_once(delta([[old, "jpg"]], "2026-01-01T00:00:00.000000Z"), both, NOW)
        self.assertEqual((first["missing_count"], first["orphan_count"]), (0, 1))
        self.assertEqual(media_reconcile.load_state(self.tmp)["watermark"], "2026-01-01T00:00:00.000000Z")
        second = self.run_once(delta([[new, "jpg"]], "2026-01-02T00:00:00.000000Z", "2026-01-01T00:00:00.000000Z"),
                               both, NOW + 86400)
        self.assertEqual((second["index_keys_read"], second["referenced_keys"]), (1, 2))
        self.assertEqual((second["missing_count"], second["orphan_count"]), (0, 0))
        self.assertEqual(len(list((self.tmp / "reports").glob("*.json"))), 2)  # one per run, none replaced
        self.assertIn(f'foundation_media_reconcile_index_rows_read{{prefix="{PREFIX}"}} 1', self.gauges())

    def test_an_empty_delta_keeps_the_watermark(self):
        kept = sha("synthetic-kept")
        self.run_once(delta([[kept, "jpg"]], "2026-01-01T00:00:00.000000Z"), listing(listed(key(kept, "jpg"), 100)), NOW)
        self.run_once(delta([], None, "2026-01-01T00:00:00.000000Z"), listing(listed(key(kept, "jpg"), 100)), NOW + 60)
        state = media_reconcile.load_state(self.tmp)
        self.assertEqual((state["watermark"], state["keys"]), ("2026-01-01T00:00:00.000000Z", [key(kept, "jpg")]))


class Wiring(unittest.TestCase):
    def test_the_contract_names_the_prefix_and_the_index(self):
        config = media_reconcile.settings()
        self.assertEqual((config["prefix"], config["index_table"]), (PREFIX, TABLE))

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
