"""The shape ledger check captures production and publishes it where CI reads it (root ADR-0259, ADR-0476)."""

import contextlib
import hashlib
import importlib.util
import tempfile
import unittest.mock
import json
import pathlib
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("shape_ledger_check", PLATFORM / "scripts/ops/shape-ledger-check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


class Differences(unittest.TestCase):
    SHAPE = {"has_rows": True, "fields": [[1, "a", "string", True]], "columns": [1], "earlier": []}

    def lakehouse(self, **tables):
        return {"captured_at": "then", "tables": tables}

    def test_the_same_shape_captured_later_is_no_difference(self):
        self.assertEqual(check.differences(self.lakehouse(t=self.SHAPE),
                                           {**self.lakehouse(t=self.SHAPE), "captured_at": "now"}, "tables"), [])

    def test_a_new_table_a_gone_table_and_a_changed_shape_are_each_named(self):
        rewritten = {**self.SHAPE, "earlier": [[1, 2]]}
        found = check.differences(self.lakehouse(kept=self.SHAPE, gone=self.SHAPE),
                                  self.lakehouse(kept=rewritten, new=self.SHAPE), "tables")
        self.assertEqual(found, ["new: new in production", "gone: gone from production", "kept: shape changed"])

    def test_postgres_tables_that_gained_or_lost_rows_are_named(self):
        found = check.differences({"tables_with_rows": ["a", "b"]}, {"tables_with_rows": ["b", "c"]},
                                  "tables_with_rows")
        self.assertEqual(found, ["c: now holds rows", "a: no longer holds rows"])

    def test_the_committed_ledgers_are_where_the_check_reads_them(self):
        for relative, key in ((check.LAKEHOUSE, "tables"), (check.POSTGRES, "tables_with_rows")):
            self.assertIn(key, json.loads((PLATFORM / relative).read_text(encoding="utf-8")))

    def test_it_runs_after_every_deploy_and_daily(self):
        jobs = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
        job = next(job for job in jobs if job["id"] == "shape_ledger_check")
        self.assertTrue(job["enabled"] and job["started_once_after_deploy"])
        self.assertEqual(job["systemd_service"], "foundation-shape-ledger-check.service")


class WaitingThenFailed(unittest.TestCase):
    """A run that cannot publish waits, and pages once nothing has been published for 48 hours."""

    NOW = check.datetime(2026, 10, 16, 12, 0, tzinfo=check.timezone.utc)

    def test_within_48_hours_the_run_waits_and_says_why(self):
        first = self.NOW - timedelta(hours=47)
        line, status = check.verdict(self.NOW, first, "not published (connection reset)")
        self.assertEqual(status, 0)
        self.assertTrue(line.startswith("foundation-job-outcome waiting shape ledgers unpublished since"))
        self.assertIn("connection reset", line)

    def test_after_48_hours_the_run_fails(self):
        line, status = check.verdict(self.NOW, self.NOW - timedelta(hours=48), "not published")
        self.assertEqual(status, 1)
        self.assertTrue(line.startswith("foundation-job-outcome failed"))

    def test_first_unpublished_is_kept_across_runs_and_cleared_by_a_publish(self):
        with tempfile.TemporaryDirectory() as state:
            with unittest.mock.patch.object(check, "STATE", pathlib.Path(state)):
                first = check.first_unpublished_at(self.NOW, False)
                later = check.first_unpublished_at(self.NOW + timedelta(hours=5), False)
                self.assertEqual(first, later)
                self.assertIsNone(check.first_unpublished_at(self.NOW, True))
                self.assertFalse((pathlib.Path(state) / check.FIRST_UNPUBLISHED).exists())

    def test_two_published_runs_48_hours_apart_both_pass_though_production_moved(self):
        # W50, 2026-11-11: the committed ledger went stale and the check failed each day after 48 hours
        # until someone copied the capture into a pull request. A published run waits on no copy.
        with tempfile.TemporaryDirectory() as state:
            with unittest.mock.patch.object(check, "STATE", pathlib.Path(state)):
                first = check.outcome(self.NOW, True, True, "published", [])
                second = check.outcome(self.NOW + timedelta(hours=48), True, True, "published", [])
        self.assertEqual(first, ("foundation-job-outcome changed", 0))
        self.assertEqual(second, ("foundation-job-outcome changed", 0))

    def test_unpublished_for_48_hours_then_published_passes_again(self):
        with tempfile.TemporaryDirectory() as state:
            with unittest.mock.patch.object(check, "STATE", pathlib.Path(state)):
                self.assertEqual(check.outcome(self.NOW, False, False, "not published", [])[1], 0)
                self.assertEqual(check.outcome(self.NOW + timedelta(hours=48), False, False, "not published", [])[1], 1)
                self.assertEqual(check.outcome(self.NOW + timedelta(hours=49), True, False, "published", []),
                                 ("foundation-job-outcome unchanged", 0))

    def test_a_layout_failure_fails_at_once_even_when_published(self):
        with tempfile.TemporaryDirectory() as state:
            with unittest.mock.patch.object(check, "STATE", pathlib.Path(state)):
                line, status = check.outcome(self.NOW, True, False, "published", ["gold.t: misplaced"])
        self.assertEqual(status, 1)
        self.assertIn("1 tables are laid out", line)


def key(region):
    """A synthetic PNU in `region` (its first two digits); never a real parcel."""
    return f"{region}99999999999999999"


class Layout(unittest.TestCase):
    """A table readers prune by key fails when a one-region read opens most of its files, or when
    a commit added far more of its bytes than the keys it changed (root ADR-0314)."""

    # gold.parcel_panel on 2026-10-28: all 9 files span 시도 11..52.
    COUNTRY_WIDE = [(key("11"), key("52"))] * 9
    # gold.parcel_characteristics the same day: 11 files in PNU order.
    ORDERED = [(key(low), key(high)) for low, high in (
        ("11", "31"), ("31", "41"), ("41", "43"), ("43", "44"), ("44", "46"), ("46", "46"),
        ("46", "47"), ("47", "48"), ("48", "51"), ("51", "52"), ("52", "52"))]

    def summary(self, added, total, records=1_000_000, keys=None):
        found = {"added-files-size": str(added), "total-files-size": str(total), "total-records": str(records)}
        return found if keys is None else {**found, check.KEYS_CHANGED: str(keys)}

    def test_files_that_each_span_the_country_are_flagged(self):
        self.assertEqual(check.region_read_share(self.COUNTRY_WIDE, 2), 1.0)
        failures, _ = check.layout_findings("gold.t", "pnu", self.COUNTRY_WIDE, None)
        self.assertEqual(len(failures), 1)
        self.assertIn("opens 100% of its 9 data files", failures[0])

    def test_files_in_key_order_pass(self):
        self.assertLess(check.region_read_share(self.ORDERED, 2), 0.3)
        self.assertEqual(check.layout_findings("gold.t", "pnu", self.ORDERED, None), ([], []))

    def test_two_files_in_key_order_pass_and_two_country_wide_ones_fail(self):
        # 2026-11-13 gold.parcel_real_transaction: range-written in pnu order, 2 files that meet in 시도 41.
        ordered = [("1100000000000000000", "4199999999999999999"), ("4100000000000000000", "5299999999999999999")]
        failures, _ = check.layout_findings("gold.t", "pnu", ordered, None)
        self.assertEqual(failures, [])
        failures, _ = check.layout_findings("gold.t", "pnu", self.COUNTRY_WIDE[:2], None)
        self.assertEqual(len(failures), 1)

    def test_one_file_is_never_misplaced(self):
        self.assertEqual(check.region_read_share(self.COUNTRY_WIDE[:1], 2), 0.0)

    def test_a_tiny_change_that_rewrote_the_table_fails(self):
        # 2026-10-26: 48,477 of 39,861,511 PNUs changed and 100% of the bytes were added.
        failures, _ = check.layout_findings("gold.t", "pnu", self.ORDERED,
                                            self.summary(4_265_500_804, 4_265_500_804, 39_861_511, 48_477))
        self.assertEqual(len(failures), 1)
        self.assertIn("changed 0.12% of its keys and added 100% of its bytes", failures[0])

    def test_a_full_rebuild_that_says_what_it_changed_is_named_not_failed(self):
        # Root ADR-0347: a whole overwrite records its changed keys; 2026-10-29 gold.building_panel
        # rewrote 5,945,309 PNUs for 0.17% of real change. The full rebuild is a declared exception.
        failures, notes = check.layout_findings("gold.t", "pnu", self.ORDERED,
                                                self.summary(100, 100, 1_000_000, 1_700), "overwrite")
        self.assertEqual(failures, [])
        self.assertIn("full rebuild that changed 0.17% of its keys", notes[0])

    def test_a_change_that_rewrote_its_share_passes(self):
        self.assertEqual(check.layout_findings("gold.t", "pnu", self.ORDERED,
                                               self.summary(50, 100, 1_000, 500)), ([], []))

    def test_the_floor_keeps_a_small_change_from_being_judged_on_noise(self):
        # 0.01% of the keys, 5% of the bytes: under 10x the 1% floor.
        self.assertEqual(check.layout_findings("gold.t", "pnu", self.ORDERED,
                                               self.summary(5, 100, 1_000_000, 100)), ([], []))

    def test_a_rewrite_that_does_not_say_what_it_changed_is_named_not_failed(self):
        failures, notes = check.layout_findings("gold.t", "pnu", self.ORDERED, self.summary(100, 100))
        self.assertEqual(failures, [])
        self.assertIn(check.KEYS_CHANGED, notes[0])

    def test_the_real_contracts_cluster_the_served_gold_by_pnu(self):
        contracts = json.loads((PLATFORM / check.CONTRACTS).read_text(encoding="utf-8"))["contracts"]
        clustered = check.clustered_tables(contracts)
        self.assertEqual(clustered.get("gold.parcel_panel"), "pnu")
        ranged = {name for name, spec in contracts.items() if spec["write_distribution"] == "range"}
        # Every range-written table has a cluster key the check knows a region of.
        self.assertEqual(set(clustered), ranged)

    def test_a_derived_change_log_is_not_judged(self):
        # gold.<table>__changes (root ADR-0191 §1) is read by snapshot, never pruned by key range.
        contracts = {"gold.a__changes": {"write_distribution": "range", "sort_order": ["pnu"]}}
        self.assertEqual(check.clustered_tables(contracts), {})

    def test_layout_reads_only_metadata_tables_and_skips_tables_without_rows(self):
        contracts = {"gold.a": {"write_distribution": "range", "sort_order": ["pnu"]},
                     "gold.b": {"write_distribution": "range", "sort_order": ["pnu"]},
                     "silver.c": {"write_distribution": "hash", "sort_order": ["pnu"]}}
        shape = {"has_rows": True, "fields": [[7, "pnu", "string", True]]}
        lakehouse = {"tables": {"gold.a": shape, "gold.b": {**shape, "has_rows": False}, "silver.c": shape}}
        asked = []

        def rows(url, sql):
            asked.append(sql)
            return [[low, high] for low, high in self.COUNTRY_WIDE] if "$files" in sql else []

        failures, _ = check.layout(rows, "http://trino", contracts, lakehouse)
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0].startswith("gold.a:"))
        self.assertEqual(len(asked), 2)
        self.assertTrue(all('"a$files"' in sql or '"a$snapshots"' in sql for sql in asked))
        self.assertIn("element_at(lower_bounds, 7)", asked[0])


class Publish(unittest.TestCase):
    """Root ADR-0476: each new shape once, the head every run, the history when the pair changed."""

    KEY = {"endpoint": "https://account.example.invalid", "bucket": "ledger-bucket", "prefix": "shape-ledger/",
           "access_key_id": "key-id", "secret_access_key": "key-secret", "session_token": "session"}
    NOW = datetime(2026, 11, 11, 12, 30, tzinfo=timezone.utc)

    def files(self, captured="x"):
        return {"lakehouse-production-shape.json": json.dumps({"captured_at": captured, "tables": {"t": {}}}),
                "postgres-production-shape.json": json.dumps({"captured_at": captured, "tables_with_rows": []})}

    def publish(self, files, previous=None, key=KEY, r2=None, now=NOW):
        runtime = tempfile.TemporaryDirectory()
        self.addCleanup(runtime.cleanup)
        if key is not None:
            (pathlib.Path(runtime.name) / check.WRITER_KEY).write_text(json.dumps(key), encoding="utf-8")
        environment = {"RUNTIME_DIRECTORY": runtime.name}
        with unittest.mock.patch.dict(check.os.environ, environment, clear=True), \
                unittest.mock.patch.object(check, "release_module", return_value=r2) as module:
            return check.publish(files, now, previous), module

    def test_without_a_minted_key_nothing_is_sent(self):
        (said, head, _), module = self.publish(self.files(), key=None)
        module.assert_not_called()
        self.assertIsNone(head)
        self.assertIn("no writer key", said)

    def test_shapes_go_by_their_hash_only_when_new_then_the_history_then_the_head(self):
        r2 = unittest.mock.Mock()
        r2.put.return_value = True
        (_, head, written), _ = self.publish(self.files(), r2=r2)
        keys = [(c.args[2], c.kwargs["only_new"]) for c in r2.put.call_args_list]
        digest = check.shape_digest(self.files()["lakehouse-production-shape.json"])
        self.assertEqual(keys[0], (f"shape-ledger/objects/lakehouse-production-shape/{digest}.json", True))
        self.assertEqual(keys[2], ("shape-ledger/history/2026-11-11T12:30:00Z.json", True))
        self.assertEqual(keys[3], (check.HEAD, False))
        self.assertTrue(all(c.kwargs["session_token"] == "session" for c in r2.put.call_args_list))
        self.assertTrue(written)
        self.assertEqual(head["checked_at"], "2026-11-11T12:30:00Z")
        self.assertEqual(head["schema_version"], check.HEAD_SCHEMA)

    def test_the_same_shape_captured_48_hours_later_writes_only_the_head(self):
        r2 = unittest.mock.Mock()
        r2.put.return_value = True
        (_, first, _), _ = self.publish(self.files(), r2=r2)
        r2.reset_mock()
        r2.put.return_value = False  # the object exists: If-None-Match refused it
        (_, head, written), _ = self.publish(self.files("y"), previous=first, r2=r2, now=self.NOW + timedelta(hours=48))
        self.assertEqual([c.args[2] for c in r2.put.call_args_list if not c.kwargs["only_new"]], [check.HEAD])
        self.assertFalse(any("history" in c.args[2] for c in r2.put.call_args_list))
        self.assertFalse(written)
        self.assertEqual(head["ledgers"], first["ledgers"])
        self.assertEqual(head["checked_at"], "2026-11-13T12:30:00Z")

    def test_a_shape_the_locked_bucket_already_keeps_is_not_written_again(self):
        # 2026-11-11..13: R2 answers a conditional PUT on a key under a lock rule with 409, not 412,
        # and every run after the first published nothing. The writer asks first.
        sent = []

        def answer(request, timeout):
            sent.append(request.get_method())
            if request.get_method() == "HEAD":
                return contextlib.nullcontext()
            raise AssertionError("an object the bucket keeps was written again")

        with unittest.mock.patch.object(r2_object.urllib.request, "urlopen", side_effect=answer):
            self.assertFalse(r2_object.put("https://a.r2.cloudflarestorage.com", "b", "shape-ledger/objects/x.json",
                                           "k", "s", b"{}", only_new=True))
        self.assertEqual(sent, ["HEAD"])

        def absent(request, timeout):
            sent.append(request.get_method())
            if request.get_method() == "HEAD":
                raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)
            self.assertEqual(request.get_header("If-none-match"), "*")
            return contextlib.nullcontext()

        sent.clear()
        with unittest.mock.patch.object(r2_object.urllib.request, "urlopen", side_effect=absent):
            self.assertTrue(r2_object.put("https://a.r2.cloudflarestorage.com", "b", "shape-ledger/objects/y.json",
                                          "k", "s", b"{}", only_new=True))
        self.assertEqual(sent, ["HEAD", "PUT"])

    def test_a_failed_upload_is_said_and_does_not_fail_the_run_by_itself(self):
        r2 = unittest.mock.Mock()
        r2.put.side_effect = OSError("connection reset")
        (said, head, _), _ = self.publish(self.files(), r2=r2)
        self.assertIsNone(head)
        self.assertIn("not published (connection reset)", said)

    def test_the_shape_hash_ignores_the_capture_time_and_nothing_else(self):
        one = json.dumps({"captured_at": "a", "tables": {"t": 1}})
        self.assertEqual(check.shape_digest(one),
                         check.shape_digest(json.dumps({"tables": {"t": 1}, "captured_at": "b"})))
        self.assertNotEqual(check.shape_digest(one),
                            check.shape_digest(json.dumps({"captured_at": "a", "tables": {"t": 2}})))


writer_spec = importlib.util.spec_from_file_location("shape_ledger_writer_key",
                                                     PLATFORM / "scripts/ops/shape-ledger-writer-key.py")
writer = importlib.util.module_from_spec(writer_spec)
writer_spec.loader.exec_module(writer)


class WriterKey(unittest.TestCase):
    """The unit's root ExecStartPre mints a 20-minute key for shape-ledger/ of the bucket only."""

    def test_it_asks_for_object_read_write_on_the_prefix_for_twenty_minutes(self):
        asked = []

        def call(token, method, path, body=None):
            asked.append((method, path, body))
            if path == "/user/tokens/verify":
                return 200, {"result": {"id": "token-id"}}
            return 200, {"success": True, "result": {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}}

        key = writer.mint("tok", "acct", "ledger-bucket", call)
        body = asked[1][2]
        self.assertEqual(asked[1][1], "/accounts/acct/r2/temp-access-credentials")
        self.assertEqual((body["bucket"], body["parentAccessKeyId"], body["permission"], body["prefixes"]),
                         ("ledger-bucket", "token-id", "object-read-write", ["shape-ledger/"]))
        self.assertEqual(body["ttlSeconds"], 1200)
        self.assertEqual(key["endpoint"], "https://acct.r2.cloudflarestorage.com")
        self.assertTrue(check.OBJECTS.startswith(writer.PREFIX) and check.HEAD.startswith(writer.PREFIX))

    def test_a_refusal_names_the_status_and_codes_never_a_value(self):
        def call(token, method, path, body=None):
            return (200, {"result": {"id": "token-id"}}) if "verify" in path else (403, {"errors": [{"code": 10000}]})

        with self.assertRaisesRegex(RuntimeError, "HTTP 403, codes .10000."):
            writer.mint("tok", "acct", "ledger-bucket", call)

    def test_it_reads_the_token_from_the_cloudflare_deploy_group_and_the_bucket_from_the_contract(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(writer.account_token(PLATFORM, pathlib.Path(root)), ("", ""))
            path = pathlib.Path(root) / "etc/foundation-platform/cloudflare-deploy.env"
            path.parent.mkdir(parents=True)
            path.write_text("CLOUDFLARE_ACCOUNT_ID=acct\nFOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN=tok\n", encoding="utf-8")
            self.assertEqual(writer.account_token(PLATFORM, pathlib.Path(root)), ("tok", "acct"))
        self.assertEqual(writer.bucket(), "foundation-platform-shape-ledger-prod")

    def test_the_unit_mints_as_root_into_its_runtime_directory(self):
        unit = (PLATFORM / "infra/systemd/foundation-shape-ledger-check.service").read_text(encoding="utf-8")
        self.assertIn("ExecStartPre=+/usr/bin/python3 /opt/perfectory-control/current/platforms/foundation-platform/"
                      "scripts/ops/shape-ledger-writer-key.py", unit)
        self.assertIn("RuntimeDirectory=foundation-shape-ledger", unit)
        self.assertNotIn("cloudflare-deploy.env", unit, "the check's own process never loads the account token")


r2_spec = importlib.util.spec_from_file_location("r2_object", PLATFORM / "scripts/ops/r2_object.py")
r2_object = importlib.util.module_from_spec(r2_spec)
r2_spec.loader.exec_module(r2_object)


class SignatureVersion4(unittest.TestCase):
    def test_the_signature_is_the_s3_documentation_get_object_example(self):
        # "Example: GET Object" of the S3 Signature Version 4 documentation, its signature included.
        empty = hashlib.sha256(b"").hexdigest()
        request, signed = r2_object.canonical_request("GET", "/test.txt", {
            "host": "examplebucket.s3.amazonaws.com", "range": "bytes=0-9",
            "x-amz-content-sha256": empty, "x-amz-date": "20130524T000000Z"}, empty)
        self.assertEqual(signed, "host;range;x-amz-content-sha256;x-amz-date")
        when = datetime(2013, 5, 24, tzinfo=timezone.utc)
        self.assertEqual(r2_object.signature("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", when, "us-east-1", request),
                         "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")

    def test_a_put_carries_its_body_hash_and_a_path_style_url(self):
        when = datetime(2026, 10, 19, tzinfo=timezone.utc)
        request = r2_object.signed_request("PUT", "https://account.example.invalid", "bucket", "shape-ledger/a.json",
                                           "key-id", "key-secret", b"{}", when)
        self.assertEqual(request.full_url, "https://account.example.invalid/bucket/shape-ledger/a.json")
        self.assertEqual(request.get_header("X-amz-content-sha256"), hashlib.sha256(b"{}").hexdigest())
        self.assertTrue(request.get_header("Authorization").startswith(
            "AWS4-HMAC-SHA256 Credential=key-id/20261019/auto/s3/aws4_request, "))


if __name__ == "__main__":
    unittest.main()
