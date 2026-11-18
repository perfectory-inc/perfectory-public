"""The black-box probe of the live by-PNU edges (scripts/ops/by-pnu-edge-probe.py, root ADR-0327).

A local HTTP server plays the edge: each path answers a list of responses in turn, the last repeated.
2026-10-29 is the case it was built for: every onbid photo GET after the first answered 500.
PNUs sit in the reserved synthetic region 99999; hashes are of the fixture's own bytes.
"""

import contextlib
import gzip
import hashlib
import http.server
import importlib.util
import io
import json
import os
import pathlib
import tempfile
import threading
import unittest
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("by_pnu_edge_probe", PLATFORM / "scripts/ops/by-pnu-edge-probe.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)

CONTRACT = probe.load(probe.CONTRACT)
PARCEL = CONTRACT["parcel_by_pnu_gateway"]
JSON_TYPE = PARCEL["content_type"]
POLICY = CONTRACT["by_pnu_section_packs"]["edge_probe"]
MEDIA_LANE = POLICY["media"]["documents_lane"]
PNU_GONE, PNU = "9999900000100000000", "9999900000200000000"
PHOTO = b"\xff\xd8\xff synthetic photo"
SHA = hashlib.sha256(PHOTO).hexdigest()
BLOB = f"/onbid/media/sha256/{SHA}.jpg"


def document(value):
    return 200, {"Content-Type": JSON_TYPE, "Content-Encoding": "gzip"}, gzip.compress(json.dumps(value).encode())


DOCUMENT = document({"pnu": PNU})
WITH_PHOTO = document({"pnu": PNU, "public_auction": {"rounds": [{"photos": [{"sha256": SHA, "media_format": "jpg"}]}]}})
PHOTO_ANSWER = (200, {"Content-Type": "image/jpeg"}, PHOTO)
SERVER_ERROR = (500, {"Content-Type": "text/plain"}, b"")
# The connection closed with no answer, as a read that times out gets none.
NO_ANSWER = None


def prefix(lane):
    return next(p for name, p, _ in probe.served_lanes(CONTRACT, "parcel") if name == lane)


class Edge(http.server.BaseHTTPRequestHandler):
    routes: dict = {}
    seen: list = []

    def do_GET(self):  # noqa: N802 - the http.server name
        Edge.seen.append((self.path, self.headers.get("User-Agent")))
        answers = Edge.routes.get(self.path, [(404, {}, b"")])
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        if answer is NO_ANSWER:
            self.close_connection = True
            return
        status, headers, body = answer
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


class EdgeFixture(unittest.TestCase):
    """A local edge, two lanes' gate samples, and the probe run against them."""

    def setUp(self):
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Edge)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.base = f"http://127.0.0.1:{server.server_address[1]}"
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.temp = pathlib.Path(temp.name)
        Edge.seen = []
        Edge.routes = {
            f"{prefix('parcel')}{PNU_GONE}": [(404, {}, b"")],
            f"{prefix('parcel')}{PNU}": [DOCUMENT],
            f"{prefix(MEDIA_LANE)}{PNU}": [WITH_PHOTO],
            BLOB: [PHOTO_ANSWER],
        }
        self.env = {probe.sample_variable("parcel"): self.sample("parcel", [PNU]),
                    probe.sample_variable(MEDIA_LANE): self.sample(MEDIA_LANE, [PNU_GONE, PNU])}

    def sample(self, lane, keys):
        path = self.temp / f"{lane}.json"
        path.write_text(json.dumps({"sample": keys}))
        return str(path)

    def run_probe(self, env=None):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, env if env is not None else self.env, clear=True), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = probe.main(["--base-url", self.base, "parcel"])
        self.output = out.getvalue() + err.getvalue()
        return status

    def reads(self, path):
        return sum(1 for seen, _ in Edge.seen if seen == path)


class TheProbe(EdgeFixture):
    def test_a_healthy_edge_passes_and_every_document_and_blob_is_read_twice(self):
        self.assertEqual(self.run_probe(), 0, self.output)
        self.assertEqual(self.reads(f"{prefix('parcel')}{PNU}"), 1 + POLICY["reads"])
        self.assertEqual(self.reads(BLOB), POLICY["reads"])
        self.assertIn("media", self.output)
        self.assertEqual({agent for _, agent in Edge.seen}, {POLICY["user_agent"]})

    def test_2026_10_29_a_blob_that_fails_on_its_cache_hit_fails_the_probe(self):
        Edge.routes[BLOB] = [PHOTO_ANSWER, SERVER_ERROR]
        self.assertEqual(self.run_probe(), 1)
        self.assertIn(f"GET {BLOB} (read 2 of 2) answered 500", self.output)

    def test_a_document_that_fails_on_its_second_read_fails_the_probe(self):
        Edge.routes[f"{prefix('parcel')}{PNU}"] = [DOCUMENT, DOCUMENT, SERVER_ERROR]
        self.assertEqual(self.run_probe(), 1)
        self.assertIn("(read 2 of 2) answered 500", self.output)

    def test_2026_11_10_a_gzip_encoded_blob_is_held_to_its_name_by_its_decoded_bytes(self):
        # CloudFront gzips a PDF for a client that accepts gzip; the PDF is whole, its encoding is not it.
        Edge.routes[BLOB] = [(200, {"Content-Type": "image/jpeg", "Content-Encoding": "gzip"}, gzip.compress(PHOTO))]
        self.assertEqual(self.run_probe(), 0, self.output)
        Edge.routes[BLOB] = [(200, {"Content-Type": "image/jpeg", "Content-Encoding": "gzip"}, gzip.compress(b"other"))]
        self.assertEqual(self.run_probe(), 1)
        self.assertIn(f"not the {SHA} in the name", self.output)

    def test_2026_11_11_a_read_with_no_answer_is_asked_again_and_an_answer_is_judged_at_once(self):
        path = f"{prefix('parcel')}{PNU}"
        # The scan's read, read 1, then read 2 gets no answer once: asked again, it passes.
        Edge.routes[path] = [DOCUMENT, DOCUMENT, NO_ANSWER, DOCUMENT]
        self.assertEqual(self.run_probe(), 0, self.output)
        self.assertEqual(self.reads(path), 1 + POLICY["reads"] + 1)
        self.assertIn("no answer", self.output)
        # No answer every time is a failure after transport_tries asks.
        Edge.seen = []
        Edge.routes[path] = [DOCUMENT, DOCUMENT, *[NO_ANSWER] * POLICY["transport_tries"], NO_ANSWER]
        self.assertEqual(self.run_probe(), 1)
        self.assertIn(f"(read 2 of 2) answered", self.output)
        self.assertIn(f"({POLICY['transport_tries']} tries)", self.output)
        # An answer, even a 500, is the edge's: never asked again.
        Edge.seen = []
        Edge.routes[path] = [DOCUMENT, DOCUMENT, SERVER_ERROR]
        self.assertEqual(self.run_probe(), 1)
        self.assertEqual(self.reads(path), 1 + POLICY["reads"])

    def test_a_blob_whose_bytes_do_not_hash_to_its_name_fails(self):
        Edge.routes[BLOB] = [(200, {"Content-Type": "image/jpeg"}, b"other bytes")]
        self.assertEqual(self.run_probe(), 1)
        self.assertIn(f"not the {SHA} in the name", self.output)

    def test_a_document_of_another_content_type_or_empty_fails(self):
        for answer in [(200, {"Content-Type": "text/html"}, b"<html>"), (200, {"Content-Type": JSON_TYPE}, b"")]:
            with self.subTest(answer[1]["Content-Type"]):
                Edge.routes[f"{prefix('parcel')}{PNU}"] = [answer]
                self.assertEqual(self.run_probe(), 1)

    def test_an_ended_entry_is_passed_over_for_the_first_one_served(self):
        # The public auction sample's first PNU answers 404 (its auction ended): not a failure.
        self.assertEqual(self.run_probe(), 0, self.output)
        self.assertEqual(self.reads(f"{prefix(MEDIA_LANE)}{PNU_GONE}"), 2)

    def test_a_sample_none_of_whose_entries_is_served_fails(self):
        env = {**self.env, probe.sample_variable("parcel"): self.sample("parcel", [PNU_GONE])}
        self.assertEqual(self.run_probe(env), 1)
        self.assertIn("parcel: none of the first", self.output)

    def test_no_named_blob_skips_the_media_read_and_says_so(self):
        Edge.routes[f"{prefix(MEDIA_LANE)}{PNU}"] = [DOCUMENT]
        self.assertEqual(self.run_probe(), 0, self.output)
        self.assertEqual(self.reads(BLOB), 0)
        self.assertIn("names a blob; nothing to read", self.output)

    def test_a_lane_not_cut_over_is_skipped_and_nothing_probed_fails(self):
        env = {probe.sample_variable("parcel"): self.env[probe.sample_variable("parcel")]}
        self.assertEqual(self.run_probe(env), 0, self.output)
        self.assertIn(f"{MEDIA_LANE}: {probe.sample_variable(MEDIA_LANE)} is not set", self.output)
        self.assertEqual(self.run_probe({}), 1)
        self.assertIn("no lane was probed", self.output)


class TheContract(unittest.TestCase):
    def test_a_kind_lane_is_served_under_its_hosts_prefix_with_its_kind(self):
        lanes = {name: path for name, path, _ in probe.served_lanes(CONTRACT, "parcel")}
        self.assertEqual(lanes["parcel"], PARCEL["request_path"]["prefix"])
        kind = CONTRACT["by_pnu_lanes"][MEDIA_LANE]["kind"]
        self.assertEqual(lanes[MEDIA_LANE], PARCEL["request_path"]["prefix"].replace("/by-pnu/", f"/{kind}/by-pnu/"))
        self.assertEqual(CONTRACT["by_pnu_lanes"][MEDIA_LANE]["host"], "parcel_by_pnu_gateway")
        self.assertEqual(CONTRACT["parcel_media"]["lane"], "parcel")

    def test_a_key_with_a_suffix_is_requested_as_pnu_then_suffix(self):
        self.assertEqual(probe.request_path_of(PNU), PNU)
        self.assertEqual(probe.request_path_of(PNU + "00000000-0000-0000-0000-000000000000"),
                         f"{PNU}/00000000-0000-0000-0000-000000000000")

    def test_the_timer_probes_every_by_pnu_worker_on_its_production_hostname(self):
        workers = dict(probe.production_workers(CONTRACT))
        self.assertEqual(set(workers), {"parcel", "building", "unit-price"})
        self.assertEqual(workers["parcel"], PARCEL["public_hostname"])

    def test_the_media_key_follows_the_served_media_rule(self):
        keys = list(probe.media_keys(json.loads(gzip.decompress(WITH_PHOTO[2])), "onbid/media"))
        self.assertEqual(keys, [BLOB.lstrip("/")])
        self.assertRegex(keys[0], f"^(?:{CONTRACT['parcel_media']['path_pattern']})$")
        self.assertEqual(probe.load(probe.ONBID)["media"]["object_prefix"], "onbid/media")


class TheUnits(unittest.TestCase):
    def test_the_timer_runs_the_probe_every_quarter_hour_and_a_failure_reaches_slack(self):
        systemd = PLATFORM / "infra/systemd"
        timer = (systemd / "foundation-by-pnu-edge-probe.timer").read_text(encoding="utf-8")
        unit = (systemd / "foundation-by-pnu-edge-probe.service").read_text(encoding="utf-8")
        self.assertIn("OnCalendar=*-*-* *:07/15:00\n", timer)
        self.assertIn("Unit=foundation-by-pnu-edge-probe.service\n", timer)
        self.assertIn("OnFailure=foundation-unit-failed@%n.service\n", unit)
        self.assertIn("/opt/foundation-platform/current/scripts/ops/by-pnu-edge-probe.py\n", unit)
        # Public hostnames only: no file with a credential.
        files = [line.split("=", 1)[1] for line in unit.splitlines() if line.startswith("EnvironmentFile=")]
        self.assertTrue(files and all(f.startswith("-/etc/foundation-platform/") and f.endswith("-serving-monitor.env")
                                      for f in files), files)
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertRegex(release, r"(?m)^\s*systemctl enable --now .*\bfoundation-by-pnu-edge-probe\.timer$")

    def test_the_worker_deploy_runs_this_probe_and_retries_when_it_changes(self):
        deploy = (PLATFORM / "scripts/deploy/worker_autodeploy.py").read_text(encoding="utf-8")
        self.assertIn('"scripts/ops/by-pnu-edge-probe.py"', deploy)
        self.assertIn('PROBE_UNIT = "foundation-by-pnu-edge-probe.service"', deploy)


if __name__ == "__main__":
    unittest.main()


class TheWeeklyScreen(EdgeFixture):
    """The weekly synthetic screen (root ADR-0480): for the sample PNUs, every section of the screen."""

    FRESH = ('foundation_by_pnu_served_manifest_age_seconds{lane="parcel"} 3600\n'
             'foundation_by_pnu_serving_freshness_budget_seconds{lane="parcel"} 172800\n'
             f'foundation_by_pnu_served_manifest_age_seconds{{lane="{MEDIA_LANE}"}} 3600\n'
             f'foundation_by_pnu_serving_freshness_budget_seconds{{lane="{MEDIA_LANE}"}} 172800\n')

    def read_screen(self, textfiles=FRESH, env=None):
        with mock.patch.dict(os.environ, env if env is not None else self.env, clear=True):
            return probe.screen(CONTRACT, [("parcel", self.base)], textfiles)

    def test_every_section_is_read_once_for_each_sample_pnu_and_a_healthy_screen_passes(self):
        rows, failures = self.read_screen()
        self.assertEqual(failures, [])
        lanes = [name for name, _, _ in probe.served_lanes(CONTRACT, "parcel")]
        self.assertEqual({row["pnu"] for row in rows}, {PNU})
        self.assertEqual(sorted(row["lane"] for row in rows), sorted(lanes))
        verdicts = {row["lane"]: row["verdict"] for row in rows}
        self.assertEqual((verdicts["parcel"], verdicts[MEDIA_LANE]), ("shown", "shown"))
        self.assertEqual({verdicts[lane] for lane in lanes if lane not in ("parcel", MEDIA_LANE)}, {"absent"})

    def test_a_section_the_edge_fails_and_the_pnus_own_section_gone_fail_the_screen(self):
        other = next(name for name, _, _ in probe.served_lanes(CONTRACT, "parcel") if name not in ("parcel", MEDIA_LANE))
        Edge.routes[f"{prefix(other)}{PNU}"] = [SERVER_ERROR]
        rows, failures = self.read_screen()
        self.assertTrue(any(failure.startswith(f"{other} for a sample PNU of parcel: error") for failure in failures), failures)
        # The parcel lane served the PNU when it was picked, then answers 404: its own section is missing.
        Edge.routes[f"{prefix('parcel')}{PNU}"] = [DOCUMENT, (404, {}, b"")]
        rows, failures = self.read_screen()
        self.assertIn("missing", {row["verdict"] for row in rows if row["lane"] == "parcel"})

    def test_a_lane_that_shows_data_past_its_budget_fails_and_one_behind_nothing_does_not(self):
        stale = self.FRESH.replace('age_seconds{lane="parcel"} 3600', 'age_seconds{lane="parcel"} 900000')
        self.assertIn("parcel shows data older than its freshness budget", self.read_screen(stale)[1])
        # Root ADR-0444: an old manifest that is behind none of its inputs is not stale.
        behind_nothing = stale + 'foundation_by_pnu_lane_behind_seconds{lane="parcel"} 0\n'
        self.assertEqual(self.read_screen(behind_nothing)[1], [])

    def test_the_textfile_counts_sections_by_lane_and_verdict(self):
        rows, _ = self.read_screen()
        text = probe.screen_textfile(rows, 1_800_000_000)
        self.assertIn('foundation_screen_probe_sections{lane="parcel",verdict="shown"} 1', text)
        self.assertIn("foundation_screen_probe_pnus 1", text)
        self.assertIn("foundation_screen_probe_timestamp_seconds 1800000000", text)

    def test_the_weekly_timer_runs_the_screen_and_a_failure_reaches_its_owner(self):
        systemd = PLATFORM / "infra/systemd"
        timer = (systemd / "foundation-by-pnu-screen-probe.timer").read_text(encoding="utf-8")
        unit = (systemd / "foundation-by-pnu-screen-probe.service").read_text(encoding="utf-8")
        self.assertIn("OnCalendar=Mon *-*-* 23:23:00\n", timer)
        self.assertIn("by-pnu-edge-probe.py --screen\n", unit)
        self.assertIn("OnFailure=foundation-unit-failed@%n.service\n", unit)
        self.assertIn("X-Foundation-Owner=serving\n", unit)
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertRegex(release, r"(?m)^\s*systemctl enable --now foundation-by-pnu-screen-probe\.timer$")
