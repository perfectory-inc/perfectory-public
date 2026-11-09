"""The host's service checks (root ADR-0209): what scripts/ops/service-checks.py measures and writes.

The hostnames come from the gateways' contract, the CI trend from root ADR-0203's own script, and
the file it writes has exactly the shape foundation-api reads (the example both sides test with).
"""

import importlib.util
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
REPOSITORY = PLATFORM.parents[1]
EXAMPLE = PLATFORM / "services/foundation-api/src/routes/data_catalog/service-checks.example.json"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checks = load(PLATFORM / "scripts/ops/service-checks.py", "service_checks")
ci_budget = load(REPOSITORY / "scripts/ci/ci_time_budget.py", "ci_time_budget")


def shape(value):
    """Keys all the way down; a list's shape is its first item's."""
    if isinstance(value, dict):
        return {key: shape(item) for key, item in value.items()}
    if isinstance(value, list):
        return [shape(value[0])] if value else []
    return None


class Edge(unittest.TestCase):
    def test_hostnames_are_the_three_gateways_live_names_from_the_contract(self):
        contract = json.loads((PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
        names = checks.hostnames()
        self.assertEqual(len(names), len(set(names)))
        for gateway in ("parcel_by_pnu_gateway", "building_by_pnu_gateway", "vector_tile_gateway"):
            self.assertIn(contract[gateway]["public_hostname"], names)
            for alias in contract[gateway]["public_hostname_aliases"]:
                self.assertIn(alias, names)
        self.assertNotIn(contract["map_edit_gateway"]["public_hostname"], names)
        self.assertFalse(any("preview" in name for name in names), "previews are not what visitors reach")

    def test_colo_is_read_from_the_trace(self):
        self.assertEqual(checks.colo_of("fl=1\nh=x\nip=192.0.2.1\ncolo=SEA\nhttp=http/2\n"), "SEA")
        self.assertIsNone(checks.colo_of("fl=1\nh=x\n"))

    def test_a_hostname_behind_the_front_door_is_entered_at_cloudfronts_pop(self):
        # Root ADR-0461: through CloudFront the trace's colo is where CloudFront entered Cloudflare.
        class Answer:
            def __init__(self, pop):
                self.headers = {"x-amz-cf-pop": pop} if pop else {}

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return b"fl=1\ncolo=ICN\n"

        original = checks.urllib.request.urlopen
        self.addCleanup(setattr, checks.urllib.request, "urlopen", original)
        for pop, entry in (("ICN57-P5", "ICN57-P5"), (None, "ICN")):
            checks.urllib.request.urlopen = lambda *_args, pop=pop, **_kwargs: Answer(pop)
            self.assertEqual(checks.trace("h")[1], entry)

    def test_percentiles_are_nearest_rank_and_the_colo_is_the_most_common(self):
        answers = iter([(t, "SEA" if t < 900 else "ICN") for t in (100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)])
        host = checks.measure_host("h", lambda _host: next(answers))
        self.assertEqual((host["p50_ms"], host["p95_ms"], host["colo"], host["samples"]), (500, 1000, "SEA", 10))
        self.assertIsNone(host["error"])

    def test_a_failed_sample_is_counted_and_all_failing_is_an_error(self):
        def refuse(_host):
            raise TimeoutError("timed out")

        host = checks.measure_host("h", refuse, samples=3)
        self.assertEqual((host["samples"], host["failed_samples"], host["p50_ms"]), (0, 3, None))
        self.assertIn("timed out", host["error"])


class CiTrend(unittest.TestCase):
    NOW = datetime(2026, 10, 14, 6, tzinfo=timezone.utc)  # a Wednesday

    def run_day(self, day, minutes):
        start = day + timedelta(hours=10)
        return {
            "name": "CI", "head_sha": f"{day:%m%d}", "head_branch": "b", "conclusion": "success",
            "run_started_at": start.isoformat(), "updated_at": (start + timedelta(minutes=minutes)).isoformat(),
        }

    def measure(self, store, remaining):
        fetched = []

        class Fake:
            week_start = staticmethod(ci_budget.week_start)
            group_attempts = staticmethod(ci_budget.group_attempts)
            weekly_trend = staticmethod(ci_budget.weekly_trend)

            @staticmethod
            def fetch_runs(_repo, day, _end, _token):
                fetched.append(day)
                return [CiTrend.run_day(self, day, 12)]

        result = checks.measure_ci(store, self.NOW, Fake, "owner/repo", 10.0, remaining=lambda: remaining)
        return result, fetched

    def test_days_are_fetched_newest_first_and_a_week_is_reported_only_when_whole(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pathlib.Path(tmp)
            result, fetched = self.measure(store, remaining=60)
            # Every day of the eight weeks and the Monday after; the current week is not asked for.
            self.assertEqual(len(fetched), checks.CI_WEEKS * 7 + 1)
            self.assertEqual(fetched, sorted(fetched, reverse=True))
            self.assertEqual(len(result["weeks"]), checks.CI_WEEKS)
            self.assertEqual(result["weeks"][-1]["week_start"], "2026-10-05")
            self.assertEqual((result["weeks"][-1]["count"], result["weeks"][-1]["p50"]), (7, 12.0))
            self.assertNotIn("attempts", result["weeks"][-1])
            self.assertEqual(result["pending_days"], 0)
            # A second run asks GitHub nothing.
            _, again = self.measure(store, remaining=60)
            self.assertEqual(again, [])

    def test_the_autodeploys_share_of_the_hour_is_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, fetched = self.measure(pathlib.Path(tmp), remaining=checks.API_RESERVE + checks.REQUESTS_PER_DAY - 1)
            self.assertEqual(fetched, [])
            self.assertEqual(result["weeks"], [])
            self.assertEqual(result["pending_days"], checks.CI_WEEKS * 7 + 1)


class Host(unittest.TestCase):
    def test_the_release_makes_the_directory_the_unit_writes_and_foundation_api_mounts(self):
        directory = str(pathlib.PurePosixPath(checks.OUT).parent)
        unit = (PLATFORM / "infra/systemd/foundation-service-checks.service").read_text(encoding="utf-8")
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        compose = (PLATFORM / "compose.orchestration-bridge.yml").read_text(encoding="utf-8")
        self.assertIn(f"ReadWritePaths={directory}\n", unit)
        self.assertIn("OnFailure=foundation-unit-failed@%n.service", unit)
        self.assertIn(f"install -d -o foundation-platform -g foundation-platform -m 0755 {directory}\n", release)
        self.assertIn("systemctl enable --now foundation-service-checks.timer\n", release)
        self.assertIn(f"- {directory}:/run/foundation-service-checks:ro\n", compose)
        self.assertIn(
            f"FOUNDATION_PLATFORM_SERVICE_CHECKS_PATH: /run/foundation-service-checks/{pathlib.PurePosixPath(checks.OUT).name}\n",
            compose,
        )


class Security(unittest.TestCase):
    """Root ADR-0278: the open advisories, each source on its own, labels that stay the same."""

    REPOSITORY = "example-org/example-repo"
    NOW = datetime(2026, 10, 13, 2, 17, tzinfo=timezone.utc)

    @staticmethod
    def github(cargo_fails=False, run_ids=None, calls=None, main_runs=None, change_runs=(), pull_files=None,
               silent=()):
        """A fake GitHub: two alerts on one advisory and package, a failed OSV run that annotated one
        advisory (and a note that is not one), and a Cargo run that cannot be read. `main_runs`
        replaces main's run list, `change_runs` are the failed pull request and merge-queue runs,
        `pull_files` each pull request's files, and a run in `silent` failed without an annotation."""
        run_ids = run_ids or {"monorepo-guard.yml": 11, "dependency-advisories.yml": 22}
        pull_files = pull_files or {}
        alert = lambda number: {"number": number, "html_url": f"https://github.com/x/security/dependabot/{number}",
                                "dependency": {"package": {"ecosystem": "npm", "name": "example-package"}},
                                "security_advisory": {"ghsa_id": "GHSA-aaaa-bbbb-cccc", "severity": "high"}}

        def ask(path, token=None):
            if calls is not None:
                calls.append((path, token))
            if "/dependabot/alerts" in path:
                return [alert(7), alert(3)], {"github-authentication-token-expiration": "2027-10-13 00:00:00 UTC"}
            if cargo_fails and "dependency-advisories.yml" in path:
                raise OSError("timed out")
            for workflow, run_id in run_ids.items():
                if f"/workflows/{workflow}/runs?status=failure" in path:
                    return {"workflow_runs": list(change_runs)}, {}
                if f"/workflows/{workflow}/runs" in path:
                    return {"workflow_runs": main_runs if main_runs is not None else [
                        {"id": run_id, "event": "schedule", "conclusion": "failure", "created_at": "2026-10-13T00:00:00Z"}]}, {}
            if "/pulls/" in path:
                number = int(path.split("/pulls/")[1].split("/")[0])
                return [{"filename": name} for name in pull_files.get(number, ["README.md"])], {}
            if "/jobs" in path:
                run_id = int(path.split("/runs/")[1].split("/")[0])
                if any(run["id"] == run_id and run["conclusion"] == "success" for run in main_runs or []):
                    return {"jobs": [{"id": 1, "conclusion": "success"}]}, {}
                return {"jobs": [{"id": 1, "conclusion": "success"}, {"id": 3 if run_id in silent else 2, "conclusion": "failure"}]}, {}
            if "/check-runs/3/annotations" in path:
                return [{"title": "", "message": "Process completed with exit code 1."}], {}
            if "/check-runs/2/annotations" in path:
                return [{"title": "advisory", "message": "GHSA-dddd-eeee-ffff crates.io/example-crate"},
                        {"title": "", "message": "Node.js 20 is deprecated."},
                        {"title": "advisory", "message": "three words here"}], {}
            raise AssertionError(f"asked {path}")
        return ask

    @classmethod
    def measure(cls, ask, token=None, previous=None, remaining=60):
        return checks.measure_security(cls.REPOSITORY, token, previous, cls.NOW, ask=ask, remaining=lambda: remaining)

    def test_dependabot_is_one_finding_per_advisory_and_package_with_a_stable_link(self):
        security = self.measure(self.github(), token="t")
        dependabot = security["sources"]["dependabot"]
        self.assertTrue(dependabot["readable"])
        self.assertEqual(dependabot["findings"], [{"advisory": "GHSA-aaaa-bbbb-cccc", "package": "npm/example-package",
                                                   "severity": "high", "link": "https://github.com/x/security/dependabot/3"}])
        self.assertEqual(security["token_expires_at"], "2027-10-13 00:00:00 UTC")

    def test_without_the_token_dependabot_says_so_and_the_ratchets_still_report(self):
        calls = []
        security = self.measure(self.github(calls=calls))
        self.assertEqual(security["sources"]["dependabot"]["reason"], "no-token")
        self.assertTrue(security["sources"]["osv-ratchet"]["readable"])
        self.assertFalse(any("/dependabot/" in path for path, _ in calls))

    def test_the_ratchets_are_read_anonymously_from_their_failed_jobs_annotations(self):
        calls = []
        osv = self.measure(self.github(calls=calls), token="t")["sources"]["osv-ratchet"]
        self.assertEqual([(f["advisory"], f["package"]) for f in osv["findings"]],
                         [("GHSA-dddd-eeee-ffff", "crates.io/example-crate")])
        self.assertEqual(osv["findings"][0]["link"],
                         "https://github.com/example-org/example-repo/actions/workflows/monorepo-guard.yml?query=branch%3Amain")
        self.assertFalse(any("/check-runs/1/" in path for path, _ in calls), "a job that passed is not asked")
        self.assertTrue(all(token is None for path, token in calls if "/dependabot/" not in path))

    def test_a_run_already_read_is_not_read_again(self):
        first = self.measure(self.github())
        calls = []
        again = self.measure(self.github(calls=calls), previous=first)
        self.assertEqual(again["sources"]["osv-ratchet"], first["sources"]["osv-ratchet"])
        self.assertFalse(any("/jobs" in path or "check-runs" in path for path, _ in calls), calls)

    def test_one_source_that_fails_leaves_the_others_and_says_why(self):
        sources = self.measure(self.github(cargo_fails=True), token="t")["sources"]
        self.assertEqual((sources["cargo-advisories"]["readable"], sources["cargo-advisories"]["reason"]),
                         (False, "github-error"))
        self.assertIn("timed out", sources["cargo-advisories"]["error"])
        self.assertTrue(sources["osv-ratchet"]["readable"] and sources["dependabot"]["readable"])

    def test_a_source_that_cannot_be_read_keeps_what_it_last_knew(self):
        known = self.measure(self.github(), token="t")
        later = self.measure(self.github(cargo_fails=True, run_ids={"monorepo-guard.yml": 12}), token="t", previous=known)
        # The OSV run is new and its jobs are asked again; the failed Cargo read keeps the last findings.
        self.assertEqual(later["sources"]["cargo-advisories"]["findings"], known["sources"]["cargo-advisories"]["findings"])
        broken = self.measure(lambda path, token=None: (_ for _ in ()).throw(OSError("down")), token="t", previous=known)
        self.assertEqual(broken["sources"]["osv-ratchet"]["findings"], known["sources"]["osv-ratchet"]["findings"])
        self.assertFalse(broken["sources"]["osv-ratchet"]["readable"])

    def test_the_autodeploys_share_of_the_hour_is_left_alone(self):
        previous = self.measure(self.github())
        calls = []
        short = self.measure(self.github(calls=calls), previous=previous,
                             remaining=checks.API_RESERVE + checks.SECURITY_REQUESTS - 1)
        self.assertEqual(calls, [])
        self.assertEqual(short["sources"]["osv-ratchet"], previous["sources"]["osv-ratchet"])

    def test_the_token_is_never_written(self):
        security = self.measure(self.github(), token="secret-value-123")
        self.assertNotIn("secret-value-123", json.dumps(security))


class SecurityGateBeyondTheDailyRun(unittest.TestCase):
    """Root ADR-0388: 2026-10-30 an npm advisory (published 23:09Z) was refused by a pull request at
    23:16Z and by main's push at 23:24Z while the alert read only the scheduled run, which next ran at
    00:48Z and was green because the fix had merged: nothing was announced. Main's verdict is its
    newest run of any event, and a newer pull request or merge-queue refusal counts when its change
    touches no lockfile."""

    REPOSITORY = Security.REPOSITORY
    GREEN_SCHEDULE = {"id": 10, "event": "schedule", "conclusion": "success", "created_at": "2026-10-30T00:48:00Z"}
    ADVISORY = ("GHSA-dddd-eeee-ffff", "crates.io/example-crate")

    @staticmethod
    def change(run_id, created_at, number=None, branch="feature", event="pull_request"):
        return {"id": run_id, "event": event, "conclusion": "failure", "created_at": created_at,
                "head_branch": branch, "pull_requests": [{"number": number}] if number else []}

    def osv(self, previous=None, remaining=60, **fake):
        security = checks.measure_security(self.REPOSITORY, None, previous, Security.NOW,
                                           ask=Security.github(**fake), remaining=lambda: remaining)
        return security["sources"]["osv-ratchet"], security

    def pairs(self, source):
        return [(f["advisory"], f["package"]) for f in source["findings"]]

    def test_a_pull_request_refusal_alerts_while_the_scheduled_run_is_still_green(self):
        # The synthetic failing case: only a pull request's run is red, main's daily run is green.
        source, _ = self.osv(main_runs=[self.GREEN_SCHEDULE],
                             change_runs=[self.change(41, "2026-10-30T23:16:59Z", number=982)])
        self.assertEqual(self.pairs(source), [self.ADVISORY])
        self.assertTrue(source["readable"])

    def test_a_merge_group_refusal_counts_through_its_queued_pull_request(self):
        calls = []
        source, _ = self.osv(main_runs=[self.GREEN_SCHEDULE], calls=calls, change_runs=[
            self.change(42, "2026-10-30T23:20:00Z", branch="gh-readonly-queue/main/pr-971-d4b0230ed3", event="merge_group")])
        self.assertEqual(self.pairs(source), [self.ADVISORY])
        self.assertTrue(any("/pulls/971/files" in path for path, _ in calls))

    def test_a_change_that_touches_a_lockfile_owns_its_refusal(self):
        for files in (["platforms/x/pnpm-lock.yaml"], ["Cargo.lock"], [f"f{n}" for n in range(100)]):
            source, _ = self.osv(main_runs=[self.GREEN_SCHEDULE], pull_files={982: files},
                                 change_runs=[self.change(41, "2026-10-30T23:16:59Z", number=982)])
            self.assertEqual(source["findings"], [], files[:1])
        fork, _ = self.osv(main_runs=[self.GREEN_SCHEDULE], change_runs=[self.change(43, "2026-10-30T23:16:59Z")])
        self.assertEqual(fork["findings"], [], "a pull request the runs API does not name is not main's")

    def test_a_push_to_main_is_main_s_verdict_before_the_next_scheduled_run(self):
        push = {"id": 44, "event": "push", "conclusion": "failure", "created_at": "2026-10-30T23:24:07Z"}
        source, _ = self.osv(main_runs=[push, self.GREEN_SCHEDULE])
        self.assertEqual((self.pairs(source), source["run_id"]), ([self.ADVISORY], 44))

    def test_a_newer_green_run_on_main_supersedes_an_older_refusal(self):
        fixed = {"id": 45, "event": "push", "conclusion": "success", "created_at": "2026-10-31T00:24:43Z"}
        source, _ = self.osv(main_runs=[fixed, self.GREEN_SCHEDULE],
                             change_runs=[self.change(41, "2026-10-30T23:16:59Z", number=982)])
        self.assertEqual(source["findings"], [])

    def test_a_main_run_that_failed_before_the_ratchet_spoke_does_not_resolve_an_advisory(self):
        red = {"id": 44, "event": "push", "conclusion": "failure", "created_at": "2026-10-30T23:24:07Z"}
        broken = {"id": 46, "event": "push", "conclusion": "failure", "created_at": "2026-10-31T00:10:00Z"}
        source, _ = self.osv(main_runs=[broken, red, self.GREEN_SCHEDULE], silent={46})
        self.assertEqual((self.pairs(source), source["run_id"]), ([self.ADVISORY], 44))

    def test_runs_past_the_hour_s_budget_wait_for_the_next_hour(self):
        changes = [self.change(41, "2026-10-30T23:16:59Z", number=982)]
        # Budget left for the lists but not for a new run: main's green verdict stands, the refusal waits.
        first = checks.ratchet(self.REPOSITORY, "monorepo-guard.yml", None, None,
                               Security.github(main_runs=[self.GREEN_SCHEDULE], change_runs=changes),
                               lambda: checks.API_RESERVE + checks.RUN_REQUESTS - 1)
        self.assertEqual((first["findings"], first["verdicts"]), ([], [{"run_id": 10, "findings": []}]))
        calls = []
        later, _ = self.osv(previous={"sources": {"osv-ratchet": first}}, main_runs=[self.GREEN_SCHEDULE],
                            change_runs=changes, calls=calls)
        self.assertEqual(self.pairs(later), [self.ADVISORY])
        again, _ = self.osv(previous={"sources": {"osv-ratchet": later}}, main_runs=[self.GREEN_SCHEDULE],
                            change_runs=changes, calls=(calls := []))
        self.assertEqual(again["findings"], later["findings"])
        self.assertFalse(any("/jobs" in path or "/pulls/" in path for path, _ in calls), "a run already read")

    def test_the_cargo_ratchet_runs_on_no_change_and_is_not_asked_for_one(self):
        calls = []
        self.osv(calls=calls)
        self.assertFalse(any("dependency-advisories.yml/runs?status=failure" in path for path, _ in calls))


class File(unittest.TestCase):
    def test_the_written_file_has_the_shape_foundation_api_reads(self):
        example = json.loads(EXAMPLE.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "service-checks.json"
            edge = checks.measure_edge(["h"], ask=lambda _host: (12.0, "ICN"))
            with tempfile.TemporaryDirectory() as store:
                ci = CiTrend("test_days_are_fetched_newest_first_and_a_week_is_reported_only_when_whole").measure(pathlib.Path(store), 60)[0]
            security = Security.measure(Security.github(cargo_fails=True), token="t")
            checks.write_atomically(out, {"schema_version": 1, "checked_at": edge["checked_at"], "edge": edge, "ci": ci,
                                          "security": security})
            written = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(shape(written), shape(example))
            self.assertEqual(list(pathlib.Path(tmp).iterdir()), [out], "no temporary file is left")


if __name__ == "__main__":
    unittest.main()
