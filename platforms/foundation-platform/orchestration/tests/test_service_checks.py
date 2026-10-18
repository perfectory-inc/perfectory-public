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
    def github(cargo_fails=False, run_ids=None, calls=None):
        """A fake GitHub: two alerts on one advisory and package, a failed OSV run that annotated one
        advisory (and a note that is not one), and a Cargo run that cannot be read."""
        run_ids = run_ids or {"monorepo-guard.yml": 11, "dependency-advisories.yml": 22}
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
                if f"/workflows/{workflow}/runs" in path:
                    return {"workflow_runs": [{"id": run_id}]}, {}
            if "/jobs" in path:
                return {"jobs": [{"id": 1, "conclusion": "success"}, {"id": 2, "conclusion": "failure"}]}, {}
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
