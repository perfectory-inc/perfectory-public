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


class File(unittest.TestCase):
    def test_the_written_file_has_the_shape_foundation_api_reads(self):
        example = json.loads(EXAMPLE.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "service-checks.json"
            edge = checks.measure_edge(["h"], ask=lambda _host: (12.0, "ICN"))
            with tempfile.TemporaryDirectory() as store:
                ci = CiTrend("test_days_are_fetched_newest_first_and_a_week_is_reported_only_when_whole").measure(pathlib.Path(store), 60)[0]
            checks.write_atomically(out, {"schema_version": 1, "checked_at": edge["checked_at"], "edge": edge, "ci": ci})
            written = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(shape(written), shape(example))
            self.assertEqual(list(pathlib.Path(tmp).iterdir()), [out], "no temporary file is left")


if __name__ == "__main__":
    unittest.main()
