"""The hourly serving monitor (scripts/ops/by-pnu-serving-monitor.sh) against a fake publisher.

2026-11-06..07 (incident W21) every Worker deploy rolled back on "the monitor wrote no report":
#1101 made the script point the publisher at its own temporary report, so the report path the
Worker deploy and the rollback rehearsal pass was replaced and the file they read never appeared.
The release layout is the host's (root ADR-0134), as in test_by_pnu_serving_bake.py.
"""

import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
OPS = PLATFORM / "scripts/ops"
RELEASE_ID = "f" * 40
PREFIX = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING"

# Writes a stale-but-passing report, then exits 1 the way the publisher does past the freshness budget.
FAKE_PUBLISHER = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MONITOR_REPORT_PATH"], "w") as out:
    json.dump({"passed": True, "stale": True, "served_published_at_utc": "2099-10-30T06:49:36Z",
               "served_age_hours": 200.0, "freshness_budget_hours": 48.0}, out)
sys.exit(1)
'''


@unittest.skipIf(os.name == "nt", "the release layout needs a POSIX shell")
class ServingMonitorReportTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="by-pnu-monitor-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("by-pnu-serving-monitor.sh", "admitted-writer-runtime.sh", "by-pnu-lane-index.sh",
                     "serving_freshness_metrics.py"):
            (release / "scripts/ops" / name).write_bytes((OPS / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (release / "config").mkdir()
        (release / "config/by-pnu-lane-index.v1.json").write_bytes(
            (PLATFORM / "config/by-pnu-lane-index.v1.json").read_bytes())
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text(FAKE_PUBLISHER)
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(FAKE_PUBLISHER.encode()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        self.script = base / "current/scripts/ops/by-pnu-serving-monitor.sh"
        self.metrics = self.root / "metrics"
        self.metrics.mkdir()

    def monitor(self, **env) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(self.script), "parcel"], capture_output=True, text=True,
                              env={"PATH": os.environ["PATH"],
                                   "FOUNDATION_BY_PNU_SERVING_MONITOR_METRICS_DIR": str(self.metrics), **env})

    def test_the_callers_report_path_holds_the_report_after_the_monitor_exits(self):
        report = self.root / "caller/report.json"
        report.parent.mkdir()
        result = self.monitor(**{f"{PREFIX}_MONITOR_REPORT_PATH": str(report)})
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(json.loads(report.read_text(encoding="utf-8"))["passed"])
        self.assertTrue((self.metrics / "by-pnu-serving-parcel.prom").exists())

    def test_without_a_callers_path_the_report_is_temporary_and_still_feeds_the_metrics(self):
        result = self.monitor()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue((self.metrics / "by-pnu-serving-parcel.prom").exists())


if __name__ == "__main__":
    unittest.main()
