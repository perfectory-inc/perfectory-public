"""The scheduled 읍면동 boundary release (scripts/ops/admin-boundary-release.sh, root ADR-0236) on a fake docker.

The fake stands in for the R2 download, GDAL and `docker compose ... spark-submit`: the download copies
synthetic ZIPs into the work folder, GDAL writes a GeoJSON named after the ZIP's layer, and the Spark jobs
answer the way the real ones do (the probe says held or not, merge refuses a round missing a sido). The
real rounds.py picks the round. The release layout is the host's (root ADR-0134). Sido codes, rounds and
keys are synthetic.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest
import zipfile

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
RELEASE_ID = "a" * 40

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, pathlib, shutil, sys, zipfile
args = sys.argv[1:]
log = open(os.environ["FAKE_LOG"], "a")
if args[:1] == ["rm"]:
    sys.exit(0)
if args[:1] == ["run"]:
    host = pathlib.Path(args[args.index("-v") + 1].rsplit(":/w", 1)[0])
    if any("amazon/aws-cli" in a for a in args):
        env = [args[i + 1] for i, a in enumerate(args) if a == "-e"]
        log.write(json.dumps({"tool": "download", "env": env}) + "\n")
        for z in pathlib.Path(os.environ["FAKE_ZIPS"]).glob("*.zip"):
            shutil.copy(z, host / "zips" / z.name)
        sys.exit(0)
    out = pathlib.Path(args[args.index("GeoJSON") + 1].replace("/w", str(host), 1))
    source = args[args.index("GeoJSON") + 2].replace("/vsizip//w", str(host), 1)
    layer = next(n for n in zipfile.ZipFile(source).namelist() if n.endswith(".shp"))[:-4]
    log.write(json.dumps({"tool": "gdal", "zip": pathlib.Path(source).name}) + "\n")
    out.write_text(json.dumps({"name": layer, "features": []}))
    sys.exit(0)
assert args[0] == "compose" and "spark-submit" in args, args
state = pathlib.Path(os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT"])
def host(path):
    return state / pathlib.Path(path).relative_to("/workspace/target/lakehouse")
submit = args[args.index("spark-submit"):]
script = next(a for a in submit if a.startswith("/workspace/") and a.endswith(".py"))
job, job_args = pathlib.Path(script).name, submit[submit.index(script) + 1:]
flag = lambda name: job_args[job_args.index(name) + 1]
log.write(json.dumps({"tool": "spark", "job": job, "args": job_args}) + "\n")
if job == "administrative_boundaries_handoff_to_silver.py" and "--probe-only" in job_args:
    print(f"administrative-boundaries-silver-probe snapshot={flag('--source-snapshot-id')} held={os.environ['FAKE_HELD']}")
elif job == "merge.py":
    if os.environ.get("FAKE_MISSING_SIDO"):
        sys.exit(f"round {flag('--round')} has no file for sido ['{os.environ['FAKE_MISSING_SIDO']}']; refusing a partial country")
    host(flag("--out-dir")).parent.joinpath("official-administrative-boundary.geojson").write_text("{}")
elif job == "administrative_boundaries_handoff_to_silver.py":
    host(flag("--summary-output")).write_text(json.dumps({"row_count": 3}))
elif job == "place_id_release_to_gold.py":
    host(flag("--summary-output")).write_text("{}")
else:
    sys.exit("unexpected job " + job)
'''

FAKE_FOLD = '#!/bin/sh\nprintf \'{"tool": "fold", "unit": "%s", "force": "%s"}\\n\' "$1" "$FOUNDATION_MAP_EDIT_FOLD_FORCE" >> "$FAKE_LOG"\n'


class AdminBoundaryRelease(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="admin-boundary-release-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        for part in ("scripts/tiles/admin-boundary", "infra/lakehouse/spark/jobs", "infra/lakehouse/contracts"):
            shutil.copytree(PLATFORM / part, release / part, ignore=shutil.ignore_patterns("__pycache__"))
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("admin-boundary-release.sh", "admitted-writer-runtime.sh", "job-journal.sh"):
            shutil.copy(PLATFORM / "scripts/ops" / name, release / "scripts/ops" / name)
        (release / "scripts/ops/map-edit-fold.sh").write_text(FAKE_FOLD)
        for path in (release / "scripts").rglob("*.sh"):
            path.chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text("#!/bin/sh\n")
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.zips = self.root / "bronze"
        self.zips.mkdir()
        self.lakehouse = self.root / "lakehouse"
        self.log = self.root / "calls.jsonl"
        self.script = base / "current/scripts/ops/admin-boundary-release.sh"
        self.env = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log), "FAKE_ZIPS": str(self.zips),
            "FAKE_HELD": "false",
            "FOUNDATION_ADMIN_BOUNDARY_RELEASE_STATE_ROOT": str(self.root / "state"),
            "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": str(self.lakehouse),
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET": "bucket", "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID": "reader-id",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY": "reader-secret",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID": "writer-id",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY": "writer-secret",
        }

    def zip(self, name, layer):
        with zipfile.ZipFile(self.zips / name, "w") as archive:
            archive.writestr(f"{layer}.shp", b"")
            archive.writestr(f"{layer}.dbf", b"")

    def two_rounds(self):
        self.zip("30603-1.zip", "LSMD_ADM_SECT_UMD_98_209901")
        self.zip("30603-2.zip", "LSMD_ADM_SECT_UMD_99_209901")
        self.zip("30603-3--sha256-ab.zip", "LSMD_ADM_SECT_UMD_98_209902")
        self.zip("30603-4--sha256-cd.zip", "LSMD_ADM_SECT_UMD_99_209902")

    def run_job(self, **env):
        result = subprocess.run(["bash", str(self.script)], env={**self.env, **env}, capture_output=True, text=True,
                                timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def jobs(self, calls):
        return [c.get("job", c["tool"]) for c in calls if c["tool"] in ("spark", "fold")]

    def runs(self):
        return list((self.lakehouse / "admin-boundary-release").iterdir())

    def test_a_new_round_is_converted_loaded_published_and_folded(self):
        self.two_rounds()
        result, calls = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.jobs(calls), ["administrative_boundaries_handoff_to_silver.py", "merge.py",
                                            "administrative_boundaries_handoff_to_silver.py",
                                            "place_id_release_to_gold.py", "fold"])
        # Only the newest round's ZIPs are converted, and they are the lineage.
        self.assertEqual(sorted(c["zip"] for c in calls if c["tool"] == "gdal"),
                         ["30603-3--sha256-ab.zip", "30603-4--sha256-cd.zip"])
        load = [c for c in calls if c.get("job") == "administrative_boundaries_handoff_to_silver.py"][1]["args"]
        self.assertEqual(load[load.index("--source-snapshot-id") + 1], "vworldkr__boundary_emd-30603-209902")
        self.assertEqual(load[load.index("--source-record-id") + 1],
                         "bronze/source=vworldkr__boundary_emd/30603-3--sha256-ab.zip,"
                         "bronze/source=vworldkr__boundary_emd/30603-4--sha256-cd.zip")
        self.assertIn("--predecessor-parcel-snapshot-id", load)
        self.assertEqual([c for c in calls if c["tool"] == "fold"], [{"tool": "fold", "unit": "admin", "force": "1"}])
        lines = result.stdout.strip().splitlines()
        self.assertEqual(lines[-1], "foundation-job-outcome changed")
        self.assertIn("round=209902 snapshot=vworldkr__boundary_emd-30603-209902 rows=3", lines[-2])
        # The download uses the read-only pair, and the downloaded ZIPs are not kept.
        (download,) = [c for c in calls if c["tool"] == "download"]
        self.assertIn("AWS_ACCESS_KEY_ID=reader-id", download["env"])
        self.assertFalse(any("writer" in value for value in download["env"]))
        (run,) = self.runs()
        self.assertFalse((run / "zips").exists())
        self.assertTrue((run / "silver-summary.json").exists())

    def test_a_round_silver_holds_is_unchanged_and_converts_nothing(self):
        self.two_rounds()
        result, calls = self.run_job(FAKE_HELD="true")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.jobs(calls), ["administrative_boundaries_handoff_to_silver.py"])
        self.assertFalse([c for c in calls if c["tool"] == "gdal"])
        self.assertEqual(result.stdout.strip().splitlines()[-2:], [
            "admin-boundary-release-outcome outcome=unchanged reason=already_loaded round=209902",
            "foundation-job-outcome unchanged"])
        (run,) = self.runs()
        self.assertFalse((run / "zips").exists())

    def test_a_round_missing_a_sido_waits(self):
        self.two_rounds()
        result, calls = self.run_job(FAKE_MISSING_SIDO="97")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.jobs(calls), ["administrative_boundaries_handoff_to_silver.py", "merge.py"])
        self.assertIn("outcome=unchanged reason=incomplete_round round=209902", result.stdout)

    def test_no_boundary_layer_is_no_round(self):
        with zipfile.ZipFile(self.zips / "30603-1.zip", "w") as archive:
            archive.writestr("readme.txt", b"")
        result, calls = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.jobs(calls), [])
        self.assertIn("outcome=unchanged reason=no_round", result.stdout)

    def test_a_probe_that_does_not_answer_fails_the_run(self):
        # Neither held nor not held: the run stops rather than loading a round it could not check.
        self.two_rounds()
        result, calls = self.run_job(FAKE_HELD="maybe")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.jobs(calls), ["administrative_boundaries_handoff_to_silver.py"])
        self.assertNotIn("foundation-job-outcome", result.stdout)


if __name__ == "__main__":
    unittest.main()
