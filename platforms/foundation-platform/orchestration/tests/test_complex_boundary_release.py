"""The scheduled industrial-complex boundary release (scripts/ops/complex-boundary-release.sh, root ADR-0240).

A fake docker stands in for the R2 listing and download and for `docker compose ... spark-submit`; a fake
publisher writes the handoff. The Spark stand-in answers the way the Silver writer does: the table's ingest
record either already holds the object (`already-ingested`) or the rows are written (`write-ok`). The release
layout is the host's (root ADR-0134). Object keys and times are synthetic.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
RELEASE_ID = "b" * 40
PREFIX = "bronze/source=vworldkr__sandan_boundary/"

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
log = open(os.environ["FAKE_LOG"], "a")
if args[:1] == ["rm"]:
    sys.exit(0)
if args[:1] == ["run"]:
    host = pathlib.Path(args[args.index("-v") + 1].rsplit(":/w", 1)[0])
    env = [args[i + 1] for i, a in enumerate(args) if a == "-e"]
    if "list-objects-v2" in args:
        log.write(json.dumps({"tool": "list", "env": env}) + "\n")
        print(os.environ["FAKE_LISTING"])
    else:
        source, target = [a for a in args if a.startswith("s3://") or a.startswith("/w/")][:2]
        log.write(json.dumps({"tool": "download", "key": source.split("/", 3)[3], "env": env}) + "\n")
        out = pathlib.Path(target.replace("/w", str(host), 1))
        out.write_bytes(b"zip")
    sys.exit(0)
assert args[0] == "compose" and "spark-submit" in args, args
submit = args[args.index("spark-submit"):]
script = next(a for a in submit if a.endswith(".py"))
log.write(json.dumps({"tool": "spark", "job": pathlib.Path(script).name, "args": submit[submit.index(script) + 1:]}) + "\n")
if os.environ["FAKE_HELD"] == "true":
    print("silver-industrial-complex-boundaries-iceberg-already-ingested rows=3 token=t snapshot=1 objects=1")
elif os.environ["FAKE_HELD"] == "false":
    print("silver-industrial-complex-boundaries-iceberg-write-ok rows=3 table=silver.industrial_complex_boundaries")
'''

FAKE_PUBLISHER = '''#!/bin/sh
P=FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF
eval root=\\$${P}_BRONZE_ROOT object=\\$${P}_SOURCE_OBJECT slug=\\$${P}_SOURCE_SLUG out=\\$${P}_OUTPUT_PATH
test -f "$root/bronze/source=$slug/$object" || exit 3
printf '{"tool": "export", "command": "%s", "object": "%s"}\\n' "$1" "$object" >> "$FAKE_LOG"
echo '{}' > "$out"
'''

FAKE_FOLD = '#!/bin/sh\nprintf \'{"tool": "fold", "unit": "%s", "force": "%s"}\\n\' "$1" "$FOUNDATION_MAP_EDIT_FOLD_FORCE" >> "$FAKE_LOG"\n'


def listing(*objects):
    return json.dumps({"Contents": [{"Key": PREFIX + key, "LastModified": at} for key, at in objects]})


class ComplexBoundaryRelease(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="complex-boundary-release-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("complex-boundary-release.sh", "admitted-writer-runtime.sh", "job-journal.sh", "job-state-root.sh"):
            shutil.copy(PLATFORM / "scripts/ops" / name, release / "scripts/ops" / name)
        (release / "scripts/ops/map-edit-fold.sh").write_text(FAKE_FOLD)
        for path in (release / "scripts/ops").iterdir():
            path.chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        publisher = artifacts / "foundation-outbox-publisher"
        publisher.write_text(FAKE_PUBLISHER)
        publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(publisher.read_bytes()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.log = self.root / "calls.jsonl"
        self.lakehouse = self.root / "lakehouse"
        self.script = base / "current/scripts/ops/complex-boundary-release.sh"
        self.env = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log), "FAKE_HELD": "false",
            "FAKE_LISTING": listing(("30137-1.zip", "2099-01-01T00:00:00Z"), ("30137-2.xlsx", "2099-03-01T00:00:00Z"),
                                    ("30137-1--sha256-ab.zip", "2099-02-01T00:00:00Z")),
            "FOUNDATION_COMPLEX_BOUNDARY_RELEASE_STATE_ROOT": str(self.root / "state"),
            "FOUNDATION_COMPLEX_BOUNDARY_RELEASE_LAKEHOUSE_STATE_ROOT": str(self.lakehouse),
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET": "bucket", "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID": "reader-id",
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY": "reader-secret",
            "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID": "writer-id",
            "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY": "writer-secret",
        }

    def run_job(self, **env):
        result = subprocess.run(["bash", str(self.script)], env={**self.env, **env}, capture_output=True, text=True,
                                timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def tools(self, calls):
        return [c["tool"] for c in calls]

    def test_a_new_file_is_loaded_and_folded(self):
        result, calls = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.tools(calls), ["list", "download", "export", "spark", "fold"])
        # The ZIP uploaded last, not the newest object of any kind, nor the one sorting last by name.
        (download,) = [c for c in calls if c["tool"] == "download"]
        self.assertEqual(download["key"], PREFIX + "30137-1--sha256-ab.zip")
        self.assertIn("AWS_ACCESS_KEY_ID=reader-id", download["env"])
        self.assertFalse(any("writer" in value for value in download["env"]))
        self.assertEqual([c for c in calls if c["tool"] == "export"][0]["command"],
                         "export-industrial-complex-boundary-silver-handoff")
        self.assertEqual([c for c in calls if c["tool"] == "fold"], [{"tool": "fold", "unit": "complex", "force": "1"}])
        self.assertEqual(result.stdout.strip().splitlines()[-2:], [
            f"complex-boundary-release-outcome outcome=changed object={PREFIX}30137-1--sha256-ab.zip",
            "foundation-job-outcome changed"])
        # The downloaded ZIP and the handoff are not kept.
        (run,) = (self.lakehouse / "complex-boundary-release").iterdir()
        self.assertFalse((run / "bronze").exists())
        self.assertFalse((run / "handoff.jsonl").exists())

    def test_a_file_silver_holds_is_unchanged_and_bakes_nothing(self):
        result, calls = self.run_job(FAKE_HELD="true")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertNotIn("fold", self.tools(calls))
        self.assertIn("outcome=unchanged reason=already_loaded", result.stdout)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome unchanged")

    def test_no_boundary_zip_is_nothing_to_do(self):
        result, calls = self.run_job(FAKE_LISTING=listing(("30137-2.xlsx", "2099-03-01T00:00:00Z")))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.tools(calls), ["list"])
        self.assertIn("outcome=unchanged reason=no_object", result.stdout)

    def test_a_load_that_says_neither_fails_the_run(self):
        result, calls = self.run_job(FAKE_HELD="silent")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("fold", self.tools(calls))
        self.assertNotIn("foundation-job-outcome", result.stdout)

    def test_a_key_outside_the_bronze_layout_is_refused(self):
        result, calls = self.run_job(FAKE_LISTING=listing(("../../etc/x.zip", "2099-03-01T00:00:00Z")))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.tools(calls), ["list"])


if __name__ == "__main__":
    unittest.main()
