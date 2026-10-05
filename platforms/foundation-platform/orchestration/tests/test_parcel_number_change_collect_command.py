"""The 필지고유번호변동연혁 collection job's real command line (scripts/ops/parcel-number-change-collect.sh,
root ADR-0144 §4).

The script runs from an installed release layout (root ADR-0134, as in
test_legal_dong_code_collect_command.py). The publisher and docker are stand-ins: the publisher writes
the plan, an inventory listing two synthetic 시도 files and the table definition, and the ingest
evidence; docker copies a synthetic zip where the read-back would land it. What runs for real is the
script, its flags and the Python it calls: an unchanged listing hands off nothing, a changed one is
collected, checked and handed off once, and a file that fails its checks hands off nothing and
leaves the state where it was.
"""

import hashlib
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

PLATFORM = job_specs.PLATFORM_ROOT
RELEASE_ID = "e" * 40
HEADER = ("OLD_ADM_SECT_CD|OLD_LAND_LOC_CD|OLD_LEDG_GBN|OLD_BOBN|OLD_BUBN|ADM_SECT_CD|LAND_LOC_CD|LEDG_GBN|BOBN|BUBN|"
          "LAND_MOV_RSN_CD|LAND_MOV_YMD|COL_ADM_SECT_CD")

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
command = sys.argv[1]
with open(os.path.join(state, "calls.log"), "a", encoding="utf-8") as log:
    log.write(command + "\n")
if command == "plan-vworld-dataset-collection":
    json.dump({"jobs": []}, open(os.environ["FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH"], "w"))
elif command == "inventory-vworld-dataset-files":
    listing = json.load(open(os.path.join(state, "listing.json"), encoding="utf-8"))
    json.dump(listing, open(os.environ["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH"], "w", encoding="utf-8"))
elif command == "ingest-vworld-dataset-files":
    assert os.environ.get("FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH") == "1", "a reused file number must be fetched again"
    inventory = json.load(open(os.environ["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH"], encoding="utf-8"))
    files = [{"download_ds_id": f["download_ds_id"], "file_no": f["file_no"], "status": "succeeded",
              "provider_file_name": f["provider_file_name"],
              "object_key": "bronze/source=vworldkr__parcel_number_change_history/run/" + f["file_no"] + ".zip"}
             for job in inventory["jobs"] for f in job["files"]]
    json.dump({"files": files}, open(os.environ["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH"], "w"))
else:
    sys.exit("unexpected command " + command)
"""

# docker run ... -v <host>:/w <image> s3 cp ... s3://bucket/<key> /w/<name>: copy the fixture for <key>.
FAKE_DOCKER = r"""#!/usr/bin/env python3
import os, shutil, sys
args = sys.argv[1:]
host = next(a.split(":")[0] for a in args if a.endswith(":/w"))
source, target = args[-2], args[-1]
name = source.rsplit("/", 1)[-1]
shutil.copy(os.path.join(os.environ["FAKE_STATE"], "objects", name), os.path.join(host, target.split("/w/", 1)[1]))
"""


def zipped(rows):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("ABPD_UNQ_NO_CHG_HIST_99_209909.txt", "\n".join([HEADER, *rows]) + "\n")
    return buffer.getvalue()


def row(n, ymd="20250701"):
    return f"99999|10100|1|{n:04d}|0000|99998|10100|1|{n:04d}|0000|52|{ymd}|99999"


def item(file_no, name, updated_at):
    return {"svc_cde": "MK", "ds_id": "30527", "download_ds_id": "30527", "file_no": file_no,
            "provider_file_name": name, "updated_at": updated_at, "base_ym": updated_at[:7], "size_kib": "3"}


class CollectCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pnch-collect-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("parcel-number-change-collect.sh", "admitted-writer-runtime.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        for relative in ("infra/lakehouse/spark/jobs", "infra/lakehouse/contracts"):
            shutil.copytree(PLATFORM / relative, release / relative, ignore=shutil.ignore_patterns("__pycache__"))
        (release / "docs/catalog").mkdir(parents=True)
        shutil.copy(PLATFORM / "docs/catalog/public-source-endpoint-catalog.v1.json", release / "docs/catalog")
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
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.script = base / "current/scripts/ops/parcel-number-change-collect.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        (self.fake / "objects").mkdir(parents=True)
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_PARCEL_NUMBER_CHANGE_STATE_ROOT": str(self.state),
            "FAKE_STATE": str(self.fake), "DATABASE_URL": "postgres://unused", "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET": "b",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT": "https://r2.invalid",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID": "r", "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY": "s",
        }

    def listing(self, *files):
        (self.fake / "listing.json").write_text(json.dumps(
            {"status": "ready", "jobs": [{"endpoint_slug": "vworld-dataset-parcel_number_change_history", "files": list(files)}]}),
            encoding="utf-8")

    def run_job(self):
        return subprocess.run(["bash", str(self.script)], env=self.env, capture_output=True, text=True, timeout=120)

    def handoffs(self):
        pending = self.state / "pending"
        return sorted(pending.iterdir()) if pending.exists() else []

    def test_a_changed_file_is_collected_once_and_an_unchanged_listing_hands_off_nothing(self):
        (self.fake / "objects" / "5.zip").write_bytes(zipped([row(n) for n in range(1, 40)]))
        (self.fake / "objects" / "7.zip").write_bytes(zipped([row(n) for n in range(1, 10)]))
        self.listing(item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip", "2099-09-15"), item("7", "ABPD_UNQ_NO_CHG_HIST_합성둘.zip", "2099-06-14"),
                     item("1", "ABPD_UNQ_NO_CHG_HIST.xlsx", "2099-01-01"))
        first = self.run_job()
        self.assertEqual(first.returncode, 0, first.stderr + first.stdout)
        [handoff] = self.handoffs()
        objects = json.loads((handoff / "handoff.json").read_text(encoding="utf-8"))["objects"]
        accepted = json.loads((self.state / "accepted.json").read_text(encoding="utf-8"))["files"]
        self.assertTrue(accepted["30527-1"]["reference"], "the table definition is collected, not loaded")
        self.assertEqual([(o["file_key"], o["rows"]) for o in objects], [("30527-5", 39), ("30527-7", 9)])
        second = self.run_job()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn('"unchanged"', second.stdout)
        self.assertEqual(len(self.handoffs()), 1, "an unchanged listing hands off nothing")
        calls = (self.fake / "calls.log").read_text(encoding="utf-8").split()
        self.assertEqual(calls.count("ingest-vworld-dataset-files"), 1)

    def test_a_file_that_fails_its_check_hands_off_nothing_and_keeps_the_state(self):
        (self.fake / "objects" / "5.zip").write_bytes(zipped([row(n, ymd="") for n in range(1, 40)]))
        self.listing(item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip", "2099-09-15"))
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.handoffs(), [])
        self.assertFalse((self.state / "accepted.json").exists())
        self.assertIn("FAILED", (self.state / "journal.log").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
