"""The daily source sweep's real command line (scripts/ops/daily-source-sweep.sh, root ADR-0077,
ADR-0168 and ADR-0172).

The script runs from an installed release layout (root ADR-0134, as in
test_parcel_number_change_collect_command.py). The publisher and curl are stand-ins: the publisher
writes each lane's plan, inventory and evidence the way a scenario file says, and curl records the
Slack payloads. What runs for real is the script, its flags, the Python it calls and the endpoint
catalog: which datasets the VWorld lane sweeps comes from the catalog alone, the lane has no byte
budget (a large day is said once, never refused), a lane that fails does not stop the other, and a
missing setting stops the job before anything happens.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

PLATFORM = job_specs.PLATFORM_ROOT
sys.path.insert(0, str(PLATFORM / "scripts/deploy"))
import runtime_secrets  # noqa: E402

UNIT = "foundation-source-sweep.service"
SCRIPT = PLATFORM / "scripts/ops/daily-source-sweep.sh"
CATALOG = PLATFORM / "docs/catalog/public-source-endpoint-catalog.v1.json"
NAMING = PLATFORM / "config/environment-variable-naming.contract.json"
NEEDS = sorted(runtime_secrets.load().consumer(UNIT).needs)
REQUIRED = sorted(runtime_secrets.script_requirements(SCRIPT))
VWORLD_LOGIN = json.loads(NAMING.read_text(encoding="utf-8"))[
    "compatibility_migrations"]["foundation-vworld-credentials"]["credentials"]
RELEASE_ID = "f" * 40

FAKE_PUBLISHER = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
command = sys.argv[1]
scenario = json.load(open(os.path.join(state, "scenario.json"), encoding="utf-8"))
with open(os.path.join(state, "calls.log"), "a", encoding="utf-8") as log:
    log.write(command + "\n")
env = os.environ
if command == "plan-building-hub-bulk-collection":
    json.dump({"jobs": []}, open(env["FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH"], "w"))
elif command == "ingest-building-hub-bulk-collection":
    hub = scenario["hub"]
    if hub.get("die"):
        sys.exit("hub ingest died")
    json.dump(hub["evidence"], open(env["FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_EVIDENCE_PATH"], "w"))
elif command == "plan-vworld-dataset-collection":
    assert env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_DAILY_COLLECTION") == "source_sweep"
    assert "FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH" not in env, "the sweep plans without a summary"
    if scenario["vworld"].get("plan_dies"):
        sys.exit("plan died")
    # What the real planner does with the catalog: the marked endpoints and the notice threshold.
    catalog = json.load(open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_ENDPOINT_CATALOG_PATH"], encoding="utf-8"))
    jobs = [e["endpoint_slug"] for e in catalog["endpoints"] if e.get("daily_collection") == "source_sweep"]
    json.dump({"status": "ready", "jobs": jobs,
               "landed_bytes_notice": catalog["daily_collections"]["source_sweep"]["landed_bytes_notice"]},
              open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH"], "w"))
elif command == "inventory-vworld-dataset-files":
    plan = json.load(open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH"], encoding="utf-8"))
    json.dump({"status": "ready", "selection_archive_file_count": 3,
               "jobs": [{"endpoint_slug": slug, "files": [
                   {"download_ds_id": "9991", "file_no": n, "size_kib": 2**20} for n in ("7", "8")]}
                        for slug in plan["jobs"][:1]]}, open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH"], "w"))
elif command == "ingest-vworld-dataset-files":
    for name, value in [("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY", "content_addressed"),
                        ("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES", "1"),
                        ("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE", "1")]:
        assert env.get(name) == value, (name, env.get(name))
    assert "FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH" not in env, "a daily run must not refetch what it holds"
    assert env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR"] == env["FOUNDATION_SOURCE_SWEEP_SPOOL_DIR"], \
        "content-addressed files are spooled where the unit may write"
    assert os.path.isdir(env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR"])
    # Root ADR-0172: the lane has no byte budget; nothing may hand the ingest one.
    assert not [name for name in env if "NEW_BYTES_BUDGET" in name and "RAON" not in name], \
        "the VWorld lane is given no byte budget"
    vworld = scenario["vworld"]
    json.dump(vworld["evidence"], open(env["FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH"], "w"))
    sys.exit(vworld.get("rc", 0))
elif command == "measure-bronze-object-members":
    # What the measuring step was handed (names and whether set, never values).
    seen = {name: name in env for name in (
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY", "DATABASE_URL")}
    seen["sources"] = env.get("FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES")
    json.dump(seen, open(os.path.join(state, "members-env.json"), "w"))
    members = scenario["members"]
    sys.stderr.write(members.get("stderr", ""))
    if members.get("summary") is not None:
        print("bronze-object-members-json " + json.dumps(members["summary"]))
    sys.exit(members.get("rc", 0))
else:
    sys.exit("unexpected command " + command)
"""

# The large-file lane's script (root ADR-0170) is a stand-in here: test_raon_large_files_command.py
# runs the real one. It records how the sweep called it and answers as the scenario says.
FAKE_RAON = r"""#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
with open(os.path.join(state, "raon.log"), "a", encoding="utf-8") as log:
    log.write(" ".join(sys.argv[1:]) + "\n")
with open(os.path.join(state, "calls.log"), "a", encoding="utf-8") as log:
    log.write("raon-large-files.sh\n")
raon = json.load(open(os.path.join(state, "scenario.json"), encoding="utf-8")).get("raon") or {}
if raon.get("summary") is not None:
    json.dump(raon["summary"], open(os.environ["FOUNDATION_RAON_LARGE_FILES_SUMMARY_PATH"], "w"))
sys.exit(raon.get("rc", 0))
"""

# curl ... -d <payload> https://slack.com/...: record the payload.
FAKE_CURL = r"""#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
with open(os.path.join(os.environ["FAKE_STATE"], "slack.log"), "a", encoding="utf-8") as log:
    log.write(args[args.index("-d") + 1] + "\n")
"""

HUB_QUIET = {"evidence": {"selected_job_count": 3, "succeeded_job_count": 0, "skipped_job_count": 3,
                          "failed_job_count": 0, "status": "ready", "jobs": []}}


def members_summary(measured=0, failed=0, members=0):
    # The fields of bronze_object_members::Summary the sweep reads.
    return {"schema_version": "x", "dry_run": False, "selected": measured + failed, "measured": measured,
            "members": members, "failed": failed, "skipped": 0}


MEMBERS_QUIET = {"summary": members_summary()}


def vworld_evidence(files, status="ready", archives=()):
    count = lambda s: sum(1 for f in files if f["status"] == s)
    return {"status": status, "selected_file_count": len(files), "succeeded_file_count": count("succeeded"),
            "skipped_file_count": count("skipped_existing"), "failed_file_count": count("failed"),
            "files": files, "selection_archives": list(archives),
            "deferred_selection_archive_file_count": sum(
                1 for f in archives if f["status"] == "deferred_selection_archive")}


def raon_summary(status="ready", committed=1, failed=0, budget=2**34, files=None):
    return {"run_id": "20991231T000000Z", "status": status, "planned": committed + failed, "committed": committed,
            "failed": failed, "listed_bytes": 3 * 2**30, "budget": budget, "budget_override": False,
            "files": files if files is not None else
            [{"source_slug": "vworldkr__synthetic", "provider_file_id": "9991-70", "status": "committed"}]}


def vfile(file_no, status):
    return {"source_slug": "vworldkr__synthetic", "download_ds_id": "9991", "file_no": file_no, "status": status}


class SweepCommand(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="source-sweep-")
        self.addCleanup(self.temp.cleanup)
        root = pathlib.Path(self.temp.name)
        base = root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        (release / "scripts/ops").mkdir(parents=True)
        for name in ("daily-source-sweep.sh", "admitted-writer-runtime.sh", "vworld-login.sh", "job-journal.sh",
                     "bronze-object-members.sh", "database-url.sh", "vworld-sweep-lane.sh"):
            (release / "scripts/ops" / name).write_bytes((PLATFORM / "scripts/ops" / name).read_bytes())
            (release / "scripts/ops" / name).chmod(0o755)
        (release / "scripts/ops/raon-large-files.sh").write_text(FAKE_RAON)
        (release / "scripts/ops/raon-large-files.sh").chmod(0o755)
        (release / "docs/catalog").mkdir(parents=True)
        shutil.copy(CATALOG, release / "docs/catalog")
        self.release_catalog = release / "docs/catalog" / CATALOG.name
        # The large-file lane runs only under a positive cap (root ADR-0170); the tests that need it
        # switched off say so with raon_cap(0), whatever the committed catalog holds today.
        self.raon_cap(2**34)
        (release / "config").mkdir()
        shutil.copy(NAMING, release / "config")
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
        (bin_dir / "curl").write_text(FAKE_CURL)
        (bin_dir / "curl").chmod(0o755)
        self.script = base / "current/scripts/ops/daily-source-sweep.sh"
        self.state = root / "state"
        self.fake = root / "fake"
        self.fake.mkdir()
        self.spool = root / "data/source-sweep/spool"
        token = root / "slack-token"
        token.write_text("planted-token\n")
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FOUNDATION_SOURCE_SWEEP_STATE_ROOT": str(self.state),
            "FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE": str(token), "FAKE_STATE": str(self.fake),
            "FOUNDATION_SOURCE_SWEEP_SPOOL_DIR": str(self.spool),
            **{name: "planted-" + name.lower() for name in NEEDS},
            VWORLD_LOGIN["username"]["canonical"]: "planted-user", VWORLD_LOGIN["password"]["canonical"]: "planted-pass",
        }

    def raon_cap(self, value):
        catalog = json.loads(self.release_catalog.read_text(encoding="utf-8"))
        catalog["daily_collections"]["source_sweep"]["selection_archive_new_bytes_budget"] = value
        self.release_catalog.write_text(json.dumps(catalog), encoding="utf-8")

    def scenario(self, hub=HUB_QUIET, vworld=None, raon=None, members=MEMBERS_QUIET):
        (self.fake / "scenario.json").write_text(json.dumps({"hub": hub, "vworld": vworld or {}, "raon": raon,
                                                             "members": members}), encoding="utf-8")

    def run_job(self, env=None):
        return subprocess.run(["bash", str(self.script)], env=env or self.env, capture_output=True, text=True,
                              timeout=120)

    def read(self, name):
        path = self.fake / name
        return path.read_text(encoding="utf-8") if path.exists() else ""

    def journal(self):
        return (self.state / "journal.log").read_text(encoding="utf-8")

    def notice(self):
        return json.loads(CATALOG.read_text(encoding="utf-8"))["daily_collections"]["source_sweep"]["landed_bytes_notice"]

    def test_new_vworld_files_are_reported_with_what_was_pending_and_landed(self):
        landed = vfile("7", "succeeded")
        landed["size_bytes"] = 5 * 2**20
        self.scenario(vworld={"evidence": vworld_evidence([landed, vfile("8", "skipped_existing")])})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        line = self.journal().splitlines()[-1]
        self.assertIn("hub planned=3 new=0", line)
        self.assertIn("vworld planned=2 new=1 skipped=1 failed=0", line)
        self.assertIn(f"pending=1 pending_bytes={2**30} landed_bytes={5 * 2**20}", line)
        self.assertNotIn("ℹ️", self.read("slack.log"), "an ordinary day says nothing extra")
        self.assertIn("vworldkr__synthetic:9991-7", self.read("slack.log"))
        self.assertNotIn("planted-user", result.stdout + result.stderr + self.read("slack.log") + self.journal())
        # New files landed: the lanes that read these sources start (root ADR-0171).
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")
        self.assertEqual(job_specs.job_outcome(result.stdout), "changed")

    def test_a_killed_runs_spool_files_are_cleared_and_nothing_else(self):
        # A run killed mid-file (OOM, timeout) leaves its spool file; the next run starts empty.
        self.spool.mkdir(parents=True)
        (self.spool / ".provider-abc.part").write_bytes(b"partial")
        (self.spool / "keep.txt").write_text("not a spool file")
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(p.name for p in self.spool.iterdir()), ["keep.txt"])

    def test_the_unit_may_write_the_default_spool(self):
        unit = (PLATFORM / "infra/systemd" / UNIT).read_text(encoding="utf-8")
        writable = [path for line in unit.splitlines() if line.startswith("ReadWritePaths=")
                    for path in line.split("=", 1)[1].split()]
        script = SCRIPT.read_text(encoding="utf-8")
        default = script.split('FOUNDATION_SOURCE_SWEEP_SPOOL_DIR:-', 1)[1].split("}", 1)[0]
        self.assertTrue(default.startswith("/data/"), "the spool is on the data disk, not the small root disk")
        self.assertTrue(any(default == w or default.startswith(w.rstrip("/") + "/") for w in writable),
                        (default, writable))
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertTrue(any(w in release for w in writable if w.startswith("/data/")),
                        "the release creates the unit's data-disk path")

    def test_a_quiet_day_leaves_a_line_and_no_message(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("vworld planned=1 new=0 skipped=1", self.journal())
        self.assertIn("selection_archives_not_swept=3", self.journal(), "what the lane does not take is visible")
        self.assertEqual(self.read("slack.log"), "")
        # Nothing new: no lane is started by this run.
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")

    def test_a_huge_day_is_landed_and_said_once_never_refused(self):
        # Root ADR-0172: production's first sweeps found 881 files / 39.8 GB not held and the old budget
        # refused them every day. Now everything lands; above the catalog's notice one line says so.
        big = [vfile(str(n), "succeeded") for n in range(40)]
        for f in big:
            f["size_bytes"] = self.notice() // 20
        self.scenario(vworld={"evidence": vworld_evidence(big)})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"landed_bytes={40 * (self.notice() // 20)}", self.journal())
        messages = self.read("slack.log").splitlines()
        notices = [m for m in messages if "ℹ️" in m and "ADR-0172" in m]
        self.assertEqual(len(notices), 1, messages)
        self.assertFalse([m for m in messages if "🔴" in m], "a large day is not a failure")

    def test_a_failed_runs_reason_reaches_the_units_journal(self):
        # 2026-10-09: `journalctl -u` said only "exit 1"; the reason was in root-only files.
        self.scenario(hub={"die": True}, vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        summary = self.journal().splitlines()[-1].split(" ", 1)[1]
        self.assertIn(summary, result.stdout.splitlines(), "the journal line is the unit's line too")
        self.assertIn("  | hub ingest died", result.stderr, "the end of the run log is relayed")
        self.assertNotIn("planted-", result.stdout + result.stderr)

    def test_the_old_budget_is_gone_end_to_end(self):
        collection = json.loads(CATALOG.read_text(encoding="utf-8"))["daily_collections"]["source_sweep"]
        self.assertNotIn("new_bytes_budget", collection)
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_NEW_BYTES_BUDGET", script)
        self.assertNotIn("FOUNDATION_SOURCE_SWEEP_VWORLD_NEW_BYTES_BUDGET", script)
        runs = [c.get("run") for c in json.loads((PLATFORM / "config/runtime-secrets.contract.json")
                                                 .read_text(encoding="utf-8"))["consumers"]]
        self.assertNotIn("source-sweep-vworld-backlog", runs)

    def test_a_failed_hub_lane_does_not_stop_the_vworld_lane(self):
        self.scenario(hub={"die": True}, vworld={"evidence": vworld_evidence([vfile("7", "succeeded")])})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ingest-vworld-dataset-files", self.read("calls.log"))
        self.assertIn("hub status=no-evidence", self.journal())
        self.assertIn("hub", self.read("slack.log"))
        # A failed run states no outcome; Airflow records no events for it either way.
        self.assertNotIn("foundation-job-outcome", result.stdout)

    def test_yesterdays_evidence_is_not_read_as_todays(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("7", "succeeded")])})
        self.assertEqual(self.run_job().returncode, 0)
        self.scenario(vworld={"plan_dies": True})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0, "a lane that wrote nothing today has failed")
        self.assertIn("vworld status=no-evidence", self.journal().splitlines()[-1])

    def test_a_missing_setting_stops_the_job_before_anything_happens(self):
        self.assertLessEqual(set(REQUIRED), set(NEEDS), "the script requires only what the contract gives the unit")
        self.assertIn("FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID", REQUIRED)
        self.scenario(vworld={"evidence": vworld_evidence([])})
        logins = [VWORLD_LOGIN[role]["canonical"] for role in ("username", "password")]
        for name in [*REQUIRED, *logins]:
            with self.subTest(name):
                env = dict(self.env)
                del env[name]
                result = self.run_job(env)
                self.assertEqual(result.returncode, 78, result.stderr)
                self.assertIn(name, result.stderr)
                self.assertEqual(self.read("calls.log"), "", "nothing reached a provider or Bronze")
                self.assertFalse(self.state.exists(), "no state was written")

    # --- ZIP members (root ADR-0169 §1) ---

    def test_zip_members_are_measured_after_every_lane_with_the_read_only_pair(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")],
                                                          archives=[self.archive("70")])},
                      raon={"summary": raon_summary()}, members={"summary": members_summary(5, 0, 120)})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.read("calls.log").split()
        self.assertEqual(calls[-1], "measure-bronze-object-members", calls)
        self.assertEqual(calls.count("measure-bronze-object-members"), 1)
        self.assertLess(calls.index("raon-large-files.sh"), calls.index("measure-bronze-object-members"))
        seen = json.loads(self.read("members-env.json"))
        self.assertTrue(seen["FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID"])
        self.assertTrue(seen["FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY"])
        self.assertFalse(seen["FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID"], "the writer pair is dropped")
        self.assertFalse(seen["FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY"])
        self.assertTrue(seen["DATABASE_URL"])
        # The default source list lives once, in bronze-object-members.sh; the sweep passes none.
        members_script = (PLATFORM / "scripts/ops/bronze-object-members.sh").read_text(encoding="utf-8")
        self.assertIn(f"DEFAULT_SOURCES='{seen['sources']}'", members_script)
        self.assertNotIn(seen["sources"], SCRIPT.read_text(encoding="utf-8"))
        self.assertIn("| members measured=5 failed=0", self.journal().splitlines()[-1])
        # Measured, then the outcome: the Silver lanes the outcome starts find today's objects measured.
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome changed")

    def test_the_sweep_unit_loads_the_read_only_pair_for_the_members_step(self):
        members = PLATFORM / "scripts/ops/bronze-object-members.sh"
        self.assertLessEqual(runtime_secrets.script_requirements(members), set(NEEDS))
        reader = {"FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID",
                  "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY"}
        # Required by the sweep itself, so a missing reader file stops it before any lane runs.
        self.assertLessEqual(reader, set(REQUIRED))
        unit = (PLATFORM / "infra/systemd" / UNIT).read_text(encoding="utf-8")
        self.assertIn("EnvironmentFile=/etc/foundation-platform/lakehouse-reader.env", unit)

    def test_a_measuring_failure_turns_the_sweep_red_and_keeps_what_landed(self):
        landed = vfile("7", "succeeded")
        landed["size_bytes"] = 2**20
        self.scenario(vworld={"evidence": vworld_evidence([landed])},
                      members={"rc": 1, "summary": members_summary(3, 2),
                               "stderr": "cannot measure object_key=bronze/x.zip reason=reset\n"})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0, "a measuring failure is not a quiet day")
        line = self.journal().splitlines()[-1]
        self.assertIn("vworld planned=1 new=1", line, "what landed is still said")
        self.assertIn("| members measured=3 failed=2", line)
        message = self.read("slack.log")
        self.assertIn("🔴", message)
        self.assertIn("members", message.split("레인 실패")[0])
        self.assertIn("  | cannot measure object_key=bronze/x.zip", result.stderr, "which objects, from the run log")
        # A failed run states no outcome: the Silver lanes would refuse the unmeasured objects anyway.
        self.assertNotIn("foundation-job-outcome", result.stdout)

    def test_a_failed_count_with_a_zero_exit_is_still_red(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])},
                      members={"rc": 0, "summary": members_summary(0, 1)})
        self.assertNotEqual(self.run_job().returncode, 0)

    def test_a_measuring_command_that_says_nothing_is_red(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])},
                      members={"rc": 1, "summary": None})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("| members status=no-summary rc=1", self.journal().splitlines()[-1])

    def test_measuring_runs_when_the_lanes_failed(self):
        self.scenario(hub={"die": True}, vworld={"plan_dies": True}, members={"summary": members_summary(4)})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("measure-bronze-object-members", self.read("calls.log"))
        self.assertIn("| members measured=4 failed=0", self.journal().splitlines()[-1])

    # --- which files failed, and why (root ADR-0174) ---

    def test_failed_files_and_their_reasons_reach_the_units_journal_masked(self):
        # 2026-10-10: the journal said `failed=3` for VWorld and nobody could see which or why.
        files = [{**vfile(str(n), "failed"), "error_message": f"provider answered 502 for file {n}"}
                 for n in range(25)]
        files[0]["error_message"] = ("connect postgres://foundation_admin:planted-password@127.0.0.1/foundation "
                                     "refused\nsecond line\twith a tab " + "x" * 400)
        files[1]["error_message"] = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=planted-secret"
        files[2]["error_message"] = None
        self.scenario(vworld={"rc": 1, "evidence": vworld_evidence([*files, vfile("99", "succeeded")])})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        failed = [line for line in result.stderr.splitlines() if line.startswith("failed ")]
        self.assertEqual(len(failed), 21, "twenty files and how many more")
        self.assertEqual(failed[3], "failed vworldkr__synthetic:9991-3 provider answered 502 for file 3")
        self.assertTrue(failed[0].startswith("failed vworldkr__synthetic:9991-0 connect postgres://foundation_admin:"
                                             "***@127.0.0.1/foundation refused second line with a tab xxx"), failed[0])
        reason = failed[0].split(" ", 2)[2]
        self.assertEqual(len(reason), 200 + 3, "the reason is cut to 200 characters")
        self.assertIn("SECRET_ACCESS_KEY=***", failed[1])
        self.assertEqual(failed[2], "failed vworldkr__synthetic:9991-2 no reason recorded")
        self.assertEqual(failed[-1], "failed vworld:+5 more failed files, not listed here")
        self.assertNotIn("9991-99", "\n".join(failed), "a landed file is not a failure")
        self.assertNotIn("planted-", result.stdout + result.stderr)

    def test_a_failed_hub_file_names_itself(self):
        hub = {"evidence": {**HUB_QUIET["evidence"], "failed_job_count": 1, "status": "blocked", "jobs": [
            {"source_slug": "hubgokr__synthetic", "provider_file_id": "F-1", "status": "failed",
             "error_message": "bearer planted-token rejected"},
            {"source_slug": "hubgokr__synthetic", "provider_file_id": "F-2", "status": "skipped_existing"}]}}
        self.scenario(hub=hub, vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        failed = [line for line in result.stderr.splitlines() if line.startswith("failed ")]
        self.assertEqual(failed, ["failed hubgokr__synthetic:F-1 bearer *** rejected"])

    def test_a_quiet_day_names_no_failed_file(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")])})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotRegex(result.stderr, r"(?m)^failed ")

    # --- the large-file lane (root ADR-0170) ---

    def archive(self, file_no, status="deferred_selection_archive", size_bytes=2**30):
        return {**vfile(file_no, status), "size_bytes": size_bytes}

    def test_a_zero_cap_switches_the_large_file_lane_off_and_says_what_waits(self):
        self.raon_cap(0)
        self.scenario(vworld={"evidence": vworld_evidence(
            [vfile("8", "skipped_existing")],
            archives=[self.archive("70", size_bytes=3 * 2**30), self.archive("71", size_bytes=2**30),
                      self.archive("72", "skipped_existing", size_bytes=5 * 2**30)])},
            raon={"rc": 1, "summary": None})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, "a switched-off lane is not a failure: " + result.stderr)
        self.assertEqual(self.read("raon.log"), "", "the lane's script is not called at all")
        line = self.journal().splitlines()[-1]
        self.assertIn(f"raon deferred=2 listed_bytes={4 * 2**30} budget=0 status=lane-off", line)
        self.assertIn("files=vworldkr__synthetic:9991-70,vworldkr__synthetic:9991-71", line)
        self.assertNotIn("9991-72", line, "a held archive is not waiting")
        message = self.read("slack.log")
        self.assertEqual(len(message.splitlines()), 1, "one informational line")
        self.assertIn("RAON 대용량 파일 2건", message)
        self.assertNotIn("🔴", message)

    def test_a_switched_off_lane_does_not_hide_another_lanes_failure(self):
        self.raon_cap(0)
        self.scenario(vworld={"rc": 1, "evidence": vworld_evidence([vfile("7", "failed")],
                                                                   archives=[self.archive("70")])})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.read("raon.log"), "")
        failure = self.read("slack.log").split("레인 실패")[0]
        self.assertIn("vworld", failure)
        self.assertNotIn("raon", failure.split("daily-source-sweep:")[1])

    def test_an_operator_cap_runs_the_lane_the_catalog_switched_off(self):
        self.raon_cap(0)
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"summary": raon_summary()})
        result = self.run_job({**self.env, "FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET": str(2**34)})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("run ", self.read("raon.log"))

    def test_a_cap_that_is_not_a_byte_count_is_a_failed_lane(self):
        self.raon_cap("lots")
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"summary": raon_summary()})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.read("raon.log"), "")
        self.assertIn("raon deferred=1 status=invalid-budget", self.journal())
        self.assertIn("🔴", self.read("slack.log"))

    def test_without_deferred_archives_the_large_file_lane_does_not_run(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")],
                                                          archives=[self.archive("70", "skipped_existing")])})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.read("raon.log"), "", "held archives are nothing to fetch")
        self.assertIn("| raon deferred=0", self.journal())

    def test_deferred_archives_are_handed_to_the_large_file_lane_with_todays_evidence(self):
        self.scenario(vworld={"evidence": vworld_evidence([vfile("8", "skipped_existing")],
                                                          archives=[self.archive("70")])},
                      raon={"summary": raon_summary()})
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.read("raon.log").split(), ["run", str(self.state / "vworld-evidence.json")])
        line = self.journal().splitlines()[-1]
        self.assertIn("raon deferred=1 planned=1 committed=1 failed=0", line)
        self.assertIn("status=ready", line)
        self.assertIn("vworldkr__synthetic:9991-70", self.read("slack.log"), "a landed large file is news")

    def test_a_missing_large_file_prerequisite_turns_the_sweep_red_loudly(self):
        # The lane's script refuses with 78 before any side effect (no package, no docker); the
        # sweep must not read that as a quiet day.
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"rc": 78, "summary": None})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("raon deferred=1 status=no-summary rc=78", self.journal())
        message = self.read("slack.log")
        self.assertIn("🔴", message)
        self.assertIn("raon", message)
        self.assertIn("ADR-0170", message)

    def test_large_files_over_their_budget_are_refused_and_said(self):
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"rc": 1, "summary": raon_summary(status="blocked_new_bytes_budget", committed=0,
                                                             failed=0, files=[])})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("status=blocked_new_bytes_budget", self.journal().splitlines()[-1])
        self.assertIn("RAON 대용량 파일", self.read("slack.log"))

    def test_a_failed_large_file_is_a_failed_lane(self):
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"rc": 1, "summary": raon_summary(status="failed", committed=0, failed=1, files=[])})
        self.assertNotEqual(self.run_job().returncode, 0)
        self.assertIn("raon", self.read("slack.log"))

    def test_the_large_file_lane_runs_even_when_the_vworld_lane_failed(self):
        self.scenario(vworld={"rc": 1, "evidence": vworld_evidence(
            [vfile("7", "failed")], status="blocked", archives=[self.archive("70")])},
            raon={"summary": raon_summary()})
        result = self.run_job()
        self.assertNotEqual(result.returncode, 0, "the vworld lane still failed")
        self.assertIn("run ", self.read("raon.log"), "lanes are independent")
        self.assertNotIn("raon", self.read("slack.log").split("레인 실패")[0].split("daily-source-sweep:")[1])

    def test_yesterdays_large_file_summary_is_not_read_as_todays(self):
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"summary": raon_summary()})
        self.assertEqual(self.run_job().returncode, 0)
        self.scenario(vworld={"evidence": vworld_evidence([], archives=[self.archive("70")])},
                      raon={"rc": 1, "summary": None})
        self.assertNotEqual(self.run_job().returncode, 0)
        self.assertIn("raon deferred=1 status=no-summary rc=1", self.journal().splitlines()[-1])

    def test_the_large_file_script_needs_nothing_the_sweep_unit_does_not_load(self):
        # The sweep runs the lane's script rather than sourcing it, so the contract check of the unit
        # does not follow it: its requirements must be the unit's.
        raon = PLATFORM / "scripts/ops/raon-large-files.sh"
        self.assertLessEqual(runtime_secrets.script_requirements(raon), set(NEEDS))

    def test_a_deprecated_login_name_is_accepted(self):
        self.scenario(vworld={"evidence": vworld_evidence([])})
        env = dict(self.env)
        for role in ("username", "password"):
            del env[VWORLD_LOGIN[role]["canonical"]]
            env[VWORLD_LOGIN[role]["deprecated_aliases"][0]] = "planted-alias"
        result = self.run_job(env)
        self.assertEqual(result.returncode, 0, result.stderr)


class SweptDatasets(unittest.TestCase):
    """The catalog is the one list of what the VWorld lane sweeps (root ADR-0168)."""

    def setUp(self):
        self.catalog = json.loads(CATALOG.read_text(encoding="utf-8"))

    def swept(self):
        return [e for e in self.catalog["endpoints"] if e.get("daily_collection") == "source_sweep"]

    def test_every_named_collection_is_declared_with_a_notice(self):
        declared = self.catalog["daily_collections"]
        for endpoint in self.catalog["endpoints"]:
            if "daily_collection" in endpoint:
                self.assertIn(endpoint["daily_collection"], declared, endpoint["endpoint_slug"])
        for name, collection in declared.items():
            self.assertIsInstance(collection["landed_bytes_notice"], int, name)
            self.assertGreater(collection["landed_bytes_notice"], 0, name)
            # Zero is a decision, not a gap (root ADR-0170): the large-file lane fetches nothing on
            # its own until an operator's supervised run has measured it.
            self.assertIsInstance(collection["selection_archive_new_bytes_budget"], int, name)
            self.assertGreaterEqual(collection["selection_archive_new_bytes_budget"], 0, name)

    def test_the_sweep_takes_only_vworld_dataset_files(self):
        self.assertTrue(self.swept(), "the VWorld lane sweeps something")
        for endpoint in self.swept():
            self.assertEqual(endpoint["group"], "vworld_dataset", endpoint["endpoint_slug"])
            self.assertEqual(endpoint["source_acquisition_lane"], "provider_dataset_file")
            self.assertTrue(endpoint["national_collection_allowed"])
            self.assertIn("provider_dataset_selector", endpoint)

    def test_a_dataset_with_its_own_collection_job_is_not_swept_twice(self):
        # Their own units collect them under their own rules (ADR-0148, ADR-0150/0152); a second
        # collector would race them for the same file numbers. Each job names its endpoint once.
        sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
        import vworld_parcel_editions  # noqa: E402
        import vworld_parcel_number_change_history  # noqa: E402

        own = {vworld_parcel_editions.PROVIDER_ENDPOINT,
               vworld_parcel_number_change_history.load_source_contract()["endpoint_slug"]}
        self.assertFalse(own & {e["endpoint_slug"] for e in self.swept()})


if __name__ == "__main__":
    unittest.main()
