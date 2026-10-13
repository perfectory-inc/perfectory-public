"""The operator's root steps of a section pack cut-over (scripts/ops/by-pnu-pack-operator.sh, root ADR-0161).

sudo grants this one script without a password, so what it must never do matters as much as what
it does: it takes a lane, an action and a generation or version id, refuses anything else before
running a thing, and runs only the admitted release's publisher with the environment files the
runtime-secrets contract names. A fake systemd-run records each unit it would start.
"""

import fcntl
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "ops" / "by-pnu-pack-operator.sh"
CONTRACT = json.loads((PLATFORM / "config" / "r2-connections.contract.json").read_text(encoding="utf-8"))
RELEASE_ID = "c" * 40
VERSION = "33333333-3333-4333-8333-333333333333"
OLD = "44444444-4444-4444-8444-444444444444"

# A unit whose name holds a FAKE_FAIL entry (comma separated) fails, as a --wait run reports it. The
# Gold rebuild's unit writes FAKE_GOLD_OUTPUT to its output file.
FAKE_SYSTEMD_RUN = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"tool": "systemd-run", "args": sys.argv[1:]}) + "\n")
unit = next((arg for arg in sys.argv[1:] if arg.startswith("--unit=")), "")
if "gold-panel-rebuild" in unit and os.environ.get("FAKE_GOLD_OUTPUT"):
    output = next(arg for arg in sys.argv[1:] if arg.startswith("StandardOutput=append:"))
    with open(output.split(":", 1)[1], "a") as out:
        out.write(os.environ["FAKE_GOLD_OUTPUT"])
if any(part and part in unit for part in os.environ.get("FAKE_FAIL", "").split(",")):
    sys.exit(3)
'''

# Each preview answers FAKE_CURL_CODE (200 when unset).
FAKE_CURL = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"tool": "curl", "args": [sys.argv[-1]]}) + "\n")
print(os.environ.get("FAKE_CURL_CODE", "200"), end="")
'''

# Units named in FAKE_ACTIVE (comma separated) are active, those in FAKE_ACTIVATING are a oneshot
# job's run (`activating`, which `is-active` calls not active), FAKE_STATES gives "unit=state" pairs
# (a failed unit too, which `list-units --all` shows); every other unit is inactive. Under
# FAKE_SYSTEMD_DOWN no listing answers.
FAKE_SYSTEMCTL = r'''#!/usr/bin/env python3
import fnmatch, os, sys
def service(unit):
    return unit if unit.endswith(".service") else unit + ".service"
states = {service(unit): "active" for unit in os.environ.get("FAKE_ACTIVE", "").split(",") if unit}
states.update({service(unit): "activating" for unit in os.environ.get("FAKE_ACTIVATING", "").split(",") if unit})
states.update({service(pair.split("=")[0]): pair.split("=")[1]
               for pair in os.environ.get("FAKE_STATES", "").split(",") if pair})
args = sys.argv[1:]
named = [arg for arg in args[1:] if not arg.startswith("-")]
if args[0] == "is-active":
    answers = [states.get(service(unit), "inactive") for unit in named]
    if "--quiet" not in args:
        print("\n".join(answers))
    sys.exit(0 if all(answer == "active" for answer in answers) else 3)
if args[0] == "list-units":
    if os.environ.get("FAKE_SYSTEMD_DOWN"):
        sys.exit("Failed to connect to bus")
    assert "--all" in args and "--type=service" in args, args
    for unit, state in states.items():
        if any(fnmatch.fnmatch(unit, pattern) for pattern in named):
            print(f"{unit} loaded {state} x fixture")
    sys.exit(0)
if args[0] in ("start", "status", "enable"):
    import json
    with open(os.environ["FAKE_LOG"], "a") as log:
        log.write(json.dumps({"tool": "systemctl", "args": args}) + "\n")
    sys.exit(0)
sys.exit(f"fake systemctl: {args}")
'''

FAKE_JOURNALCTL = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"tool": "journalctl", "args": sys.argv[1:]}) + "\n")
'''

FAKE_HEALTH = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"tool": "health", "args": sys.argv[1:]}) + "\n")
'''

FAKE_PUBLISHER = "#!/usr/bin/env bash\nexit 0\n"


class OperatorHarness(unittest.TestCase):
    """A release on a temporary host: its contracts, its operator scripts, fake systemd and curl."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="pack-operator-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        base = self.root / "opt/foundation-platform"
        release = base / "releases" / RELEASE_ID
        # What the operator reads from the release: its contracts, its secrets tool, its admission.
        for part in ("config", "scripts/deploy", "infra/systemd", "infra/lakehouse/contracts", "orchestration"):
            shutil.copytree(PLATFORM / part, release / part)
        # The lanes' derived fields and the rebuild's tables (root ADR-0193 §9) are read through these.
        shutil.copytree(PLATFORM / "infra/lakehouse/spark/jobs", release / "infra/lakehouse/spark/jobs",
                        ignore=shutil.ignore_patterns("__pycache__"))
        (release / "scripts/ops").mkdir(parents=True)
        # The granted copy hands every action to the release's own copy of itself.
        for name in ("admitted-writer-runtime.sh", "by-pnu-pack-operator.sh", "by-pnu-pack-bake.sh",
                     "by-pnu-bake-shards.sh", "by-pnu-lane-cutover.sh"):
            shutil.copy(PLATFORM / "scripts/ops" / name, release / "scripts/ops")
        health = release / "scripts/ops/by-pnu-gateway-health.sh"
        health.write_text(FAKE_HEALTH)
        health.chmod(0o755)
        (base / "current").symlink_to(pathlib.Path("releases") / RELEASE_ID)
        self.release = release
        artifacts = base / "artifacts" / RELEASE_ID
        artifacts.mkdir(parents=True)
        self.publisher = artifacts / "foundation-outbox-publisher"
        self.publisher.write_text(FAKE_PUBLISHER)
        self.publisher.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({
            "source": RELEASE_ID, "publisher_image": "sha256:" + "a" * 64,
            "tippecanoe_image": "sha256:" + "d" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(FAKE_PUBLISHER.encode()).hexdigest(),
                      "jars/fixture.jar": "0" * 64}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        for name, body in (("systemd-run", FAKE_SYSTEMD_RUN), ("systemctl", FAKE_SYSTEMCTL),
                           ("journalctl", FAKE_JOURNALCTL), ("curl", FAKE_CURL)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        self.bake = self.root / "data/by-pnu-bake"
        self.etc = self.root / "etc"
        self.etc.mkdir()
        self.log = self.root / "calls.jsonl"
        owner = subprocess.run(["id", "-un"], capture_output=True, text=True, check=True).stdout.strip()
        group = subprocess.run(["id", "-gn"], capture_output=True, text=True, check=True).stdout.strip()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_LOG": str(self.log),
            "FOUNDATION_OPERATOR_RELEASE_ROOT": str(base / "current"),
            "FOUNDATION_OPERATOR_SYSTEMD_RUN": str(bin_dir / "systemd-run"),
            "FOUNDATION_OPERATOR_BAKE_ROOT": str(self.bake),
            "FOUNDATION_OPERATOR_ETC": str(self.etc),
            "FOUNDATION_OPERATOR_SERVICE_OWNER": owner,
            "FOUNDATION_OPERATOR_ROOT_OWNER": owner,
            "FOUNDATION_OPERATOR_SERVICE_GROUP": group,
        }

    def work(self, lane="parcel", generation=1):
        work = self.bake / f"{lane}-pack-g{generation}"
        (work / "summaries").mkdir(parents=True, exist_ok=True)
        (work / "summaries/shard-11.json").write_text(json.dumps({"gold_iceberg_snapshot_id": "777"}))
        return work

    def run_operator(self, *args, active=(), activating=(), states=None, **env):
        result = subprocess.run(["bash", str(SCRIPT), *args],
                                env={**self.env, "FAKE_ACTIVE": ",".join(active), "FAKE_ACTIVATING": ",".join(activating),
                                     "FAKE_STATES": ",".join(f"{unit}={state}" for unit, state in (states or {}).items()),
                                     **env},
                                capture_output=True, text=True, timeout=60)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return result, calls

    def unit(self, calls):
        (call,) = [call for call in calls if call["tool"] == "systemd-run"]
        args = call["args"]
        env = dict(args[index + 1].split("=", 1) for index, arg in enumerate(args) if arg == "-E")
        props = [args[index + 1] for index, arg in enumerate(args) if arg == "-p"]
        return args, env, props


class PackOperator(OperatorHarness):
    def test_silver_plan_runs_the_lanes_plan_as_the_service_with_its_contract_environment(self):
        # 2026-10-10: the owner had to run and paste a sudo loop for every lane plan (root ADR-0178).
        result, calls = self.run_operator("silver", "plan", "land-individual-price")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, _, props = self.unit(calls)
        self.assertIn("--wait", args)
        self.assertIn("--pipe", args)
        self.assertEqual(args[-2:], ["land-individual-price", "--plan"])
        self.assertTrue(args[-3].endswith("/scripts/ops/silver-refresh.sh"))
        self.assertTrue(any(prop.startswith("EnvironmentFile=") for prop in props), props)

    def test_silver_start_and_status_name_the_lanes_scheduled_unit(self):
        result, calls = self.run_operator("silver", "start", "building-register-titles")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([c["args"] for c in calls if c["tool"] == "systemctl"],
                         [["start", "--no-block", "foundation-silver-refresh@building-register-titles.service"]])
        result, calls = self.run_operator("silver", "status", "building-register-titles")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(c["tool"] == "journalctl" and
                            "foundation-silver-refresh@building-register-titles.service" in c["args"] for c in calls))

    def test_silver_start_waits_for_another_lane_and_the_gold_rebuild(self):
        for active in (["foundation-silver-refresh@land-use-plan.service"],
                       ["foundation-gold-panel-rebuild.service"]):
            with self.subTest(active=active):
                result, calls = self.run_operator("silver", "start", "land-characteristic", active=active)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertFalse([c for c in calls if c["tool"] == "systemctl" and c["args"][0] == "start"])

    def test_a_running_oneshot_job_is_running(self):
        # A oneshot job is `activating` for its whole run; `is-active` called a running Gold rebuild
        # stopped, so a bake could start under it (2026-10-10).
        result, calls = self.run_operator("parcel", "bake", "1", activating=["foundation-gold-panel-rebuild.service"])
        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertFalse([c for c in calls if c["tool"] == "systemd-run"])

    def test_running_is_the_one_definition_every_state_but_inactive_and_failed(self):
        # job_units.py decides (root ADR-0184): a unit stopping or reloading still runs; a failed one
        # does not hold anything back. Each refusal site asks it, the pack bakes' pattern and the
        # generation's own unit too.
        self.work()
        for state in ("deactivating", "reloading", "maintenance"):
            for unit, args in (("foundation-gold-panel-rebuild.service", ("parcel", "bake", "1")),
                               ("foundation-building-pack-bake-g7.service", ("parcel", "gold-rebuild")),
                               ("foundation-parcel-pack-equality-g1.service", ("parcel", "equality", "1")),
                               ("foundation-silver-refresh@land-use-plan.service",
                                ("silver", "start", "land-characteristic"))):
                with self.subTest(state=state, unit=unit):
                    self.log.unlink(missing_ok=True)
                    result, calls = self.run_operator(*args, states={unit: state})
                    self.assertEqual(result.returncode, 75, result.stderr)
                    self.assertFalse([c for c in calls if c["tool"] == "systemd-run" or c["args"][:1] == ["start"]])
        result, calls = self.run_operator("parcel", "bake", "1", states={"foundation-gold-panel-rebuild.service": "failed"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue([c for c in calls if c["tool"] == "systemd-run"])

    def test_a_systemd_that_cannot_answer_starts_nothing(self):
        result, calls = self.run_operator("parcel", "bake", "1", FAKE_SYSTEMD_DOWN="1")
        self.assertEqual(result.returncode, 70, result.stderr)
        self.assertIn("systemd cannot tell whether", result.stderr)
        self.assertFalse([c for c in calls if c["tool"] == "systemd-run"])

    def test_raon_build_and_plan_wait_as_the_service_with_the_contract_environment(self):
        for action, tail in (("build", ["build"]),
                             ("plan", ["plan", "/var/lib/foundation-platform/source-sweep/vworld-evidence.json"])):
            with self.subTest(action=action):
                self.log.unlink(missing_ok=True)
                result, calls = self.run_operator("raon", action)
                self.assertEqual(result.returncode, 0, result.stderr)
                args, _, props = self.unit(calls)
                self.assertIn("--wait", args)
                self.assertTrue(args[-len(tail) - 1].endswith("/scripts/ops/raon-large-files.sh"), args)
                self.assertEqual(args[-len(tail):], tail)
                self.assertTrue(any(prop.startswith("EnvironmentFile=") for prop in props), props)

    def test_raon_run_starts_one_detached_unit_with_the_runs_limits(self):
        result, calls = self.run_operator("raon", "run", "1", "1600000000")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, _ = self.unit(calls)
        self.assertIn("--unit=foundation-raon-large-files-operator", args)
        self.assertNotIn("--wait", args)
        self.assertEqual(env, {"FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET": "1600000000",
                               "FOUNDATION_RAON_LARGE_FILES_MAX_FILES": "1"})
        self.log.unlink()
        result, calls = self.run_operator("raon", "run", "all", "13000000000")
        self.assertEqual(result.returncode, 0, result.stderr)
        _, env, _ = self.unit(calls)
        self.assertNotIn("FOUNDATION_RAON_LARGE_FILES_MAX_FILES", env)

    def test_raon_never_overlaps_the_daily_sweep_or_itself_and_takes_only_its_words(self):
        for running in (["foundation-source-sweep.service"], ["foundation-raon-large-files-operator.service"]):
            with self.subTest(running=running):
                result, calls = self.run_operator("raon", "run", "1", "1000", activating=running)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertFalse([c for c in calls if c["tool"] == "systemd-run"])
        for args in (("raon", "run"), ("raon", "run", "0", "1000"), ("raon", "run", "1", "-5"),
                     ("raon", "run", "1", "1000", "extra"), ("raon", "plan", "/etc/passwd"),
                     ("raon", "drop"), ("raon", "run", "1", "1e9")):
            with self.subTest(args=args):
                result, calls = self.run_operator(*args)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertFalse([c for c in calls if c["tool"] == "systemd-run"])

    def test_silver_takes_only_a_listed_lane_and_a_known_action(self):
        for args in (("silver", "plan", "not-a-lane"), ("silver", "plan", "../../etc/passwd"),
                     ("silver", "plan"), ("silver", "drop", "land-use-plan"),
                     ("silver", "plan", "land-use-plan", "extra")):
            with self.subTest(args=args):
                result, calls = self.run_operator(*args)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertFalse([c for c in calls if c["tool"] in ("systemd-run", "systemctl")])

    def test_bake_runs_the_release_bake_script_with_the_contracts_files_and_memory_per_worker(self):
        # Nothing is baked yet: the bake is the one action that needs no summaries.
        result, calls = self.run_operator("parcel", "bake", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, props = self.unit(calls)
        work = self.bake / "parcel-pack-g2"
        self.assertIn("--unit=foundation-parcel-pack-bake-g2", args)
        # The release's own copy of the bake script, by its physical path, with the lane and generation.
        self.assertEqual(args[-3:], [str((self.release / "scripts/ops/by-pnu-pack-bake.sh").resolve()), "parcel", "2"])
        self.assertEqual(env, {})
        self.assertIn("User=foundation-platform", props)
        self.assertIn(f"StandardOutput=append:{work}/logs/bake.log", props)
        self.assertTrue((work / "logs").is_dir())
        # The environment files are the contract's for this run, not a list kept in the script.
        expected = subprocess.run(["python3", "scripts/deploy/runtime_secrets.py", "properties", "section-pack-bake"],
                                  cwd=PLATFORM, capture_output=True, text=True, check=True).stdout.split()
        self.assertEqual([prop for prop in props if prop.startswith("EnvironmentFile=")], expected[1::2])
        # One shard export's measured bound (the scheduled bake's MemoryMax) per worker.
        unit = (PLATFORM / "infra/systemd/foundation-by-pnu-serving-bake.service").read_text(encoding="utf-8")
        shard = int(next(line for line in unit.splitlines() if line.startswith("MemoryMax="))[len("MemoryMax="):-1])
        shards = (PLATFORM / "scripts/ops/by-pnu-bake-shards.sh").read_text(encoding="utf-8")
        workers = int(next(line for line in shards.splitlines() if line.startswith("BY_PNU_PACK_BAKE_WORKERS="))
                      .split("=")[1])
        self.assertIn(f"MemoryMax={shard * workers}G", props)
        self.assertIn("OOMPolicy=continue", props)

    def test_bake_and_gold_rebuild_refuse_while_the_other_or_a_scheduled_one_runs(self):
        for active in (["foundation-gold-panel-rebuild.service"], ["foundation-gold-panel-rebuild-unconditional"]):
            with self.subTest(active=active):
                result, calls = self.run_operator("parcel", "bake", "1", active=active)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertEqual(calls, [])
        for active in (["foundation-gold-panel-rebuild.service"], ["foundation-gold-panel-rebuild-unconditional"],
                       ["foundation-by-pnu-serving-bake.service"], ["foundation-building-pack-bake-g3.service"]):
            with self.subTest(active=active):
                result, calls = self.run_operator("parcel", "gold-rebuild", active=active)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertEqual(calls, [])
        # A lane lock held by anything (a hand-run publish, a bake starting up) refuses too, and
        # the lock is never created by the check.
        self.assertFalse((self.bake / "parcel/lane.lock").exists())
        result, calls = self.run_operator("parcel", "gold-rebuild")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.bake / "parcel/lane.lock").exists())
        self.log.unlink()
        (self.bake / "building").mkdir(parents=True)
        with open(self.bake / "building/lane.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result, calls = self.run_operator("parcel", "gold-rebuild")
        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertIn("building/lane.lock", result.stderr)
        self.assertEqual(calls, [])

    def test_kind_sizes_runs_the_read_only_measurement_as_the_service_never_beside_a_rebuild_or_bake(self):
        # Root ADR-0193 §6-1: the measure job's only way onto the host is this step.
        result, calls = self.run_operator("parcel", "kind-sizes")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, props = self.unit(calls)
        self.assertIn("--unit=foundation-parcel-kind-sizes", args)
        self.assertIn("--no-block", args)
        self.assertTrue(args[-2].endswith("/scripts/ops/by-pnu-kind-sizes.sh"), args)
        self.assertEqual(args[-1], "parcel")
        self.assertTrue(any(prop.startswith("User=") for prop in props), props)
        secrets = json.loads((PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
        catalog = next(g["path"] for g in secrets["groups"] if g["name"] == "map-edit-fold")
        self.assertIn(f"EnvironmentFile={catalog}", props)
        self.assertEqual(env, {})
        for active in (["foundation-gold-panel-rebuild.service"], ["foundation-by-pnu-serving-bake.service"],
                       ["foundation-parcel-kind-sizes.service"]):
            with self.subTest(active=active):
                self.log.unlink(missing_ok=True)
                result, calls = self.run_operator("parcel", "kind-sizes", active=active)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertEqual(calls, [])
        for args in (("parcel", "kind-sizes", "1"), ("unit-price", "kind-sizes")):
            with self.subTest(args=args):
                result, calls = self.run_operator(*args)
                self.assertEqual(result.returncode, 64, result.stderr)

    def test_gold_rebuild_is_the_scheduled_unit_with_a_fixed_unconditional_reason(self):
        result, calls = self.run_operator("building", "gold-rebuild")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, props = self.unit(calls)
        self.assertIn("--unit=foundation-gold-panel-rebuild-unconditional", args)
        self.assertIn("--no-block", args)
        self.assertEqual(env, {})
        # The scheduled unit's command, both tables, with the one reason naming its decision.
        self.assertEqual(args[-4:-1], ["/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh", "all",
                                       "--unconditional"])
        self.assertIn("ADR-0166", args[-1])
        # Everything else the scheduled unit states, so the run honours its account, files, time
        # bound, Spark cleanup and state directory (where the rebuild's lock lives).
        unit = (PLATFORM / "infra/systemd/foundation-gold-panel-rebuild.service").read_text(encoding="utf-8")
        for line in unit.splitlines():
            key = line.split("=", 1)[0]
            if "=" in line and not line.startswith("#") and key not in ("Description", "ExecStart", "OnFailure"):
                self.assertIn(line, props)
        self.assertIn("OnFailure=foundation-unit-failed@foundation-gold-panel-rebuild-unconditional.service.service",
                      props)
        secrets = subprocess.run(["python3", "scripts/deploy/runtime_secrets.py", "properties",
                                  "foundation-gold-panel-rebuild.service"],
                                 cwd=PLATFORM, capture_output=True, text=True, check=True).stdout.split()
        self.assertEqual([prop for prop in props if prop.startswith("EnvironmentFile=")], secrets[1::2])
        self.assertIn(f"StandardOutput=append:{self.bake}/gold-rebuild/logs/gold-rebuild.log", props)
        # A unit the transient copy cannot reproduce is refused, not half-copied.
        unit_file = self.release / "infra/systemd/foundation-gold-panel-rebuild.service"
        unit_file.write_text(unit.replace("gold-panel-rebuild.sh all", "gold-panel-rebuild.sh parcel"))
        self.log.unlink()
        result, calls = self.run_operator("building", "gold-rebuild")
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertEqual(calls, [])

    def test_status_shows_the_bake_and_the_gold_units(self):
        result, _ = self.run_operator("parcel", "status", "1", active=["foundation-gold-panel-rebuild-unconditional"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("foundation-parcel-pack-bake-g1: inactive", result.stdout)
        self.assertIn("foundation-gold-panel-rebuild-unconditional: active", result.stdout)
        self.assertIn("foundation-gold-panel-rebuild.service: inactive", result.stdout)
        self.assertIn("foundation-by-pnu-serving-bake.service: inactive", result.stdout)

    def test_the_equality_gate_is_told_its_generation_by_the_action(self):
        # 2026-10-13: the first parcel kind lane's gate (가) stopped with "PACK_GENERATION is
        # required"; the earlier lanes had it from an environment file a new lane does not have.
        for lane, generation in (("parcel", "2"), ("parcel-zoning", "1")):
            with self.subTest(lane=lane):
                if self.log.exists():
                    self.log.unlink()
                work = self.work(lane, int(generation))
                result, calls = self.run_operator(lane, "equality", generation)
                self.assertEqual(result.returncode, 0, result.stderr)
                args, env, _ = self.unit(calls)
                prefix = "FOUNDATION_PLATFORM_" + lane.replace("-", "_").upper() + "_BY_PNU_SERVING_"
                self.assertEqual(env[prefix + "PACK_GENERATION"], generation)
                self.assertEqual(env[prefix + "PACK_SUMMARY_DIR"], f"{work}/summaries")
                self.assertEqual(args[-1], f"verify-{lane}-by-pnu-section-pack-equality")

    def test_the_latency_gate_runs_the_release_publisher_against_the_lane_preview(self):
        work = self.work()
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        (work / "latency.json").write_text("{}")
        result, calls = self.run_operator("parcel", "latency", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, props = self.unit(calls)
        self.assertIn("--unit=foundation-parcel-pack-latency-g1", args)
        self.assertEqual(args[-1], "probe-parcel-by-pnu-section-pack-latency")
        self.assertEqual(pathlib.Path(args[-2]).resolve(), self.publisher.resolve())
        preview = CONTRACT["parcel_by_pnu_gateway"]["section_packs"]["preview_worker"]["public_hostname"]
        prefix = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_"
        self.assertEqual(env[prefix + "PACK_PREVIEW_BASE_URL"], f"https://{preview}")
        self.assertEqual(env[prefix + "PACK_LATENCY_EVIDENCE_PATH"], f"{work}/latency.json")
        self.assertFalse([name for name in env if "BUILDING" in name], env)
        # The environment files are the contract's for this run, not a list kept in the script.
        secrets = json.loads((PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
        analytics = next(g["path"] for g in secrets["groups"] if g["name"] == "cloudflare-analytics")
        self.assertIn(f"EnvironmentFile={analytics}", props)
        # The earlier evidence is kept, never overwritten.
        self.assertEqual(len(list(work.glob("latency-*.json"))), 1)

    def test_a_kind_lane_is_probed_through_its_hosts_preview(self):
        # Root ADR-0193 §4: a lane written as a host and a kind has no Worker of its own; its host's
        # preview serves its unpublished generation under its own path.
        work = self.work("parcel-zoning")
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        result, calls = self.run_operator("parcel-zoning", "latency", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, _ = self.unit(calls)
        self.assertEqual(args[-1], "probe-parcel-zoning-by-pnu-section-pack-latency")
        preview = CONTRACT["parcel_by_pnu_gateway"]["section_packs"]["preview_worker"]["public_hostname"]
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_ZONING_BY_PNU_SERVING_PACK_PREVIEW_BASE_URL"], f"https://{preview}")

    def test_every_kind_lanes_monitor_sample_is_a_line_of_the_kinds_one_file(self):
        # Root ADR-0193: a kind line needs no secrets group of its own; each monitor-sample replaces
        # its lane's line and keeps the others'.
        shared = self.etc / "by-pnu-kind-serving-monitor.env"
        for lane in ("parcel-zoning", "parcel-price", "parcel-zoning"):
            work = self.work(lane)
            (work / "equality.json").write_text(json.dumps({"passed": True}))
            result, calls = self.run_operator(lane, "monitor-sample", "1")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn({"tool": "systemctl", "args": ["enable", "--now", f"foundation-by-pnu-serving-monitor@{lane}.timer"]},
                          calls)
            self.log.unlink()
        self.assertEqual(sorted(shared.read_text().splitlines()), [
            f"FOUNDATION_PLATFORM_PARCEL_PRICE_BY_PNU_SERVING_MONITOR_SAMPLE_PATH={self.work('parcel-price')}/equality.json",
            f"FOUNDATION_PLATFORM_PARCEL_ZONING_BY_PNU_SERVING_MONITOR_SAMPLE_PATH={self.work('parcel-zoning')}/equality.json",
        ])
        self.assertFalse((self.etc / "parcel-zoning-serving-monitor.env").exists())

    def test_publish_needs_gate_ga_passed_and_gate_na_measured_and_pins_the_bake_snapshot(self):
        work = self.work()
        (work / "equality.json").write_text(json.dumps({"passed": False}))
        result, calls = self.run_operator("parcel", "publish", "1")
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertIn("equality.json did not pass", result.stderr)
        self.assertEqual(calls, [])
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        result, calls = self.run_operator("parcel", "publish", "1")
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertIn("latency.json does not exist", result.stderr)
        self.assertEqual(calls, [])
        # A refused gate (나) still reaches the publisher: it alone applies the contract's waivers
        # (root ADR-0162) and refuses the rest.
        (work / "latency.json").write_text(json.dumps({"passed": False}))
        result, calls = self.run_operator("parcel", "publish", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        args, env, _ = self.unit(calls)
        self.assertEqual(args[-1], "publish-parcel-by-pnu-section-packs")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "777")
        self.assertEqual(env["FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_PACK_PUBLISH"], "true")

    def test_anything_but_a_lane_an_action_and_an_id_is_refused_before_anything_runs(self):
        self.work()
        for args in (
            ("buildings", "latency", "1"),
            ("parcel", "latency", "1;id"),
            ("parcel", "latency", "../1"),
            ("parcel", "latency", "0"),
            ("parcel", "latency", "1", "2"),
            ("parcel", "shell", "1"),
            ("parcel", "bake"),
            ("parcel", "bake", "0"),
            ("parcel", "bake", "../1"),
            ("parcel", "bake", "1", "--force"),
            ("parcel", "gold-rebuild", "1"),
            ("parcel", "gold-rebuild", "because I said so"),
            ("parcel", "gold-rebuild", "--unconditional", "x"),
            ("parcel", "health", "not-a-version"),
            ("parcel", "health", VERSION, "$(id)"),
            ("parcel",),
        ):
            result, calls = self.run_operator(*args)
            self.assertEqual(result.returncode, 64, (args, result.stderr))
            self.assertEqual(calls, [], args)

    def test_health_hands_the_release_check_its_lane_and_versions(self):
        result, calls = self.run_operator("parcel", "health", VERSION, OLD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [{"tool": "health", "args": ["parcel", VERSION, OLD]}])

    def test_the_monitor_sample_names_this_generations_equality_evidence(self):
        work = self.work()
        result, _ = self.run_operator("parcel", "monitor-sample", "1")
        self.assertEqual(result.returncode, 65, result.stderr)
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        result, _ = self.run_operator("parcel", "monitor-sample", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.etc / "parcel-serving-monitor.env").read_text(),
            f"FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MONITOR_SAMPLE_PATH={work}/equality.json\n",
        )

    def test_a_pack_only_lane_is_watched_and_judged_after_its_first_publish(self):
        # Root ADR-0190: the monitor-sample step names the sample and switches the lane's hourly
        # monitor on; health hands the canary's verdict the lane like any other.
        work = self.work("unit-price")
        (work / "equality.json").write_text(json.dumps({"passed": True}))
        result, calls = self.run_operator("unit-price", "monitor-sample", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.etc / "unit-price-serving-monitor.env").read_text(),
            f"FOUNDATION_PLATFORM_UNIT_PRICE_BY_PNU_SERVING_MONITOR_SAMPLE_PATH={work}/equality.json\n",
        )
        self.assertIn({"tool": "systemctl", "args": ["enable", "--now", "foundation-by-pnu-serving-monitor@unit-price.timer"]},
                      calls)
        self.log.unlink()
        result, calls = self.run_operator("unit-price", "health", VERSION, OLD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [{"tool": "health", "args": ["unit-price", VERSION, OLD]}])
        # Nothing but its lane's steps.
        result, calls = self.run_operator("unit-price", "shell", "1")
        self.assertEqual(result.returncode, 64, result.stderr)


class MonitorReasons(unittest.TestCase):
    def test_each_lanes_monitor_reason_names_its_own_timer_instance_and_documents(self):
        # A copied reason told the operator the wrong timer and an object comparison a pack-only
        # lane (root ADR-0190) never makes.
        lanes = {"parcel": CONTRACT["parcel_by_pnu_gateway"], "building": CONTRACT["building_by_pnu_gateway"],
                 **CONTRACT.get("by_pnu_lanes", {})}
        for lane, block in lanes.items():
            if "host" in block:
                continue  # Its monitor block is its host's (root ADR-0193 §4); the host's case covers it.
            with self.subTest(lane=lane):
                reason = block["section_packs"]["monitor"]["reason"]
                self.assertIn(f"instance {lane}", reason)
                if block.get("pack_only"):
                    self.assertNotIn("compared with the object documents", reason)


if __name__ == "__main__":
    unittest.main()
