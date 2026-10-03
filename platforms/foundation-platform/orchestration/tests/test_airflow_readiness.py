"""The deployment wrapper must see every declared DAG before changing scheduler state."""
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]


class ParserReadiness(unittest.TestCase):
    def run_fixture(self, scenario):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "scripts/deploy").mkdir(parents=True)
            (root / "orchestration").mkdir()
            script = root / "scripts/deploy/airflow-runtime.sh"
            shutil.copyfile(PLATFORM / "scripts/deploy/airflow-runtime.sh", script)
            jobs = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text())
            (root / "orchestration/jobs.v1.json").write_text(json.dumps(jobs))
            expected = ["foundation_" + job["id"] for job in jobs["jobs"]]
            self.assertGreater(len(expected), 1)
            (root / "expected.json").write_text(json.dumps(expected))
            (root / "runtime.env").touch()
            (root / "oidc.env").touch()
            bin_dir = root / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
r=pathlib.Path(os.environ["FIXTURE_ROOT"]); a=sys.argv[1:]
if a[:2]==["network","inspect"]: sys.exit(0)
if "exec" not in a: sys.exit(0)
a=a[a.index("airflow-scheduler")+2:]
if a[:2]==["dags","list"]:
    count=r/"count"; n=int(count.read_text())+1 if count.exists() else 1; count.write_text(str(n))
    ids=json.loads((r/"expected.json").read_text()); mode=os.environ["FIXTURE_SCENARIO"]
    if mode=="cli_error": sys.exit(7)
    if mode=="malformed": print("not-json"); sys.exit(0)
    if mode=="wrong_shape": print(json.dumps({"dag_id":ids[0]})); sys.exit(0)
    if mode=="missing" or (mode=="eventual" and n==1): ids=ids[:-1]
    if "--output" in a: print(json.dumps([{"dag_id":i} for i in ids]))
    else: print("\\n".join(ids))
else:
    with (r/"mutations").open("a") as f: f.write(json.dumps(a)+"\\n")
''')
            curl = bin_dir / "curl"
            curl.write_text('#!/bin/sh\ncase "$*" in *redirect_url*) printf "http://127.0.0.1:18453/oauth/v2/authorize?fixture" ;; *) printf 200 ;; esac\n')
            sleep = bin_dir / "sleep"
            sleep.write_text("#!/bin/sh\nexit 0\n")
            for executable in [docker, curl, sleep]:
                executable.chmod(0o755)
            env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
                   "FIXTURE_ROOT": str(root), "FIXTURE_SCENARIO": scenario,
                   "AIRFLOW_RUNTIME_DIR": str(root), "AIRFLOW_OIDC_CLIENT_FILE": str(root / "oidc.env")}
            result = subprocess.run(["bash", str(script), "up", "-d"], env=env, capture_output=True, text=True, timeout=30)
            mutations = (root / "mutations").read_text() if (root / "mutations").exists() else ""
            count = int((root / "count").read_text())
            return result, mutations, count, expected

    def test_existing_dags_cannot_activate_a_partially_parsed_release(self):
        result, mutations, _, _ = self.run_fixture("missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(mutations, "")

    def test_all_declared_dags_are_required_before_pool_or_activation(self):
        result, mutations, count, expected = self.run_fixture("eventual")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreaterEqual(count, 2)
        actions = [json.loads(line) for line in mutations.splitlines()]
        # Every pool jobs.v1.json declares, with its slots and description, before any DAG changes.
        pools = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["pools"]
        self.assertEqual(actions[:len(pools)], [
            ["pools", "set", name, str(pool["slots"]), pool["description"]] for name, pool in pools.items()])
        self.assertEqual({action[2] for action in actions[len(pools):]}, set(expected))

    def test_invalid_cli_results_never_mutate_scheduler_state(self):
        for scenario in ["cli_error", "malformed", "wrong_shape"]:
            with self.subTest(scenario=scenario):
                result, mutations, _, _ = self.run_fixture(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(mutations, "")


if __name__ == "__main__":
    unittest.main()
