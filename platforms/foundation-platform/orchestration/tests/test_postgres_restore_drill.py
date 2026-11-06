"""The weekly production restore drill (scripts/recovery/run-postgres-restore-drill.sh) against a fake docker.

Root ADR-0300 K: the PostgreSQL backup is restored every week, as the identity dumps are. The fake
stands in for the live database, the repository and the restored copy; each case breaks one thing a
real restore can get wrong and the drill must fail on it. The incident: the nightly backup did not
run at all from 2026-07-26 to 10-02 and nothing said so, and no timer has ever restored production.
"""

import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/recovery/run-postgres-restore-drill.sh"

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
sys.stdout.reconfigure(newline="\n")
args = sys.argv[1:]
state = json.load(open(os.environ["FAKE_STATE"]))
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if args[:1] == ["ps"]:
    print("live1")
elif args[:1] == ["info"]:
    print(os.environ["FAKE_DOCKER_ROOT"])
elif args[:1] == ["inspect"]:
    print("true" if state["restored_runs"] else "false")
elif args[:1] == ["logs"]:
    print("FATAL: the fake copy stopped")
elif args[:1] == ["exec"]:
    side = state["live"] if args[1] == "live1" else state["restored"]
    sql = args[-1]
    if sql.startswith("SELECT EXISTS (SELECT FROM "):
        print("t" if sql[len("SELECT EXISTS (SELECT FROM "):-1] in side["nonempty"] else "f")
    elif "pg_is_in_recovery" in sql:
        print("t" if state["promotes"] else "f")
    elif "server_version_num" in sql:
        print(side["version"])
    elif "information_schema.tables" in sql:
        print(side["tables"])
    elif "_sqlx_migrations" in sql:
        print(side["migrations"])
    elif "pg_stat_user_tables" in sql:
        print("\n".join(side["nonempty"]))
    elif "pg_last_xact_replay_timestamp" in sql:
        print("2026-11-08T05:20:00Z")
    else:
        sys.exit("fake docker: unexpected SQL " + sql)
elif args[:1] == ["compose"]:
    if "info" in args and "--output=json" in args:
        print(json.dumps([{"backup": [
            {"label": "20261101-021500F", "timestamp": {"stop": 1}, "info": {"size": 10}},
            {"label": "20261108-021500F", "timestamp": {"stop": 2}, "info": {"size": state["backup_bytes"]}}]}]))
else:
    sys.exit("fake docker: unexpected call " + " ".join(args))
'''

LIVE = {"version": "170005", "tables": 86, "migrations": 69,
        "nonempty": ["catalog.dataset", "public._sqlx_migrations"]}


class PostgresRestoreDrill(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="postgres-restore-drill-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        if os.name == "nt":  # a laptop run: Windows Python ends its lines with CR LF, the host's does not
            real = pathlib.Path(shutil.which("python")).as_posix()
            (bin_dir / "python3").write_text(f'#!/usr/bin/env bash\nset -o pipefail\n"{real}" "$@" | tr -d "\\r"\n')
        self.log = self.root / "calls.jsonl"
        self.evidence = self.root / "evidence"
        self.state = {"live": dict(LIVE), "restored": json.loads(json.dumps(LIVE)), "backup_bytes": 1024,
                      "promotes": True, "restored_runs": True}
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                    "FAKE_LOG": str(self.log), "FAKE_STATE": str(self.root / "state.json"),
                    "FAKE_DOCKER_ROOT": str(self.root),
                    "FOUNDATION_RECOVERY_EVIDENCE_DIR": str(self.evidence),
                    "FOUNDATION_RESTORE_DRILL_RECOVERY_WAIT_SECONDS": "5"}

    def drill(self):
        (self.root / "state.json").write_text(json.dumps(self.state))
        run = subprocess.run([shutil.which("bash") or "bash", str(SCRIPT)], env=self.env,
                             capture_output=True, text=True, timeout=120)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return run, calls

    def assert_cleaned_up(self, calls):
        self.assertIn(["down", "--volumes", "--remove-orphans"], [c[-3:] for c in calls if c[:1] == ["compose"]])

    def test_a_restore_that_matches_the_live_database_passes_and_leaves_evidence(self):
        run, calls = self.drill()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("postgres-restore-drill-ok backup=20261108-021500F", run.stdout)
        evidence = json.loads((self.evidence / "postgres-restore-drill.json").read_text())
        self.assertEqual((evidence["result"], evidence["tables"], evidence["nonempty_tables_checked"]),
                         ("pass", 86, 2))
        restore = [c for c in calls if "pgbackrest-restore" in c]
        self.assertEqual(restore[0][-3:], ["pgbackrest-restore", "restore", "--archive-mode=off"])
        # The copy archives nothing and listens on no network.
        started = next(c for c in calls if "-d" in c and "foundation-restored-postgres" in c)
        self.assertEqual(started[-5:], ["postgres", "-c", "archive_mode=off", "-c", "listen_addresses="])
        # Nothing ever writes into the repository it proves.
        self.assertFalse([c for c in calls if "foundation-backup" in c or "backup" in c[-2:]])
        self.assert_cleaned_up(calls)

    def test_a_table_with_rows_in_production_that_the_restore_lost_fails_it(self):
        self.state["restored"]["nonempty"] = ["public._sqlx_migrations"]
        run, calls = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("empty in the restore: catalog.dataset", run.stderr)
        self.assertEqual(json.loads((self.evidence / "postgres-restore-drill.json").read_text())["result"], "fail")
        self.assert_cleaned_up(calls)

    def test_missing_tables_or_migrations_fail_it(self):
        self.state["restored"].update(tables=85, migrations=68)
        run, _ = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("tables: restored 85, live 86", run.stderr)
        self.assertIn("migrations: restored 68, live 69", run.stderr)

    def test_another_server_version_fails_it(self):
        self.state["restored"]["version"] = "160004"
        run, _ = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("version: restored 160004, live 170005", run.stderr)

    def test_a_copy_that_stops_before_recovery_ends_fails_with_its_log(self):
        self.state.update(promotes=False, restored_runs=False)
        run, calls = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("did not finish recovery", run.stderr)
        self.assertIn("FATAL: the fake copy stopped", run.stderr)
        self.assert_cleaned_up(calls)

    def test_a_set_the_disk_cannot_hold_is_refused_before_it_is_restored(self):
        self.state["backup_bytes"] = 1 << 60
        run, calls = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("needs", run.stderr)
        self.assertFalse([c for c in calls if "pgbackrest-restore" in c])
        self.assert_cleaned_up(calls)


if __name__ == "__main__":
    unittest.main()
