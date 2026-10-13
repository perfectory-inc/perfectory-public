"""The production restore drill for the PostGIS parcel boundary tables
(scripts/recovery/parcel-boundary-restore-drill.sh, root ADR-0194 §6, §8).

A fake `docker` stands in for both PostgreSQL instances and records every call. The drill must
take the expected counts from the production ledgers alone, restore with `--archive-mode=off`
and start the side instance without archiving or a TCP listener, compare the restored counts
with the ledgers, remove the file that carried the repository settings, and refuse a work
directory off the data disk.
"""

import json
import os
import pathlib
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "recovery" / "parcel-boundary-restore-drill.sh"
RUN = "00000000-0000-7000-8000-000000000001"
LOAD = "00000000-0000-4000-8000-000000000002"

FAKE_DOCKER = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >> "${FAKE_LOG}"
case "$1" in
  inspect)
    case "$*" in
      *Config.Image*) echo "postgres-image@sha256:0" ;;
      *Mounts*) echo "/opt/fake/pgbackrest.conf" ;;
      *Config.Env*) printf 'PGBACKREST_REPO1_S3_KEY=synthetic\nPOSTGRES_USER=admin\n' ;;
    esac ;;
  exec)
    sql="$(cat)"
    if [[ "$3" == "${FAKE_PRODUCTION}" ]]; then
      case "${sql}" in
        *parcel_boundary_mirror_rebuild_run*) echo "${FAKE_RUN},${FAKE_LEDGER_MIRROR}" ;;
        *spatial_projection_load*) echo "${FAKE_LOAD},${FAKE_LEDGER_LOAD}" ;;
        *pg_settings*) echo "-c max_connections=100" ;;
        *parcel_boundary_mirror\ *|*parcel_boundary_publication\ *) echo "touched-a-dropped-table" >> "${FAKE_LOG}"; exit 1 ;;
      esac
    else
      case "${sql}" in
        *pg_is_in_recovery*) echo f ;;
        *parcel_boundary_mirror\ WHERE*) echo "${FAKE_RESTORED_MIRROR}" ;;
        *parcel_boundary_publication\ WHERE*) echo "${FAKE_RESTORED_LOAD}" ;;
        *pg_database_size*) echo "130 GB" ;;
      esac
    fi ;;
  run)
    for argument in "$@"; do
      [[ "${argument}" == --env-file ]] && take=1 && continue
      [[ -n "${take:-}" ]] && { cp "${argument}" "${FAKE_SEEN_ENV}"; take=""; }
    done ;;
esac
exit 0
'''


class RestoreDrillTest(unittest.TestCase):
    def drill(self, restored_mirror="39861511", restored_load="39861511", work=None):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text(FAKE_DOCKER, encoding="utf-8")
            docker.chmod(0o755)
            data = root / "data"
            data.mkdir()
            log = root / "docker.log"
            env = {
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_LOG": str(log),
                "FAKE_PRODUCTION": "foundation-platform-runtime-postgres-1",
                "FAKE_RUN": RUN, "FAKE_LOAD": LOAD,
                "FAKE_LEDGER_MIRROR": "39861511", "FAKE_LEDGER_LOAD": "39861511",
                "FAKE_RESTORED_MIRROR": restored_mirror, "FAKE_RESTORED_LOAD": restored_load,
                "FAKE_SEEN_ENV": str(root / "seen.env"),
                "FOUNDATION_RESTORE_DRILL_DATA_ROOT": str(data),
            }
            target = work if work is not None else str(data / "drill")
            done = subprocess.run(["bash", str(SCRIPT), target], env=env, capture_output=True, text=True, timeout=60)
            calls = log.read_text(encoding="utf-8") if log.exists() else ""
            seen = (root / "seen.env").read_text(encoding="utf-8") if (root / "seen.env").exists() else ""
            leftover = pathlib.Path(target).exists()
            return done, calls, seen, leftover

    def test_a_restore_that_holds_every_ledgered_row_passes(self):
        done, calls, seen, leftover = self.drill()
        self.assertEqual(done.returncode, 0, done.stderr)
        verdict = json.loads(done.stdout.strip().splitlines()[-1])
        self.assertTrue(verdict["equal"])
        self.assertEqual(verdict["mirror"], {"rebuild_run_id": RUN, "ledger_rows": 39861511, "restored_rows": 39861511})
        self.assertEqual(verdict["publication"]["projection_load_id"], LOAD)
        self.assertFalse(leftover, "the work directory and the settings file are removed")
        self.assertIn("PGBACKREST_REPO1_S3_KEY", seen)
        self.assertNotIn("POSTGRES_USER", seen, "only the repository settings go to the restore")

    def test_the_restore_never_archives_into_the_production_repository(self):
        _, calls, _, _ = self.drill()
        restore = next(line for line in calls.splitlines() if line.endswith(" restore"))
        self.assertIn("--archive-mode=off", restore)
        start = next(line for line in calls.splitlines() if line.startswith("run -d "))
        self.assertIn("-c archive_mode=off", start)
        self.assertIn("-c listen_addresses=", start)
        self.assertIn("-c max_connections=100", start)

    def test_production_is_asked_only_for_its_ledgers(self):
        _, calls, _, _ = self.drill()
        self.assertNotIn("touched-a-dropped-table", calls)

    def test_a_restore_missing_rows_fails(self):
        done, _, _, _ = self.drill(restored_load="39861510")
        self.assertEqual(done.returncode, 1)
        self.assertFalse(json.loads(done.stdout.strip().splitlines()[-1])["equal"])

    def test_a_work_directory_off_the_data_disk_is_refused(self):
        done, calls, _, _ = self.drill(work="/tmp/restore-drill")
        self.assertEqual(done.returncode, 64)
        self.assertEqual(calls, "")


if __name__ == "__main__":
    unittest.main()
