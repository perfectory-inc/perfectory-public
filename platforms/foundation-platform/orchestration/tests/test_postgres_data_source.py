"""Where the runtime Postgres keeps its data directory (root ADR-0598).

2026-11-20: the data directory lived in a Docker volume on the spinning data disk beside Spark, OCR and
docker; I/O pressure full avg300 47.5%, the staging database step took 17 s to over 300 s for the same
work, and the API's first connection did not open within 500 ms. The deploy moves it to the system
disk's SSD once (scripts/deploy/postgres-data-move.sh). These hold the parts that must agree: every
mount of the data directory takes the same variable, every compose caller of the runtime project
decides it the same way and from the marker only, and a restore drill never does.
"""

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
LAYOUT = PLATFORM / "config/host-layout.contract.json"
SOURCE = PLATFORM / "scripts/deploy/postgres-data-source.sh"
MOVE = PLATFORM / "scripts/deploy/postgres-data-move.sh"
RELEASE = PLATFORM / "scripts/deploy/foundation-release.sh"
MOUNT = "${FOUNDATION_PLATFORM_POSTGRES_DATA:-pgdata}:/var/lib/postgresql/data"


def read(path):
    return (PLATFORM / path).read_text(encoding="utf-8")


class EveryMountTakesTheSameSource(unittest.TestCase):
    def test_every_mount_of_the_data_directory_names_the_variable(self):
        for name in ("docker-compose.yml", "compose.recovery.yml"):
            mounts = re.findall(r"^\s*- (\S+):/var/lib/postgresql/data(:ro)?$", read(name), flags=re.MULTILINE)
            runtime = [source for source, _ in mounts if "pgdata" in source]
            self.assertTrue(runtime, name)
            # A restore drill's own volume (recovery_data) is not the runtime's.
            for source in runtime:
                self.assertEqual(f"{source}:/var/lib/postgresql/data", MOUNT, name)

    def test_the_runtime_callers_decide_it_and_the_drills_never_do(self):
        for caller in ("scripts/deploy/foundation-runtime.sh", "scripts/recovery/run-postgres-backup.sh"):
            text = read(caller)
            self.assertIn("scripts/deploy/postgres-data-source.sh", text, caller)
            self.assertIn('postgres_data_source "${root_dir}"', text, caller)
            self.assertLess(text.index("postgres_data_source \"${root_dir}\""), text.index("\"${compose[@]}\""), caller)
        for drill in ("scripts/recovery/postgres-restore-drill.sh", "scripts/recovery/run-postgres-restore-drill.sh"):
            text = read(drill)
            self.assertIn("unset FOUNDATION_PLATFORM_POSTGRES_DATA", text, drill)
            self.assertNotIn("postgres-data-source.sh", text, drill)

    def test_the_move_runs_before_the_chain_and_a_failed_one_stops_the_deploy(self):
        text = RELEASE.read_text(encoding="utf-8")
        self.assertLess(text.index('move_postgres_data "${runtime}" "${release_root}/current/config/host-layout.contract.json" \\\n'
                                   '    || exit "${schema_not_applied}"'),
                        text.index('runtime_chain_before "${runtime}" foundation-api'))

    def test_the_layout_keeps_the_marker_outside_the_data_directory(self):
        entry = json.loads(LAYOUT.read_text(encoding="utf-8"))["postgres_data"]
        data, marker = pathlib.PurePosixPath(entry["path"]), pathlib.PurePosixPath(entry["moved_marker"])
        self.assertNotIn(data, marker.parents, "the marker must be readable by every compose caller")
        self.assertFalse(str(data).startswith("/data/"), "the move is to the system disk, off the data disk")
        self.assertTrue(entry["volume"].endswith("_pgdata"))

    def test_a_requested_full_backup_is_taken_and_the_request_cleared_only_after_it(self):
        text = read("scripts/recovery/run-postgres-backup.sh")
        self.assertIn('-e "${evidence_dir}/full-backup-requested"', text)
        cleared = text.index('rm -f "${evidence_dir}/full-backup-requested"')
        self.assertGreater(cleared, text.index('pgbackrest-backup backup "--type=${backup_type}"'))
        self.assertIn("full-backup-requested", MOVE.read_text(encoding="utf-8"))

    def test_postgres_times_its_reads_writes_and_wal(self):
        recovery = read("compose.recovery.yml")
        for setting in ("track_io_timing=on", "track_wal_io_timing=on"):
            self.assertIn(setting, recovery)


@unittest.skipUnless(shutil.which("bash") and os.name == "posix", "bash on Linux")
class TheMarkerAloneDecides(unittest.TestCase):
    def source_with(self, marker_exists):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root / "config").mkdir()
            data, marker = root / "pg/data", root / "pg/moved"
            data.mkdir(parents=True)
            (root / "config/host-layout.contract.json").write_text(json.dumps(
                {"postgres_data": {"path": str(data), "moved_marker": str(marker), "volume": "x_pgdata"}}))
            if marker_exists:
                marker.write_text("2026-11-21T00:00:00Z\n")
            result = subprocess.run(
                ["bash", "-c", f'source "{SOURCE}"; FOUNDATION_PLATFORM_POSTGRES_DATA=stale; '
                               f'postgres_data_source "{root}"; printf "%s" "${{FOUNDATION_PLATFORM_POSTGRES_DATA-unset}}"'],
                capture_output=True, text=True, check=True)
            return result.stdout, str(data)

    def test_before_the_move_compose_mounts_the_volume(self):
        found, _ = self.source_with(marker_exists=False)
        self.assertEqual(found, "unset")

    def test_after_the_move_compose_binds_the_copy(self):
        found, data = self.source_with(marker_exists=True)
        self.assertEqual(found, data)


if __name__ == "__main__":
    unittest.main()
