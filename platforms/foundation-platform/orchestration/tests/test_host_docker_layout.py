"""The configuration edits that move Docker and containerd to the data disk (root ADR-0241).

host-docker-data-root.sh writes what scripts/ops/host_docker_layout.py prints, so these are the
edits the host gets. A daemon.json key lost or a containerd setting dropped would change how the
engine runs after the move without anyone having decided it.
"""

import json
import pathlib
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

import host_docker_layout as layout  # noqa: E402

# The shape of the host's file on 2026-10-14 (Docker's packaged default).
HOST_CONTAINERD = '''#   Copyright 2018-2022 Docker Inc.

disabled_plugins = ["cri"]

#root = "/var/lib/containerd"
#state = "/run/containerd"

#[grpc]
#  address = "/run/containerd/containerd.sock"
'''


class DaemonJson(unittest.TestCase):
    def test_every_existing_key_is_kept_and_data_root_is_set(self):
        existing = json.dumps({
            "log-driver": "json-file", "log-opts": {"max-size": "50m", "max-file": "3"},
            "features": {"containerd-snapshotter": True}, "default-address-pools": [{"base": "192.0.2.0/24", "size": 28}],
        })
        merged = json.loads(layout.merged_daemon_json(existing, "/data/docker"))
        self.assertEqual(merged.pop("data-root"), "/data/docker")
        self.assertEqual(merged, json.loads(existing))

    def test_a_host_without_daemon_json_gets_only_data_root(self):
        self.assertEqual(json.loads(layout.merged_daemon_json(None, "/data/docker")), {"data-root": "/data/docker"})
        self.assertEqual(json.loads(layout.merged_daemon_json("", "/data/docker")), {"data-root": "/data/docker"})

    def test_a_second_run_changes_nothing(self):
        once = layout.merged_daemon_json(json.dumps({"log-driver": "local"}), "/data/docker")
        self.assertEqual(layout.merged_daemon_json(once, "/data/docker"), once)

    def test_an_existing_data_root_is_replaced_not_duplicated(self):
        merged = json.loads(layout.merged_daemon_json(json.dumps({"data-root": "/srv/docker"}), "/data/docker"))
        self.assertEqual(merged, {"data-root": "/data/docker"})

    def test_the_retired_graph_key_is_refused(self):
        """dockerd refuses `graph` and `data-root` together, so the engine would not start."""
        with self.assertRaises(ValueError):
            layout.merged_daemon_json(json.dumps({"graph": "/srv/docker"}), "/data/docker")


class ContainerdToml(unittest.TestCase):
    def test_the_hosts_file_gets_root_and_keeps_everything_else(self):
        merged = layout.merged_containerd_toml(HOST_CONTAINERD, "/data/containerd")
        parsed = tomllib.loads(merged)
        self.assertEqual(parsed, {"disabled_plugins": ["cri"], "root": "/data/containerd"})
        # The comments stay: the file still documents its defaults.
        for line in HOST_CONTAINERD.splitlines():
            self.assertIn(line, merged.splitlines())

    def test_root_goes_before_the_first_table(self):
        """A key after a table header belongs to that table: containerd would ignore it."""
        existing = 'version = 2\n\n[plugins."io.containerd.grpc.v1.cri"]\n  sandbox_image = "pause:3.9"\n'
        parsed = tomllib.loads(layout.merged_containerd_toml(existing, "/data/containerd"))
        self.assertEqual(parsed["root"], "/data/containerd")
        self.assertEqual(parsed["plugins"]["io.containerd.grpc.v1.cri"], {"sandbox_image": "pause:3.9"})
        self.assertEqual(parsed["version"], 2)

    def test_an_existing_root_is_replaced_and_a_second_run_changes_nothing(self):
        once = layout.merged_containerd_toml('root = "/var/lib/containerd"\nstate = "/run/containerd"\n', "/data/containerd")
        self.assertEqual(tomllib.loads(once), {"root": "/data/containerd", "state": "/run/containerd"})
        self.assertEqual(layout.merged_containerd_toml(once, "/data/containerd"), once)

    def test_a_root_inside_a_table_is_not_taken_for_the_top_level_one(self):
        existing = '[plugins."io.containerd.snapshotter.v1.overlayfs"]\n  root_path = ""\n'
        parsed = tomllib.loads(layout.merged_containerd_toml(existing, "/data/containerd"))
        self.assertEqual(parsed["root"], "/data/containerd")
        self.assertEqual(parsed["plugins"]["io.containerd.snapshotter.v1.overlayfs"], {"root_path": ""})


class Dags(unittest.TestCase):
    def test_only_unpaused_foundation_dags_are_restored(self):
        listed = json.dumps([
            {"dag_id": "foundation_source_sweep", "is_paused": "False"},
            {"dag_id": "foundation_vworld_parcel_edition", "is_paused": "True"},
            {"dag_id": "foundation_data_quality", "is_paused": False},
            {"dag_id": "example_bash_operator", "is_paused": "False"},
        ])
        self.assertEqual(layout.unpaused_dags(listed), ["foundation_data_quality", "foundation_source_sweep"])


class TheContract(unittest.TestCase):
    def test_both_roots_are_on_the_data_disk(self):
        shipped = layout.load_layout()
        mount = shipped["data_mount"].rstrip("/") + "/"
        self.assertTrue(shipped["docker"]["data_root"].startswith(mount))
        self.assertTrue(shipped["containerd"]["root"].startswith(mount))

    def test_a_root_off_the_data_disk_is_refused(self):
        shipped = layout.load_layout()
        shipped["containerd"]["root"] = "/var/lib/containerd"
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "layout.json"
            path.write_text(json.dumps(shipped), encoding="utf-8")
            with self.assertRaises(ValueError):
                layout.load_layout(path)

    def test_a_spark_tmp_off_the_data_disk_is_refused(self):
        shipped = layout.load_layout()
        shipped["spark_tmp"] = "/var/tmp/spark"
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "layout.json"
            path.write_text(json.dumps(shipped), encoding="utf-8")
            with self.assertRaises(ValueError):
                layout.load_layout(path)

    def test_the_script_parses_and_refuses_an_unknown_mode(self):
        script = PLATFORM / "scripts/ops/host-docker-data-root.sh"
        self.assertEqual(subprocess.run(["bash", "-n", script.as_posix()]).returncode, 0)
        refused = subprocess.run(["bash", script.as_posix(), "move"], capture_output=True, text=True)
        self.assertEqual(refused.returncode, 64)


def compose_services(text: str) -> dict[str, str]:
    """Each top-level service of a compose file and its body, comments dropped."""
    services = {}
    body = text.split("\nservices:\n", 1)[1] + "\nend:\n"
    for match in re.finditer(r"(?ms)^  ([a-z][a-z0-9-]*):\n(.*?)(?=^\S|^  \S)", body):
        services[match.group(1)] = "\n".join(
            line for line in match.group(2).splitlines() if not line.lstrip().startswith("#"))
    return services


class SparkTmpOnTheDataDisk(unittest.TestCase):
    """Spark's spill never lands in a container layer on the system disk (2026-10-15 ENOSPC)."""

    def mount(self):
        return "${FOUNDATION_PLATFORM_SPARK_TMP:-" + layout.load_layout()["spark_tmp"] + "}:/tmp"

    def spark_services_without_it(self, text):
        return sorted(name for name, body in compose_services(text).items()
                      if "image: apache/spark:" in body and "- " + self.mount() + "\n" not in body + "\n")

    def test_every_spark_container_and_its_check_mount_the_contract_path_at_tmp(self):
        text = (PLATFORM / "compose.lakehouse.yml").read_text(encoding="utf-8")
        services = compose_services(text)
        self.assertLessEqual({"spark", "spark-small"}, set(services))
        self.assertEqual(self.spark_services_without_it(text), [])
        self.assertIn("- " + self.mount() + "\n", services["lakehouse-target-init"] + "\n")
        self.assertIn("test -w /tmp", services["lakehouse-target-init"], "the init check proves /tmp is writable")

    def test_a_spark_service_without_it_is_found(self):
        text = (PLATFORM / "compose.lakehouse.yml").read_text(encoding="utf-8")
        planted = text.replace("      - " + self.mount() + "\n", "")
        self.assertEqual(self.spark_services_without_it(planted), ["spark", "spark-small"])

    def test_the_release_creates_it_from_the_contract(self):
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertRegex(release, r'install -d -m 1777 "\$\(python3 -I -c .*\["spark_tmp"\]')

    def migrate_makes_it_before_the_unit_starts(self, release):
        body = re.search(r"^migrate_lakehouse\(\) \{\n(.*?)^\}", release, re.M | re.S).group(1)
        made = body.find("prepare_spark_tmp")
        return made != -1 and made < body.find("systemctl start foundation-lakehouse-migrate.service")

    def test_the_migration_has_it_before_its_init_check_runs(self):
        # 2026-10-15: made only in `timers`, after migrate; Docker made it root 0755 and every deploy stopped.
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertTrue(self.migrate_makes_it_before_the_unit_starts(release))
        planted = release.replace("  prepare_spark_tmp\n  systemctl daemon-reload\n", "  systemctl daemon-reload\n")
        self.assertNotEqual(planted, release)
        self.assertFalse(self.migrate_makes_it_before_the_unit_starts(planted))


if __name__ == "__main__":
    unittest.main()
