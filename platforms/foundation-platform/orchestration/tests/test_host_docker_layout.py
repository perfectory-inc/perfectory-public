"""The configuration edits that move Docker and containerd to the data disk (root ADR-0241).

host-docker-data-root.sh writes what scripts/ops/host_docker_layout.py prints, so these are the
edits the host gets. A daemon.json key lost or a containerd setting dropped would change how the
engine runs after the move without anyone having decided it.
"""

import json
import pathlib
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


GC = {"enabled": True, "defaultKeepStorage": "20GB"}
WITH_GC = {"builder": {"gc": GC}}


def merged(existing, data_root="/data/docker", gc=GC):
    return layout.merged_daemon_json(existing, data_root, gc)


class DaemonJson(unittest.TestCase):
    def test_every_existing_key_is_kept_and_data_root_is_set(self):
        existing = json.dumps({
            "log-driver": "json-file", "log-opts": {"max-size": "50m", "max-file": "3"},
            "features": {"containerd-snapshotter": True}, "default-address-pools": [{"base": "192.0.2.0/24", "size": 28}],
        })
        result = json.loads(merged(existing))
        self.assertEqual(result.pop("data-root"), "/data/docker")
        self.assertEqual(result.pop("builder"), {"gc": GC})
        self.assertEqual(result, json.loads(existing))

    def test_a_host_without_daemon_json_gets_data_root_and_the_cache_bound(self):
        self.assertEqual(json.loads(merged(None)), {"data-root": "/data/docker", **WITH_GC})
        self.assertEqual(json.loads(merged("")), {"data-root": "/data/docker", **WITH_GC})

    def test_a_second_run_changes_nothing(self):
        once = merged(json.dumps({"log-driver": "local"}))
        self.assertEqual(merged(once), once)

    def test_an_existing_data_root_is_replaced_not_duplicated(self):
        self.assertEqual(json.loads(merged(json.dumps({"data-root": "/srv/docker"}))), {"data-root": "/data/docker", **WITH_GC})

    def test_the_cache_bound_overrides_its_keys_and_keeps_other_builder_settings(self):
        existing = json.dumps({"builder": {"gc": {"enabled": False, "policy": []}, "features": {"x": True}}})
        result = json.loads(merged(existing))["builder"]
        self.assertEqual(result, {"gc": {"enabled": True, "defaultKeepStorage": "20GB", "policy": []}, "features": {"x": True}})

    def test_the_rendered_file_carries_the_contracts_bound(self):
        """What host-docker-data-root.sh writes: the CLI, reading the shipped contract."""
        bound = layout.load_layout()["docker"]["builder_gc"]
        self.assertIs(bound["enabled"], True)
        self.assertRegex(bound["defaultKeepStorage"], r"^\d+GB$")
        with tempfile.TemporaryDirectory() as directory:
            daemon = pathlib.Path(directory) / "daemon.json"
            daemon.write_text(json.dumps({"log-driver": "local"}), encoding="utf-8")
            out = subprocess.run([sys.executable, "-I", str(PLATFORM / "scripts/ops/host_docker_layout.py"),
                                  "daemon-json", str(daemon), "/data/docker"], capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(out.stdout)["builder"]["gc"], bound)

    def test_the_retired_graph_key_is_refused(self):
        """dockerd refuses `graph` and `data-root` together, so the engine would not start."""
        with self.assertRaises(ValueError):
            merged(json.dumps({"graph": "/srv/docker"}))


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

    def test_the_script_parses_and_refuses_an_unknown_mode(self):
        script = PLATFORM / "scripts/ops/host-docker-data-root.sh"
        self.assertEqual(subprocess.run(["bash", "-n", script.as_posix()]).returncode, 0)
        refused = subprocess.run(["bash", script.as_posix(), "move"], capture_output=True, text=True)
        self.assertEqual(refused.returncode, 64)


if __name__ == "__main__":
    unittest.main()
