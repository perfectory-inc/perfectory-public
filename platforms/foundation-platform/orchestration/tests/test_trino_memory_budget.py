"""One query cannot take Trino past its container cap (root ADR-0504).

2026-11-11 01:25Z: an unsampled holdout query ran on the one Trino node; the kernel killed the
process at 16.55GB anon RSS inside the 16g cap (memory cgroup OOM), with the heap ceiling at 80%
(12.8GiB) and query.max-memory 8GB. Trino's memory accounting sees only the heap, so its own limits
never fired: the rest was outside the heap. Measured the same day on the restarted, idle Trino:
RSS 3.99GB with 0.98GB of heap committed, 2.5GB of it in 164 glibc malloc arenas.
"""

from __future__ import annotations

import json
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
TRINO = PLATFORM / "infra/lakehouse/trino"
COMPOSE = PLATFORM / "compose.lakehouse.yml"
LAYOUT = PLATFORM / "config/host-layout.contract.json"

GIB = 2**30
# Outside the heap at the kill: anon RSS minus the whole heap ceiling (docker logs, journalctl -k).
INCIDENT_NATIVE_BYTES = 16_550_188 * 1024 - 13_757_317_120
# Idle Trino after the restart: RSS minus committed heap (/proc/1/status, jcmd GC.heap_info).
IDLE_NATIVE_BYTES = (3_991_568 - 983_040) * 1024


def size(value: str) -> int:
    number, unit = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMGT]?)B?", value.strip(), re.I).groups()
    return int(float(number) * 1024 ** "KMGT".find(unit.upper()) * 1024) if unit else int(number)


def properties(name: str) -> dict[str, str]:
    rows = {}
    for line in (TRINO / name).read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            rows[key.strip()] = value.strip()
    return rows


def trino_service() -> str:
    text = COMPOSE.read_text(encoding="utf-8")
    start = text.index("\n  trino:\n")
    following = re.search(r"\n  [a-z-]+:\n", text[start + 1:])
    return text[start: start + 1 + following.start()]


def cap_bytes() -> int:
    return size(re.search(r"mem_limit: (\S+)", trino_service()).group(1).upper())


def heap_bytes() -> int:
    jvm = (TRINO / "jvm.config").read_text(encoding="utf-8")
    return int(cap_bytes() * int(re.search(r"-XX:MaxRAMPercentage=(\d+)", jvm).group(1)) / 100)


class TheHeapLeavesRoomForWhatTrinoCannotSee(unittest.TestCase):
    def test_the_cap_leaves_half_again_the_incidents_native_memory_outside_the_heap(self):
        self.assertGreaterEqual(INCIDENT_NATIVE_BYTES, 2.9 * GIB, "the incident's own numbers")
        self.assertGreaterEqual(cap_bytes() - heap_bytes(), 1.5 * max(INCIDENT_NATIVE_BYTES, IDLE_NATIVE_BYTES))

    def test_the_incident_heap_would_be_refused(self):
        self.assertLess(cap_bytes() * 0.20, 1.5 * INCIDENT_NATIVE_BYTES, "80% of the cap, as on 2026-11-11")

    def test_glibc_arenas_are_capped(self):
        # 164 arenas held 2.5GB idle; Hadoop caps them at 4 for the same reason (HADOOP-7154).
        self.assertRegex(trino_service(), r'\n      MALLOC_ARENA_MAX: "[1-4]"\n')

    def test_a_query_and_the_untracked_heap_fit_in_the_heap(self):
        # Trino's docs: query.max-memory-per-node + memory.heap-headroom-per-node < the heap.
        config = properties("config.properties")
        per_node = size(config["query.max-memory-per-node"])
        headroom = size(config["memory.heap-headroom-per-node"])
        self.assertLess(per_node + headroom, heap_bytes())
        self.assertLessEqual(size(config["query.max-memory"]), per_node, "one node: the cluster is the node")
        self.assertLessEqual(size(config["query.max-total-memory"]), heap_bytes() - headroom)


class AnAdHocQueryRunsAloneUnderLowerCaps(unittest.TestCase):
    def groups(self) -> dict[str, dict]:
        found = {}

        def walk(group: dict, prefix: str) -> None:
            name = f"{prefix}.{group['name']}" if prefix else group["name"]
            found[name] = group
            for child in group.get("subGroups", []):
                walk(child, name)

        for root in json.loads((TRINO / "resource-groups.json").read_text(encoding="utf-8"))["rootGroups"]:
            walk(root, "")
        return found

    def test_the_trino_cli_lands_in_one_slot(self):
        config = json.loads((TRINO / "resource-groups.json").read_text(encoding="utf-8"))
        cli = next(s for s in config["selectors"] if s.get("source") == "trino-cli")
        self.assertEqual(self.groups()[cli["group"]]["hardConcurrencyLimit"], 1)
        self.assertNotIn("source", config["selectors"][-1], "every other query has a group")

    def test_its_session_caps_are_under_the_nodes(self):
        rules = json.loads((TRINO / "session-property-config.json").read_text(encoding="utf-8"))
        adhoc = next(r for r in rules if r["group"] == "global.adhoc")["sessionProperties"]
        config = properties("config.properties")
        self.assertLess(size(adhoc["query_max_memory"]), size(config["query.max-memory-per-node"]))
        self.assertIn("query_max_execution_time", adhoc)

    def test_the_managers_read_the_mounted_files(self):
        self.assertEqual(properties("resource-groups.properties")["resource-groups.config-file"],
                         "/etc/trino/resource-groups.json")
        self.assertEqual(properties("session-property-config.properties")["session-property-manager.config-file"],
                         "/etc/trino/session-property-config.json")


class EveryFileFollowsTheRelease(unittest.TestCase):
    def test_every_config_file_is_mounted_where_trino_reads_it(self):
        # foundation-release.sh trino compares these files' bytes with the running container's.
        service = trino_service()
        for path in sorted([*TRINO.glob("*.properties"), *TRINO.glob("*.json"), TRINO / "jvm.config"]):
            with self.subTest(path.name):
                self.assertIn(f"- ./infra/lakehouse/trino/{path.name}:/etc/trino/{path.name}:ro\n", service)

    def test_spill_is_on_the_contracts_data_disk_path(self):
        layout = json.loads(LAYOUT.read_text(encoding="utf-8"))
        self.assertTrue(layout["trino_spill"].startswith(layout["data_mount"] + "/"))
        self.assertIn("- ${FOUNDATION_PLATFORM_TRINO_SPILL:-" + layout["trino_spill"] + "}:/spill\n", trino_service())
        self.assertEqual(properties("config.properties")["spiller-spill-path"], "/spill")

    def test_the_deploy_runs_the_trino_step_after_the_catalog(self):
        deploy = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        self.assertLess(deploy.index('"${release}" datahub'), deploy.index('"${release}" trino'))
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertIn('source "${release_root}/current/scripts/deploy/trino-follows-the-release.sh"\n    apply_trino\n',
                      release)
        step = (PLATFORM / "scripts/deploy/trino-follows-the-release.sh").read_text(encoding="utf-8")
        self.assertIn("apply_trino() {", step)


if __name__ == "__main__":
    unittest.main()
