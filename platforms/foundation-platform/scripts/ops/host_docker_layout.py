#!/usr/bin/env python3
"""The configuration edits behind host-docker-data-root.sh (root ADR-0241), kept testable.

    host_docker_layout.py layout <field>                    a value of config/host-layout.contract.json
    host_docker_layout.py daemon-json <path> <data-root>    daemon.json with data-root and the build cache bound set, every other key kept
    host_docker_layout.py containerd-toml <path> <root>     config.toml with the top-level root set
    host_docker_layout.py unpaused-dags <airflow-json>      the foundation_ DAGs Airflow lists as unpaused

Each edit prints the whole new file and changes nothing on disk: the shell script writes it
atomically and keeps the old file to roll back to. A file that already says what it should is
printed unchanged, so a second run is a no-op.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys
import tomllib
from typing import Any

CONTRACT = pathlib.Path(__file__).resolve().parents[2] / "config" / "host-layout.contract.json"
DAG_PREFIX = "foundation_"


def load_layout(path: pathlib.Path = CONTRACT) -> dict[str, Any]:
    layout = json.loads(path.read_text(encoding="utf-8"))
    if layout.get("schema_version") != 1:
        raise ValueError(f"{path}: unsupported schema_version {layout.get('schema_version')!r}")
    mount = layout["data_mount"].rstrip("/") + "/"
    for name in (layout["docker"]["data_root"], layout["containerd"]["root"]):
        if not name.startswith(mount):
            raise ValueError(f"{name} is not on the data disk {layout['data_mount']}")
    return layout


def layout_field(layout: dict[str, Any], dotted: str) -> str:
    value: Any = layout
    for part in dotted.split("."):
        value = value[part]
    if isinstance(value, dict):
        return "\n".join(f"{key} {item}" for key, item in value.items())
    return str(value)


def merged_daemon_json(existing: str | None, data_root: str, builder_gc: dict[str, Any]) -> str:
    """daemon.json with `data-root` and the contract's `builder.gc` keys set; every other key and
    its value kept as it was."""
    config = {} if existing is None or not existing.strip() else json.loads(existing)
    if not isinstance(config, dict):
        raise ValueError("daemon.json is not a JSON object")
    if "graph" in config:
        # The pre-17.05 spelling of data-root: dockerd refuses both at once.
        raise ValueError("daemon.json sets the retired `graph` key; remove it by hand first")
    config["data-root"] = data_root
    builder = config.setdefault("builder", {})
    builder["gc"] = {**builder.get("gc", {}), **builder_gc}
    return json.dumps(config, indent=2) + "\n"


TOP_LEVEL_ROOT = re.compile(r'^root\s*=.*$', re.M)
TABLE_HEADER = re.compile(r'^\[', re.M)


def merged_containerd_toml(existing: str, root: str) -> str:
    """config.toml with the top-level `root` set; every other line kept.

    A top-level key must come before the first table header, so a missing `root` goes there. The
    result is parsed back and refused unless it says exactly this root and kept every other value.
    """
    before = tomllib.loads(existing)
    line = f'root = "{root}"'
    header = TABLE_HEADER.search(existing)
    top = existing if header is None else existing[: header.start()]
    if TOP_LEVEL_ROOT.search(top):
        text = TOP_LEVEL_ROOT.sub(line, top, count=1) + existing[len(top):]
    elif header is None:
        text = existing + ("" if existing.endswith("\n") or not existing else "\n") + line + "\n"
    else:
        text = existing[: header.start()] + line + "\n\n" + existing[header.start():]
    after = tomllib.loads(text)
    if after.get("root") != root or {k: v for k, v in after.items() if k != "root"} != {
        k: v for k, v in before.items() if k != "root"
    }:
        raise ValueError("the containerd config did not come out as the same config with this root")
    return text


def unpaused_dags(airflow_json: str) -> list[str]:
    """The foundation_ DAGs `airflow dags list -o json` lists as unpaused, sorted."""
    rows = json.loads(airflow_json)
    return sorted(
        row["dag_id"]
        for row in rows
        if row["dag_id"].startswith(DAG_PREFIX) and str(row["is_paused"]).lower() != "true"
    )


def main(argv: list[str]) -> int:
    command, *args = argv
    if command == "layout":
        print(layout_field(load_layout(), args[0]))
    elif command == "daemon-json":
        path = pathlib.Path(args[0])
        existing = path.read_text(encoding="utf-8") if path.exists() else None
        sys.stdout.write(merged_daemon_json(existing, args[1], load_layout()["docker"]["builder_gc"]))
    elif command == "containerd-toml":
        sys.stdout.write(merged_containerd_toml(pathlib.Path(args[0]).read_text(encoding="utf-8"), args[1]))
    elif command == "unpaused-dags":
        print("\n".join(unpaused_dags(pathlib.Path(args[0]).read_text(encoding="utf-8"))))
    else:
        print(f"host_docker_layout: unknown command {command!r}", file=sys.stderr)
        return 64
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
