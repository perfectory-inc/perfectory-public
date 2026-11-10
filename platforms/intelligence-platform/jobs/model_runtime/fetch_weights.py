#!/usr/bin/env python3
"""Downloads a runtime's weights from config/model-runtime.contract.json and verifies every byte (root ADR-0478).

Each file is fetched from the pinned revision of its Hugging Face repository (never `main`), resumed
when a partial file is there, and kept only when its size and SHA-256 equal the contract's. The
target is the contract's model_root, and it must sit on the declared device class: the weights are
read again for every token, so they never go to the HDD (/data) and, for `nvme`, not to a SATA disk
either. `--check` downloads nothing and only verifies what is there.

    python3 fetch_weights.py strata_qwen38_flash_next [--root DIR] [--check]

Standard library only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import sys
import urllib.request

CONTRACT = pathlib.Path(__file__).resolve().parents[2] / "config/model-runtime.contract.json"
HUB = "https://huggingface.co"
CHUNK = 8 << 20


class Refused(Exception):
    pass


def block_devices(path: pathlib.Path, sys_root: pathlib.Path = pathlib.Path("/sys")) -> list[pathlib.Path]:
    """The physical block devices under the filesystem holding `path` (through LVM, RAID, partitions)."""
    device = os.stat(path).st_dev
    start = (sys_root / "dev/block" / f"{os.major(device)}:{os.minor(device)}").resolve()
    found, pending = [], [start]
    while pending:
        node = pending.pop()
        slaves = sorted((node / "slaves").iterdir()) if (node / "slaves").is_dir() else []
        if slaves:
            pending.extend(slave.resolve() for slave in slaves)
        elif (node / "partition").is_file():
            pending.append(node.parent)
        else:
            found.append(node)
    return found


def device_class(path: pathlib.Path, sys_root: pathlib.Path = pathlib.Path("/sys")) -> str:
    """nvme when every device under `path` is NVMe, ssd when none rotates, else hdd."""
    devices = block_devices(path, sys_root)
    if not devices:
        raise Refused(f"cannot tell which disk holds {path}")
    if any((device / "queue/rotational").read_text().strip() != "0" for device in devices):
        return "hdd"
    return "nvme" if all(device.name.startswith("nvme") for device in devices) else "ssd"


def sha256_of(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(CHUNK):
            digest.update(block)
    return digest.hexdigest()


def download(url: str, target: pathlib.Path, size: int) -> None:
    have = target.stat().st_size if target.exists() else 0
    if have > size:
        target.unlink()
        have = 0
    if have == size:
        return
    request = urllib.request.Request(url, headers={"Range": f"bytes={have}-"} if have else {})
    with urllib.request.urlopen(request, timeout=120) as response, target.open("ab" if have else "wb") as out:
        if have and response.status != 206:
            raise Refused(f"{url} ignored the range request (status {response.status})")
        shutil.copyfileobj(response, out, CHUNK)


def run(runtime: str, root: pathlib.Path | None, check: bool, contract: dict, sys_root: pathlib.Path,
        fetch=download) -> list[str]:
    entry = contract["runtimes"][runtime]
    weights = entry.get("weights")
    if not weights:
        raise Refused(f"{runtime} declares no weights to fetch")
    model_root = contract["model_root"]
    root = root or pathlib.Path(model_root["path"])
    root.mkdir(parents=True, exist_ok=True)
    wanted = model_root["device_class"]
    found = device_class(root, sys_root)
    if found != wanted:
        raise Refused(f"{root} is on a {found} disk; the contract asks for {wanted} (root ADR-0478)")
    files = weights["files"]
    need = sum(item["size"] for item in files) + model_root["free_bytes_margin"]
    have = sum((root / runtime / item["path"]).stat().st_size for item in files if (root / runtime / item["path"]).exists())
    if not check and shutil.disk_usage(root).free + have < need:
        raise Refused(f"{root} has {shutil.disk_usage(root).free} bytes free; the weights and margin need {need - have} more")
    done = []
    for item in files:
        target = root / runtime / item["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        if not check:
            fetch(f"{HUB}/{weights['repository']}/resolve/{weights['revision']}/{item['path']}", target, item["size"])
        if not target.is_file() or target.stat().st_size != item["size"]:
            raise Refused(f"{target}: size is not {item['size']}")
        if sha256_of(target) != item["sha256"]:
            raise Refused(f"{target}: SHA-256 is not {item['sha256']}; remove it and fetch again")
        done.append(str(target))
    return done


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("runtime")
    parser.add_argument("--root", type=pathlib.Path)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args(argv)
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    try:
        for path in run(arguments.runtime, arguments.root, arguments.check, contract, pathlib.Path("/sys")):
            print(f"verified {path}")
    except (Refused, KeyError, OSError) as error:
        print(f"fetch_weights: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
