#!/usr/bin/env python3
"""The release build's measurements as a Markdown table for the job summary (root ADR-0276 §9)."""

import json
import sys
from pathlib import Path


def mib(value):
    return "" if value is None else f"{value / 1048576:,.0f}"


def main(out: Path) -> int:
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    measured = json.loads((out / "measurements.json").read_text(encoding="utf-8"))
    print(f"### Release {manifest['source_commit']}\n")
    print("| build | seconds | peak memory (MiB) | blob MiB |\n|---|---:|---:|---:|")
    for name, entry in measured.items():
        if name != "total":
            print(f"| {name} | {entry.get('seconds', '')} | {mib(entry.get('peak_memory_bytes'))} | {mib(entry.get('blob_bytes'))} |")
    print(f"\n{measured['total']['blobs']} blobs, {mib(measured['total']['blob_bytes'])} MiB in all.\n")
    print("| file | MiB |\n|---|---:|")
    for name, entry in manifest["files"].items():
        print(f"| {name} | {mib(entry['size'])} |")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
