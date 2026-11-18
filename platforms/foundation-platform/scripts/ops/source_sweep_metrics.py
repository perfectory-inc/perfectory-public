#!/usr/bin/env python3
"""The files the provider lists and Bronze still lacks after a sweep, for Prometheus (root ADR-0514).

    source_sweep_metrics.py <vworld evidence> <vworld inventory> <raon summary> <metrics file>

Writes, for node-exporter's textfile collector, per source:

    foundation_source_sweep_unfetched_files{source}                       listed, not in Bronze after this run
    foundation_source_sweep_unfetched_bytes{source}                       their listed size
    foundation_source_sweep_unfetched_oldest_listed_timestamp_seconds{source}
                                                                          the oldest one's provider update date
                                                                          (00:00 UTC); 0 when it lists none

and foundation_source_sweep_metrics_timestamp_seconds. A file not in Bronze is a RAON selection archive
the VWorld lane deferred (status deferred_selection_archive) that the large-file lane did not commit
this run. 2026-10-09..11-11 the large-file lane was switched off and 49 such files waited, among them
경기·경북 of every 용도지역지구 month since 2026-07: silver.land_use_plan stayed on 2026-06-09 and the
parcel-zoning lane served it with no alert, because the lane served everything Silver had (root
ADR-0444) and the source's other files kept its Bronze fresh (ADR-0443). An unknown date is the oldest:
not knowing is no reason to be quiet (as ADR-0444 §2 falls back to the manifest's age). With no evidence
(the VWorld lane died before it wrote one) it writes only the timestamp, so the unit's own failure is
the signal and the last measured files do not go stale silently.
"""

from __future__ import annotations

import datetime
import json
import os
import sys
import time

DEFERRED = "deferred_selection_archive"


def load(path: str):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return None


def listed_date(value) -> float:
    try:
        day = datetime.date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return 0.0
    return datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp()


def unfetched(evidence: dict, inventory: dict, raon: dict | None) -> dict:
    """{source: [(listed bytes, listed timestamp)]} of the deferred archives no RAON run committed."""
    listed = {(job.get("source_slug"), f.get("download_ds_id"), f.get("file_no")): f
              for job in inventory.get("jobs", []) for f in job.get("files", [])}
    committed = {(f.get("source_slug"), f.get("provider_file_id"))
                 for f in (raon or {}).get("files", []) if f.get("status") == "committed"}
    waiting: dict[str, list] = {}
    for archive in evidence.get("selection_archives") or []:
        if archive.get("status") != DEFERRED:
            continue
        source, ds_id, file_no = archive.get("source_slug"), archive.get("download_ds_id"), archive.get("file_no")
        if (source, f"{ds_id}-{file_no}") in committed:
            continue
        item = listed.get((source, ds_id, file_no)) or {}
        size = archive.get("size_bytes") or (item.get("size_kib") or 0) * 1024
        waiting.setdefault(source, []).append((size, listed_date(item.get("updated_at"))))
    return waiting


def series(waiting: dict, now: float) -> list[str]:
    lines = [
        "# HELP foundation_source_sweep_unfetched_files Files the provider lists that Bronze lacks after the sweep (root ADR-0514).",
        "# TYPE foundation_source_sweep_unfetched_files gauge",
        "# HELP foundation_source_sweep_unfetched_bytes The provider's listed size of those files.",
        "# TYPE foundation_source_sweep_unfetched_bytes gauge",
        "# HELP foundation_source_sweep_unfetched_oldest_listed_timestamp_seconds The oldest such file's provider update date; 0 when it lists none.",
        "# TYPE foundation_source_sweep_unfetched_oldest_listed_timestamp_seconds gauge",
    ]
    for source in sorted(waiting):
        files = waiting[source]
        labels = f'source="{source}"'
        lines += [
            f"foundation_source_sweep_unfetched_files{{{labels}}} {len(files)}",
            f"foundation_source_sweep_unfetched_bytes{{{labels}}} {sum(size for size, _ in files)}",
            f"foundation_source_sweep_unfetched_oldest_listed_timestamp_seconds{{{labels}}} "
            f"{min(at for _, at in files):.0f}",
        ]
    lines += [
        "# HELP foundation_source_sweep_metrics_timestamp_seconds When the sweep last wrote these series.",
        "# TYPE foundation_source_sweep_metrics_timestamp_seconds gauge",
        f"foundation_source_sweep_metrics_timestamp_seconds {now:.0f}",
    ]
    return lines


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        print("usage: source_sweep_metrics.py <vworld evidence> <vworld inventory> <raon summary> <metrics file>",
              file=sys.stderr)
        return 64
    evidence_path, inventory_path, raon_path, metrics_path = argv[1:]
    evidence = load(evidence_path)
    waiting = {} if evidence is None else unfetched(evidence, load(inventory_path) or {}, load(raon_path))
    text = "\n".join(series(waiting, time.time())) + "\n"
    # Written whole beside the file and renamed, so the collector never reads half of it.
    os.makedirs(os.path.dirname(metrics_path) or ".", exist_ok=True)
    partial = metrics_path + ".tmp"
    with open(partial, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(partial, metrics_path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
