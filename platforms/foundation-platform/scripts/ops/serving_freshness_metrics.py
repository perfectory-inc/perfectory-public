#!/usr/bin/env python3
"""A by-PNU serving monitor report as node-exporter textfile series (root ADR-0306).

    serving_freshness_metrics.py <monitor report JSON> <lane> <metrics directory>

Writes <directory>/by-pnu-serving-<lane>.prom: the served manifest's age, the freshness budget it is
held to, and when it was measured, for FoundationByPnuServingStale and
FoundationByPnuServingMonitorSilent (infra/observability/prometheus/foundation-api.rules.yml). A run
not held to the budget (a generation or recorded state), or one that wrote no report, leaves the
file as it was. 2026-10-28..11-05 the 공매 lane served a pre-photo manifest for 191 hours and the
only signal was a failed unit among some thirty open notices.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def series(report: dict, lane: str, now: float) -> list[str] | None:
    """The textfile lines of `report`, or None when the run was not held to the budget."""
    if report.get("freshness_budget_hours") is None:
        return None
    labels = f'lane="{lane}"'
    return [
        "# HELP foundation_by_pnu_served_manifest_age_seconds How long ago the manifest the lane serves was published (root ADR-0306).",
        "# TYPE foundation_by_pnu_served_manifest_age_seconds gauge",
        f"foundation_by_pnu_served_manifest_age_seconds{{{labels}}} {float(report['served_age_hours']) * 3600:.0f}",
        "# HELP foundation_by_pnu_serving_freshness_budget_seconds The age past which the lane's served manifest is stale.",
        "# TYPE foundation_by_pnu_serving_freshness_budget_seconds gauge",
        f"foundation_by_pnu_serving_freshness_budget_seconds{{{labels}}} {float(report['freshness_budget_hours']) * 3600:.0f}",
        "# HELP foundation_by_pnu_serving_monitor_timestamp_seconds When the lane's serving monitor last measured it.",
        "# TYPE foundation_by_pnu_serving_monitor_timestamp_seconds gauge",
        f"foundation_by_pnu_serving_monitor_timestamp_seconds{{{labels}}} {now:.0f}",
    ]


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[2].strip(), file=sys.stderr)
        return 64
    report, lane, directory = Path(argv[0]), argv[1], Path(argv[2])
    if not report.exists() or report.stat().st_size == 0:
        return 0
    lines = series(json.loads(report.read_text(encoding="utf-8")), lane, time.time())
    if lines is None:
        return 0
    target = directory / f"by-pnu-serving-{lane}.prom"
    temporary = directory / f".{target.name}.{os.getpid()}"
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.replace(target)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
