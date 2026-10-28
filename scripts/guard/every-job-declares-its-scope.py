#!/usr/bin/env python3
"""Every scheduled job says what one run of it processes, and `full` only shrinks (root ADR-0314).

What failing this prevents: the owner's rule that every run redoes only what changed was written
down area by area, so each new job started from nothing. The 2026-10-28 audit measured the leak:
a 0.12% change (48,477 PNUs) rewrote all 39,861,511 rows (4.27 GB) of gold.parcel_panel, eleven of
twelve Silver lanes rewrote whole tables, the 공매 Gold was always overwritten, and the legal-dong
collector downloaded 55 MB about ten times a day while the table stood still. None of those jobs
had to say so anywhere.

Each job in platforms/foundation-platform/orchestration/jobs.v1.json declares `scope`, one of
`changed-keys`, `new-objects`, `skip-if-unchanged` or `full` (the job list's `scope_means`). A
`full` job also states `full_reason`, `full_cost` and `full_tracked_by`, and is listed in
tools/ci/full-scope-jobs.baseline.json. The list is the ratchet of
scripts/catalog/documentation_ratchets.py: a full job it does not list fails, a listed job that is
no longer full is reported so the change removes it, and `--shrink` rewrites it to the jobs that
are still full; it cannot add one.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOBS = Path("platforms/foundation-platform/orchestration/jobs.v1.json")
BASELINE = Path("tools/ci/full-scope-jobs.baseline.json")
SCOPES = ("changed-keys", "new-objects", "skip-if-unchanged", "full")
FULL_FIELDS = ("full_reason", "full_cost", "full_tracked_by")
NAME = "every-job-declares-its-scope"


def problems_and_full(jobs: dict) -> tuple[list[str], set[str]]:
    """(what is wrong with the declarations, the ids of the jobs declared full)."""
    problems, full = [], set()
    for job in jobs.get("jobs", []):
        job_id = job.get("id", "<no id>")
        scope = job.get("scope")
        if scope not in SCOPES:
            problems.append(f"{job_id}: scope {scope!r} is not one of {', '.join(SCOPES)}")
            continue
        stated = [field for field in FULL_FIELDS if isinstance(job.get(field), str) and job[field].strip()]
        if scope == "full":
            full.add(job_id)
            problems += [f"{job_id}: a full job states {field}" for field in FULL_FIELDS if field not in stated]
        elif stated or any(field in job for field in FULL_FIELDS):
            problems.append(f"{job_id}: scope {scope} carries {', '.join(f for f in FULL_FIELDS if f in job)}, "
                            "which only a full job states")
    return problems, full


def load_baseline(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    return set(json.loads(path.read_text(encoding="utf-8")).get("jobs", []))


def write_baseline(path: Path, jobs: set[str]) -> None:
    data = {"$comment": "Shrink-only (scripts/guard/every-job-declares-its-scope.py, root ADR-0314). "
                        "A job that becomes partial leaves this list; never add one.",
            "jobs": sorted(jobs)}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", nargs="?", type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument("--shrink", action="store_true", help="drop baseline jobs that are no longer full")
    args = parser.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    jobs_path, baseline_path = args.root / JOBS, args.root / BASELINE
    if not jobs_path.is_file():
        print(f"FAIL {NAME}: missing {jobs_path}", file=sys.stderr)
        return 1
    problems, full = problems_and_full(json.loads(jobs_path.read_text(encoding="utf-8")))
    baseline = load_baseline(baseline_path)
    if args.shrink:
        write_baseline(baseline_path, baseline & full)
        return 0
    problems += [f"{job_id}: declared full, and {BASELINE} does not list it; make the job partial "
                 "(root ADR-0314) rather than adding it" for job_id in sorted(full - baseline)]
    for job_id in sorted(baseline - full):
        print(f"warning: {NAME}: {job_id} is no longer full; run "
              f"python3 scripts/guard/{NAME}.py --shrink", file=sys.stderr)
    if problems:
        print(f"FAIL {NAME}:", file=sys.stderr)
        for problem in problems:
            print(f"    {problem}", file=sys.stderr)
        print("    root ADR-0314; the jobs' scope_means says what each scope means", file=sys.stderr)
        return 1
    print(f"OK {NAME} (jobs={len(json.loads(jobs_path.read_text(encoding='utf-8'))['jobs'])}, full={len(full)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
