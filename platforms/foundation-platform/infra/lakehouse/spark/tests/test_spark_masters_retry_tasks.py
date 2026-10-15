"""Every Spark master a job runs with lets a failed task run again (root ADR-0102).

Plain `local[N]` ignores `spark.task.maxFailures`, so one reset R2 read aborts the whole job: that
stopped silver.parcel_boundaries's compaction on 2026-10-15, after the same lesson had been learned
once per program. This scans every place a master is written for a job — contracts, scripts, Rust
that builds a spark-submit, Python — and refuses one without the engine contract's
`r2_read.task_max_failures`. Test sessions in this directory read no R2 and are not scanned.
"""

from __future__ import annotations

import re
import sys
import tempfile
import unittest
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS))

import lakehouse_engine  # noqa: E402

PLATFORM = Path(__file__).resolve().parents[4]
SCANNED_SUFFIXES = {".sh", ".rs", ".json", ".py"}
# A concrete master: threads are a number, `*` or a shell variable; prose such as "plain local[N]"
# names no thread count and is not a master.
MASTER = re.compile(r"local\[(\d+|\*|\$\{?[A-Za-z_][A-Za-z0-9_]*\}?)(?:,\s*(\d+))?\]")
COMMENT = re.compile(r"^\s*(#|//)")


def excluded(path: Path, root: Path) -> bool:
    relative = path.relative_to(root).as_posix()
    return (relative.startswith("infra/lakehouse/spark/tests/") or "/target/" in relative
            or "/node_modules/" in relative)


def offenders(root: Path, failures: int) -> list[str]:
    found = []
    for path in sorted(root.rglob("*")):
        if path.suffix not in SCANNED_SUFFIXES or not path.is_file() or excluded(path, root):
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if COMMENT.match(line):
                continue
            for match in MASTER.finditer(line):
                if match.group(2) != str(failures):
                    found.append(f"{path.relative_to(root).as_posix()}:{number}: {match.group(0)}")
    return found


class EveryMasterRetriesTasks(unittest.TestCase):
    def setUp(self):
        self.failures = lakehouse_engine.r2_read_policy()["task_max_failures"]

    def test_no_job_runs_on_a_master_without_task_retries(self):
        self.assertEqual(offenders(PLATFORM, self.failures), [])

    def test_the_scan_finds_each_form_it_refuses(self):
        planted = {
            "scripts/ops/planted.sh": "spark-submit --master 'local[4]' job.py\n",
            "scripts/load/planted.sh": 'spark-submit --master "local[$TASKS]" job.py\n',
            "crates/x/src/planted.rs": 'let master = "local[*]";\n',
            "infra/lakehouse/contracts/planted.json": '{"spark": {"master": "local[8,1]"}}\n',
        }
        allowed = {
            "scripts/ops/fine.sh": f"spark-submit --master 'local[4,{self.failures}]' job.py\n# local[4] in a comment\n",
            "crates/x/src/prose.rs": 'let why = "plain local[N] ignores maxFailures";\n',
            "infra/lakehouse/spark/tests/test_planted.py": 'SparkSession.builder.master("local[2]")\n',
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative, text in {**planted, **allowed}.items():
                (root / relative).parent.mkdir(parents=True, exist_ok=True)
                (root / relative).write_text(text, encoding="utf-8")
            found = offenders(root, self.failures)
        self.assertEqual(sorted(entry.split(":", 1)[0] for entry in found), sorted(planted))


if __name__ == "__main__":
    unittest.main()
