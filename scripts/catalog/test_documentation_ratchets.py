"""Each documentation ratchet rejects what it claims to, on a planted git tree."""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import documentation_ratchets as ratchets  # noqa: E402

TODAY = dt.date(2026, 10, 28)


def setUpModule() -> None:
    # A hook or rebase binds git to the real repository through these; the fixtures must not
    # inherit that (scripts/guard/lib/fixture-repo.sh has the incident).
    for name in [n for n in os.environ if n.startswith("GIT_")]:
        del os.environ[name]


class Fixture:
    def __init__(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.root = Path(self._dir.name)
        self.write("tools/areas.contract.json", json.dumps({"areas": [{"dir": "platforms/foundation-platform"}]}))
        self.write("scripts/real.sh", "")
        self.write("platforms/foundation-platform/scripts/ops/area.sh", "")
        self.write("docs/adr/0001-a.md", "# ADR 0001: a\n\n- Status: Accepted\n")

    def write(self, path: str, text: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def check(self, baseline: dict | None = None) -> tuple[list[str], list[str]]:
        subprocess.run(["git", "-C", str(self.root), "init", "-q"], check=True)
        subprocess.run(["git", "-C", str(self.root), "add", "-A"], check=True)
        path = self.root / "baseline.json"
        path.write_text(json.dumps(baseline or {}), encoding="utf-8")
        return ratchets.check(self.root, path, TODAY)

    def close(self) -> None:
        self._dir.cleanup()


class RatchetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = Fixture()
        self.addCleanup(self.fx.close)

    def test_dead_reference_fails_unless_baselined(self) -> None:
        self.fx.write(
            "docs/guide.md",
            "`scripts/real.sh` `scripts/ops/area.sh` `scripts/gone.sh` `gold/manifest.json`\n",
        )
        errors, _ = self.fx.check()
        self.assertEqual(errors, ["dead-reference: scripts/gone.sh named by docs/guide.md"])
        errors, _ = self.fx.check({"dead-reference": ["scripts/gone.sh"]})
        self.assertEqual(errors, [])

    def test_adr_dead_reference_is_a_warning_not_a_ratchet(self) -> None:
        self.fx.write("docs/adr/0002-b.md", "# ADR 0002: b\n\n- Status: Accepted\n\n`scripts/guard/never-written.sh`\n")
        self.fx.write("docs/adr/0003-c.md", "# ADR 0003: c\n\n- Status: Accepted\n\n- `scripts/old.sh` → `scripts/real.sh`\n")
        errors, notes = self.fx.check()
        self.assertEqual(errors, [])
        self.assertEqual(len([n for n in notes if "never-written.sh" in n]), 1)
        self.assertFalse([n for n in notes if "old.sh" in n])

    def test_fixed_baseline_entry_is_reported_not_failed(self) -> None:
        errors, notes = self.fx.check({"dead-reference": ["scripts/gone.sh"]})
        self.assertEqual(errors, [])
        self.assertTrue(any("delete it from the baseline" in n for n in notes))

    def test_runbook_missing_from_its_readme(self) -> None:
        self.fx.write("ops/runbooks/README.md", "- [a](./a.md)\n")
        self.fx.write("ops/runbooks/a.md", "a\n")
        self.fx.write("ops/runbooks/b.md", "b\n")
        errors, _ = self.fx.check()
        self.assertEqual(errors, ["unindexed-runbook: ops/runbooks/b.md"])

    def test_proposed_adr_cited_by_code_needs_a_dated_missing_note(self) -> None:
        self.fx.write("docs/adr/0002-b.md", "# ADR 0002: b\n\n- Status: Proposed\n")
        self.fx.write("docs/adr/0003-c.md", "# ADR 0003: c\n\n- Status: Proposed\n\n2026-10-28: 구현 점검 — Proposed 유지. 단계 ②가 없다.\n")
        self.fx.write("docs/adr/0004-d.md", "# ADR 0004: d\n\n- Status: Proposed\n")
        self.fx.write("src/lib.rs", "// root ADR-0002, ADR 0003, FP-ADR-0004\n")
        errors, _ = self.fx.check()
        self.assertEqual(errors, ["proposed-adr-cited-by-code: docs/adr/0002-b.md"])

    def test_supersession_must_be_recorded_on_the_older_adr(self) -> None:
        self.fx.write("docs/adr/0002-b.md", "# ADR 0002: b\n\n- Status: Accepted\n- Supersedes: ADR-0001\n")
        errors, _ = self.fx.check()
        self.assertEqual(errors, ["supersession-unrecorded: ADR-0002 supersedes ADR-0001"])
        self.fx.write("docs/adr/0001-a.md", "# ADR 0001: a\n\n- Status: Superseded by ADR-0002\n")
        errors, _ = self.fx.check()
        self.assertEqual(errors, [])

    def test_stale_runbook_warns(self) -> None:
        meta = "---\nstatus: current\nowner: x\ndoc_type: {kind}\nlast_reviewed: 2026-07-01\n---\n"
        self.fx.write("ops/runbooks/README.md", "- [a](./a.md)\n")
        self.fx.write("ops/runbooks/a.md", meta.format(kind="runbook"))
        self.fx.write("docs/guide.md", meta.format(kind="guide"))
        errors, notes = self.fx.check()
        self.assertEqual(errors, [])
        self.assertEqual([n for n in notes if "days old" in n], [n for n in notes if "ops/runbooks/a.md" in n])
        self.assertEqual(len([n for n in notes if "days old" in n]), 1)

    def test_shrink_never_adds(self) -> None:
        self.fx.write("docs/guide.md", "`scripts/gone.sh`\n")
        self.fx.check()
        path = self.fx.root / "baseline.json"
        path.write_text(json.dumps({"dead-reference": ["scripts/x.sh"]}), encoding="utf-8")
        ratchets.main(["--root", str(self.fx.root), "--baseline", str(path), "--shrink"])
        written = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(written["dead-reference"], [])


class RealTreeTests(unittest.TestCase):
    def test_the_repository_passes(self) -> None:
        errors, _ = ratchets.check(ratchets.ROOT, ratchets.BASELINE, TODAY)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
