from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("audit-documentation.py")
SPEC = importlib.util.spec_from_file_location("audit_documentation", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class AuditDocumentationTests(unittest.TestCase):
    def test_classifies_korean_prose_without_counting_code(self) -> None:
        text = """---
status: current
---

이 문서는 수집 파이프라인의 운영 절차를 설명합니다.

```python
def english_identifier():
    return \"not prose\"
```
"""
        self.assertEqual(MODULE.classify_language(text), "korean")

    def test_classifies_english_prose(self) -> None:
        text = """# Documentation

This page describes the deployment procedure and recovery steps.
"""
        self.assertEqual(MODULE.classify_language(text), "english")

    def test_reports_mixed_prose(self) -> None:
        text = """# 운영 가이드

This paragraph is intentionally English and is not a code identifier.
"""
        self.assertEqual(MODULE.classify_language(text), "mixed")

    def test_parses_required_frontmatter(self) -> None:
        text = """---
status: current
owner: foundation
doc_type: guide
last_reviewed: 2026-07-28
---

본문
"""
        self.assertEqual(
            MODULE.parse_frontmatter(text),
            {
                "status": "current",
                "owner": "foundation",
                "doc_type": "guide",
                "last_reviewed": "2026-07-28",
            },
        )

    def test_finds_duplicate_non_readme_basenames(self) -> None:
        paths = [
            Path("platforms/foundation-platform/docs/runbooks/deploy.md"),
            Path("platforms/identity-platform/docs/runbooks/deploy.md"),
            Path("platforms/foundation-platform/README.md"),
            Path("platforms/identity-platform/README.md"),
        ]
        self.assertEqual(
            MODULE.duplicate_basenames(paths),
            {"deploy.md": paths[:2]},
        )


if __name__ == "__main__":
    unittest.main()
