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

    def test_ignores_technical_identifiers_when_classifying_korean_prose(self) -> None:
        text = """# 운영 가이드

`PostGIS`와 `Iceberg` snapshot을 R2에 저장하고 API로 제공합니다.
"""
        self.assertEqual(MODULE.classify_language(text), "korean")

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

    def test_treats_adr_fields_as_metadata_exemption(self) -> None:
        self.assertEqual(
            MODULE.metadata_status(
                Path("docs/adr/0001-example.md"),
                "# ADR\n\n| Status | Accepted |\n",
            ),
            "not applicable: ADR fields",
        )

    def test_adr_directory_outranks_evidence_keyword_in_filename(self) -> None:
        catalog = MODULE.load_catalog_module()
        path = Path(
            "docs/adr/0030-parcel-publication-evidence-requires-two-distinct-approvals.md"
        )

        self.assertEqual(catalog.type_for(path), "ADR")
        self.assertEqual(catalog.status_for(path), "current")

    def test_treats_machine_contracts_as_metadata_exemption(self) -> None:
        self.assertEqual(
            MODULE.metadata_status(Path("docs/catalog/contract.v1.json"), "{}"),
            "not applicable: machine contract",
        )

    def test_treats_legal_text_as_metadata_exemption(self) -> None:
        self.assertEqual(
            MODULE.metadata_status(Path("THIRD_PARTY_NOTICES.md"), "legal text"),
            "not applicable: legal text",
        )

    def test_treats_draft_documents_as_metadata_exemption(self) -> None:
        self.assertEqual(
            MODULE.metadata_status(Path("docs/catalog/rules.v1.draft.md"), "# Draft"),
            "not applicable: draft",
        )

    def test_ignores_status_code_fields_when_auditing_metadata(self) -> None:
        self.assertEqual(
            MODULE.metadata_status(
                Path("docs/catalog/proposed.md"),
                "---\nstatus: proposed\nowner: foundation\ndoc_type: reference\nlast_reviewed: 2026-07-30\n---\n\n```yaml\nstatus\n```\n",
            ),
            "ok",
        )

    def test_flags_maintained_english_only_documents(self) -> None:
        rows = [
            {"path": Path("docs/guide.md"), "language": "english", "metadata": "ok"},
            {"path": Path("docs/contract.json"), "language": "english", "metadata": "not applicable: machine contract"},
            {"path": Path("docs/mixed.md"), "language": "mixed", "metadata": "ok"},
        ]
        self.assertEqual(
            MODULE.human_language_violations(rows),
            [rows[0]],
        )

    def test_current_root_readme_has_no_english_narrative_sentence(self) -> None:
        rows = [{"path": Path("README.md"), "metadata": "ok"}]
        self.assertEqual(MODULE.english_sentence_violations(rows), [])

    def test_flags_english_bullet_paragraph_without_fixed_prefix(self) -> None:
        path = Path("docs/english-bullet.md")
        path.write_text(
            "# 문서\n\n- Product services consume the published contract and never write owner data.\n",
            encoding="utf-8",
        )
        try:
            rows = [{"path": path, "metadata": "ok"}]
            violations = MODULE.english_sentence_violations(rows)
            self.assertEqual(violations[0][0], path)
        finally:
            path.unlink()

    def test_flags_unreferenced_proposed_or_draft_documents(self) -> None:
        proposed = {"path": Path("docs/proposed.md"), "status": "proposed", "inbound": 0}
        draft = {"path": Path("docs/rules.v1.draft.md"), "status": "review required", "inbound": 0}
        referenced = {"path": Path("docs/used.md"), "status": "proposed", "inbound": 1}
        self.assertEqual(
            MODULE.review_reference_violations([proposed, draft, referenced]),
            [proposed, draft],
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

    def test_classifies_scoped_duplicates_as_intentional(self) -> None:
        paths = [
            Path("docs/glossary.md"),
            Path("products/gongzzang/docs/glossary.md"),
        ]
        self.assertEqual(MODULE.duplicate_basenames(paths), {})
        self.assertEqual(
            MODULE.intentional_duplicate_basenames(paths),
            {"glossary.md": paths},
        )

    def test_finds_broken_local_links(self) -> None:
        source = Path("docs/example.md")
        self.assertEqual(
            MODULE.broken_local_links(
                source,
                "[ok](../README.md) [bad](./missing.md) [external](https://example.com)",
            ),
            ["./missing.md"],
        )


if __name__ == "__main__":
    unittest.main()


class MetadataStrictFailureTests(unittest.TestCase):
    """`--strict` must honour the same exemptions the report shows.

    The report has three outcomes: compliant, exempt by an audit rule, missing. Collapsing that to
    "ok or not ok" made 135 machine contracts, legal texts, drafts, ADRs and agent routers look
    like violations, which is why the ratchet could not be turned on.
    """

    def test_ok_passes(self) -> None:
        self.assertFalse(MODULE.metadata_is_failure("ok"))

    def test_every_exemption_reason_passes(self) -> None:
        for reason in (
            "not applicable: machine contract",
            "not applicable: legal text",
            "not applicable: draft",
            "not applicable: ADR fields",
            "not applicable: agent router",
        ):
            with self.subTest(reason=reason):
                self.assertFalse(MODULE.metadata_is_failure(reason))

    def test_missing_metadata_fails(self) -> None:
        self.assertTrue(
            MODULE.metadata_is_failure("missing: status, owner, doc_type, last_reviewed")
        )
        self.assertTrue(MODULE.metadata_is_failure("missing: last_reviewed"))

    def test_an_unrecognised_status_fails_rather_than_passing_silently(self) -> None:
        self.assertTrue(MODULE.metadata_is_failure("unknown"))
        self.assertTrue(MODULE.metadata_is_failure(""))
