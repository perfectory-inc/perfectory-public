import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


RAON_RUNBOOK = REPO_ROOT / "docs/runbooks/provider-acquisition-fargate.md"


def test_raon_runbook_keeps_runtime_neutral_security_boundary() -> None:
    runbook = RAON_RUNBOOK.read_text(encoding="utf-8")
    # The YAML front matter carries repository metadata such as `last_reviewed`.
    # That date is not operational evidence; apply the security-boundary scan to
    # the public runbook body so metadata cannot create a false positive.
    runbook_body = re.sub(
        r"\A---\r?\n.*?\r?\n---\r?\n",
        "",
        runbook,
        count=1,
        flags=re.DOTALL,
    )

    public_evidence_patterns = {
        "dated execution result": r"\b20\d{2}-\d{2}-\d{2}\b",
        "concrete dataset assignment": r"download_ds_id=\d",
        "concrete file-number assignment": r"file_no=\d",
        "concrete provider-file assignment": r"provider_file_id=[^<\s]",
        "concrete Bronze object key": r"bronze/source=[^<\s]",
        "checksum value": r"\b[0-9a-f]{64}\b",
        "execution ratio": r"\b\d+/\d+\b",
        "measured byte count": r"\b\d+(?:\.\d+)?\s*(?:GiB|MiB|bytes?)\b",
        "dated operational status": (
            r"\bcurrent(?:ly)?\b[^\n]{0,120}"
            r"\b(?:blocked|passed|completed|started|requires?)\b"
        ),
        "measured proof outcome": (
            r"\b(?:proof|collection|run)\b[^\n]{0,120}"
            r"\b(?:passed|completed|started)\b"
        ),
    }

    for evidence_kind, pattern in public_evidence_patterns.items():
        assert re.search(pattern, runbook_body, flags=re.IGNORECASE) is None, evidence_kind

    # Anchors are identifiers, placeholders, and the Korean sentences the runbook now uses. Six of
    # these were English prose until the Korean-first migration (558c5beb) translated them, at which
    # point this test failed for a policy-mandated edit rather than for a missing contract. Rewording
    # must not fail it; deleting a contract must.
    required_contracts = [
        # Runtime-neutral: this document selects no runtime.
        "런타임 중립 참고 문서",
        "Fargate를 선택하지 않음",
        # The adapter acquires; Rust owns validation, storage, lineage, and commit.
        "수집 adapter일 뿐이다",
        "Rust가 소유한다",
        "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE=1",
        "private replay request는 runtime 동안만",
        "공개 증거에는 cookie",
        "CreateOnly",
        "BronzeCommitter",
        "<provider-linux-package-url>",
        "<dataset-id>",
        "private operations",
    ]

    for required_contract in required_contracts:
        assert required_contract in runbook
