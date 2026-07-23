import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


RAON_RUNBOOK = REPO_ROOT / "docs/runbooks/provider-acquisition-fargate.md"


def test_raon_runbook_keeps_runtime_neutral_security_boundary() -> None:
    runbook = RAON_RUNBOOK.read_text(encoding="utf-8")

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
        assert re.search(pattern, runbook, flags=re.IGNORECASE) is None, evidence_kind

    required_contracts = [
        "Status: runtime-neutral reference; Fargate is not selected by this document",
        "Python/browser code is an acquisition adapter only. Rust owns validation",
        "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE=1",
        "The private replay request is runtime-only.",
        "Public proof must not contain cookies",
        "R2 writes must use CreateOnly.",
        "Bronze commit must go through `BronzeCommitter`.",
        "<provider-linux-package-url>",
        "<dataset-id>",
        "private operations evidence system",
    ]

    for required_contract in required_contracts:
        assert required_contract in runbook
