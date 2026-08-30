#!/usr/bin/env python3
"""Read the Iceberg package coordinates every lakehouse job submits with.

The version used to be spelled in each job that needed it, plus the Rust submitters, plus
`.env.example`, plus a runbook — twelve places. Raising it meant editing all twelve, so it was
never raised, and the deployment ran 1.6.1 while five releases went by. One of them fixed the
vectorized-read defect that cost two days to find (root ADR-0064).

No PySpark import here on purpose: the lane that runs `infra/lakehouse/spark/tests` has no
PySpark, and a module-level import would make every check that touches this file skip itself.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

CONTRACT_PATH_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_ENGINE_CONTRACT_PATH"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[2] / "contracts" / "lakehouse-engine.contract.json"
)
CONTRACT_SCHEMA_VERSION = 1


def load_engine_contract() -> dict[str, Any]:
    path = Path(os.getenv(CONTRACT_PATH_ENV, str(DEFAULT_CONTRACT_PATH)))
    contract = json.loads(path.read_text(encoding="utf-8"))
    version = contract.get("schema_version")
    if version != CONTRACT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported lakehouse engine contract schema_version {version!r}; "
            f"expected {CONTRACT_SCHEMA_VERSION!r}"
        )
    return contract


def _version_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def iceberg_packages() -> str:
    """Return the comma-joined Maven coordinates for `spark-submit --packages`.

    Assembled from the contract rather than written out, so a job cannot submit with a version
    the contract does not name. The minimum is enforced here rather than left as a comment:
    a version below it reads this deployment's larger tables by corrupting native memory, and
    the failure surfaces as a JVM abort with no mention of Iceberg in it.
    """
    contract = load_engine_contract()
    iceberg = contract["iceberg"]
    version = iceberg["version"]
    minimum = iceberg["minimum_version"]

    if _version_tuple(version) < _version_tuple(minimum):
        raise ValueError(
            f"iceberg version {version} is below the contract minimum {minimum}: "
            f"{iceberg['minimum_version_reason']}"
        )

    return ",".join(f"{artifact}:{version}" for artifact in iceberg["artifacts"])
