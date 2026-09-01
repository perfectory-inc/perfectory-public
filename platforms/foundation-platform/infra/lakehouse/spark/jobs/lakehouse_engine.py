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
CONTRACT_SCHEMA_VERSION = 2


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

    Every block the contract names is included. Iceberg's bundle backs its own storage layer
    and Hadoop's backs the `s3a://` filesystem; a job that reads a handoff object out of R2
    needs both, and a job that only writes tables carries one jar it does not open. Splitting
    the submission by what each job happens to touch would put that decision in eight places.
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

    coordinates = [f"{artifact}:{version}" for artifact in iceberg["artifacts"]]
    hadoop = contract["hadoop"]
    coordinates += [f"{artifact}:{hadoop['version']}" for artifact in hadoop["artifacts"]]
    return ",".join(coordinates)


# 카탈로그 접속 설정이 사는 곳.
#
# 2026-09-01 실측: 여덟 개 job 이 같은 여덟 줄을 각자 들고 있었고, **이미 갈라져 있었다** —
# 한 곳은 `s3.remote-signing-enabled` 가 없고 다른 한 곳은 `oauth2-server-uri` 가 없다. 새
# 설정이 필요해졌을 때 여섯 곳만 고친 것이다. 같은 사실이 여덟 곳에 있으면 언젠가 갈라지고,
# 갈라진 뒤에는 어느 쪽이 맞는지 아무도 모른다.
#
# 아홉 번째를 만들지 않으려고 여기 둔다. 기존 여덟 곳의 이전은 각각 돌려 봐야 하므로 별도로
# 한다 (root ADR-0069).
CATALOG_URI_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"
CATALOG_WAREHOUSE_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"
CATALOG_TOKEN_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN"
CATALOG_OAUTH2_SERVER_URI_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI"

ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value.strip():
        raise ValueError(f"{name} is required to reach the lakehouse catalog")
    return value.strip()


def oauth2_server_uri(catalog_uri: str) -> str:
    """Where the catalog issues tokens.

    Overridable because the catalog and its token endpoint need not share a host; derived from the
    catalog URI otherwise, which is what every job did on its own.
    """
    configured = os.environ.get(CATALOG_OAUTH2_SERVER_URI_ENV, "")
    if configured.strip():
        return configured.strip()
    return f"{catalog_uri.rstrip('/')}/v1/oauth/tokens"


def catalog_settings(catalog: str) -> dict[str, str]:
    """Every setting needed to reach the Iceberg REST catalog, as one mapping.

    Returned rather than applied so a caller can add it to a builder it already has, and so a test
    can read what would be applied without a Spark session.
    """
    catalog_uri = _required_env(CATALOG_URI_ENV)
    return {
        "spark.sql.extensions": ICEBERG_SPARK_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "rest",
        f"spark.sql.catalog.{catalog}.uri": catalog_uri,
        f"spark.sql.catalog.{catalog}.oauth2-server-uri": oauth2_server_uri(catalog_uri),
        f"spark.sql.catalog.{catalog}.warehouse": _required_env(CATALOG_WAREHOUSE_ENV),
        f"spark.sql.catalog.{catalog}.token": _required_env(CATALOG_TOKEN_ENV),
        # 자격증명을 카탈로그가 발급해 준다. 이것이 없으면 표는 열리는데 파일을 못 읽는다.
        f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"spark.sql.catalog.{catalog}.s3.remote-signing-enabled": "false",
    }


def apply_catalog_settings(builder: Any, catalog: str) -> Any:
    """Add the catalog settings to a Spark builder."""
    for key, value in catalog_settings(catalog).items():
        builder = builder.config(key, value)
    return builder
