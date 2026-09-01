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
# 2026-09-01 이전 완료: 여덟 곳이 여기를 부른다. 함께 있던 복사본들도 같이 없앴다 —
# `required_iceberg_env()` 7벌, `require_env()` 6벌, `lakehouse_oauth2_server_uri()` 6벌.
CATALOG_URI_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"
CATALOG_WAREHOUSE_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"
CATALOG_TOKEN_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN"
CATALOG_OAUTH2_SERVER_URI_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI"

ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


class _EnvReader:
    """Reads the environment and remembers what it was asked for.

    The remembering is the point. A precondition check ("these three variables must be set
    before Spark starts") written beside the settings is a second copy of the same fact, and
    seven jobs each carried one — so a setting could be added to the assembly without any of
    the seven learning that it existed. `required_catalog_env` runs the real assembly against
    this reader instead, so the list is asked of the code rather than kept next to it.
    """

    def __init__(self, lookup: Any) -> None:
        self._lookup = lookup
        self.required: list[str] = []
        self.optional: list[str] = []

    def require(self, name: str) -> str:
        self.required.append(name)
        value = (self._lookup(name) or "").strip()
        if not value:
            raise ValueError(f"{name} is required to reach the lakehouse catalog")
        return value

    def optional_value(self, name: str) -> str:
        self.optional.append(name)
        return (self._lookup(name) or "").strip()


def _assemble_catalog_settings(catalog: str, reader: _EnvReader) -> dict[str, str]:
    """The one place the settings are spelled. Everything else asks this."""
    catalog_uri = reader.require(CATALOG_URI_ENV)
    settings = {
        "spark.sql.extensions": ICEBERG_SPARK_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "rest",
        f"spark.sql.catalog.{catalog}.uri": catalog_uri,
        f"spark.sql.catalog.{catalog}.warehouse": reader.require(CATALOG_WAREHOUSE_ENV),
        f"spark.sql.catalog.{catalog}.token": reader.require(CATALOG_TOKEN_ENV),
        # 자격증명을 카탈로그가 발급해 준다. 이것이 없으면 표는 열리는데 파일을 못 읽는다.
        f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"spark.sql.catalog.{catalog}.s3.remote-signing-enabled": "false",
    }
    # 토큰 발급처는 준 경우에만 넣는다. 여섯 개 job 은 카탈로그 주소에서 만들어 넣었고 한
    # 개는 넣지 않았는데, 후자는 빠뜨린 것이 아니라 "이 정적 토큰 방식에는 발급처가 없다"고
    # 시험으로 고정해 둔 것이었다. 정적 토큰을 주면 REST 클라이언트가 그 주소를 부르지
    # 않으므로 넣어도 무해했고, 그래서 양쪽 다 돌았다 — 그런 설정은 없는 쪽이 정본이다.
    configured_token_endpoint = reader.optional_value(CATALOG_OAUTH2_SERVER_URI_ENV)
    if configured_token_endpoint:
        settings[f"spark.sql.catalog.{catalog}.oauth2-server-uri"] = configured_token_endpoint
    return settings


def catalog_settings(catalog: str, lookup: Any = None) -> dict[str, str]:
    """Every setting needed to reach the Iceberg REST catalog, as one mapping.

    Returned rather than applied so a caller can add it to a builder it already has, and so a test
    can read what would be applied without a Spark session. `lookup` takes a name and returns the
    value or `None`, for callers that read the environment through something they can substitute.
    """
    return _assemble_catalog_settings(catalog, _EnvReader(lookup or os.environ.get))


def required_catalog_env(catalog: str = "lakehouse") -> tuple[str, ...]:
    """Which variables the settings need, asked of the assembly rather than listed beside it.

    The probe answers every lookup, so the assembly completes and reports what it read. A
    variable added to `_assemble_catalog_settings` therefore appears here without anyone
    editing this function — which is the whole reason the seven hand-written copies of this
    list could go stale and this one cannot.
    """
    reader = _EnvReader(lambda _name: "probe")
    _assemble_catalog_settings(catalog, reader)
    return tuple(dict.fromkeys(reader.required))


def assert_catalog_env(catalog: str = "lakehouse") -> None:
    """Fail before Spark starts if a variable the settings need is missing.

    Starting a session first turns a missing variable into a failure minutes later, inside a
    container, under a stack trace that names Iceberg rather than the variable.
    """
    reader = _EnvReader(os.environ.get)
    for name in required_catalog_env(catalog):
        reader.require(name)


def apply_catalog_settings(builder: Any, catalog: str, lookup: Any = None) -> Any:
    """Add the catalog settings to a Spark builder."""
    for key, value in catalog_settings(catalog, lookup).items():
        builder = builder.config(key, value)
    return builder


def catalog_classes(catalog: str = "lakehouse") -> tuple[str, ...]:
    """The JVM classes the settings name, read back out of the settings.

    Spelled again in the runtime check, they became a fifth and sixth copy of the same two
    strings — and a check that names a class the settings do not configure passes while the
    session is unusable.
    """
    settings = catalog_settings(catalog, lookup=lambda _name: "probe")
    return tuple(
        dict.fromkeys(
            value for value in settings.values() if value.startswith("org.apache.iceberg")
        )
    )


def assert_iceberg_runtime_loaded(spark: Any, packages: str) -> None:
    """Fail with the submit line to fix rather than with a ClassNotFoundException.

    Five jobs carried this check and each named the classes itself. Without it the failure is a
    Java class-loading error several frames below anything that mentions how a job is submitted.
    """
    class_loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for class_name in catalog_classes():
        try:
            class_loader.loadClass(class_name)
        except Exception as error:  # noqa: BLE001 - JVM 쪽 예외는 파이썬에서 종류를 못 좁힌다
            raise RuntimeError(
                "Iceberg Spark runtime is not loaded; run spark-submit with "
                f"--conf spark.jars.ivy=/tmp/.ivy2 --packages {packages}"
            ) from error
