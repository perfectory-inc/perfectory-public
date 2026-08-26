from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Literal


VWorldCredential = Literal["api_key", "domain", "username", "password"]

_LOGGER = logging.getLogger(__name__)
_CONTRACT_FILENAME = "environment-variable-naming.contract.json"
_CREDENTIAL_FIELDS: tuple[VWorldCredential, ...] = (
    "api_key",
    "domain",
    "username",
    "password",
)


def _contract_path() -> Path:
    roots = (Path.cwd(), *Path.cwd().parents, Path("/app"))
    for root in roots:
        candidate = root / "config" / _CONTRACT_FILENAME
        if candidate.is_file():
            return candidate
    raise RuntimeError(f"cannot find {_CONTRACT_FILENAME}")


@lru_cache(maxsize=1)
def _credential_names() -> dict[VWorldCredential, tuple[str, tuple[str, ...]]]:
    with _contract_path().open(encoding="utf-8") as handle:
        contract = json.load(handle)
    if contract.get("schema_version") != 1:
        raise RuntimeError("environment-variable naming contract schema must be 1")
    migration = contract.get("compatibility_migrations", {}).get(
        "foundation-vworld-credentials", {}
    )
    if migration.get("precedence") != "canonical-first":
        raise RuntimeError("VWorld credential precedence must be canonical-first")
    credentials = migration.get("credentials")
    if not isinstance(credentials, dict):
        raise RuntimeError("VWorld credential contract must declare credentials")

    names: dict[VWorldCredential, tuple[str, tuple[str, ...]]] = {}
    for field in _CREDENTIAL_FIELDS:
        value = credentials.get(field)
        if not isinstance(value, dict):
            raise RuntimeError(f"VWorld credential contract is missing {field}")
        canonical = value.get("canonical")
        aliases = value.get("deprecated_aliases")
        if not isinstance(canonical, str) or not isinstance(aliases, list) or not all(
            isinstance(alias, str) for alias in aliases
        ):
            raise RuntimeError(f"VWorld credential contract is invalid for {field}")
        names[field] = (canonical, tuple(aliases))
    return names


def resolve_vworld_credential(
    env: Mapping[str, str],
    credential: VWorldCredential,
    *,
    warn: Callable[[str], None] = _LOGGER.warning,
) -> str | None:
    canonical, deprecated_aliases = _credential_names()[credential]
    value = env.get(canonical)
    if value:
        return value
    for alias in deprecated_aliases:
        value = env.get(alias)
        if value:
            warn(
                f"deprecated environment variable {alias} supplied the value; "
                f"use {canonical}"
            )
            return value
    return None


def normalize_vworld_credentials(
    env: Mapping[str, str],
    *,
    warn: Callable[[str], None] = _LOGGER.warning,
) -> dict[str, str]:
    normalized = dict(env)
    for credential in _CREDENTIAL_FIELDS:
        canonical, deprecated_aliases = _credential_names()[credential]
        value = resolve_vworld_credential(normalized, credential, warn=warn)
        for alias in deprecated_aliases:
            normalized.pop(alias, None)
        if value:
            normalized[canonical] = value
    return normalized
