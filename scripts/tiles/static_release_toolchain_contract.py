#!/usr/bin/env python3
"""Project and install the static-release toolchain from its JSON contract."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import pathlib
import platform
import re
import shlex
import shutil
import stat
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from collections.abc import Callable, Mapping
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_CONTRACT = (
    ROOT
    / "platforms"
    / "foundation-platform"
    / "config"
    / "static-release-toolchain.contract.json"
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
ENVIRONMENT_VARIABLE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
SUPPORTED_TOOL_NAMES = frozenset({"martin-cp", "mbtiles", "pmtiles"})


class ContractError(RuntimeError):
    """The contract or a materialized distribution violates the pin."""


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{field} must be an object")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be non-empty text")
    return value


def _sha256(value: object, field: str) -> str:
    text = _text(value, field)
    if not SHA256.fullmatch(text):
        raise ContractError(f"{field} must be a lowercase SHA-256")
    return text


def load_contract(path: pathlib.Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"could not read toolchain contract {path}: {error}") from error
    contract = dict(_mapping(value, "contract"))
    validate_contract(contract)
    return contract


def validate_contract(
    contract: Mapping[str, Any],
    *,
    _expected_tools: frozenset[str] = SUPPORTED_TOOL_NAMES,
) -> None:
    if contract.get("schema_version") != 1:
        raise ContractError("schema_version must be 1")
    tools = _mapping(contract.get("tools"), "tools")
    distributions = _mapping(contract.get("distributions"), "distributions")
    if set(tools) != _expected_tools:
        raise ContractError("tools must declare the complete supported tool set")

    distribution_tools: dict[str, set[str]] = {name: set() for name in distributions}
    for name, raw_tool in tools.items():
        tool = _mapping(raw_tool, f"tools.{name}")
        version = _text(tool.get("version"), f"tools.{name}.version")
        if not SEMVER.fullmatch(version):
            raise ContractError(f"tools.{name}.version must be an exact semantic version")
        command = tool.get("version_command")
        if not isinstance(command, list) or not command or not all(
            isinstance(argument, str) and argument for argument in command
        ):
            raise ContractError(f"tools.{name}.version_command must be a non-empty string array")
        if not isinstance(tool.get("banner_prefix"), str) or not isinstance(
            tool.get("banner_suffix"), str
        ):
            raise ContractError(f"tools.{name} banner fragments must be strings")
        _text(tool.get("compatibility_reason"), f"tools.{name}.compatibility_reason")
        distribution = _text(tool.get("distribution"), f"tools.{name}.distribution")
        if distribution not in distributions:
            raise ContractError(f"tools.{name} names unknown distribution {distribution}")
        distribution_tools[distribution].add(name)

    environment_variables: set[str] = set()
    for name, raw_distribution in distributions.items():
        distribution = _mapping(raw_distribution, f"distributions.{name}")
        _text(distribution.get("source"), f"distributions.{name}.source")
        oci = _mapping(distribution.get("oci"), f"distributions.{name}.oci")
        environment_variable = _text(
            oci.get("environment_variable"),
            f"distributions.{name}.oci.environment_variable",
        )
        if not ENVIRONMENT_VARIABLE.fullmatch(environment_variable):
            raise ContractError(
                f"distributions.{name}.oci.environment_variable must be a shell-safe name"
            )
        if environment_variable in environment_variables:
            raise ContractError(f"duplicate OCI environment variable {environment_variable}")
        environment_variables.add(environment_variable)
        _text(oci.get("repository"), f"distributions.{name}.oci.repository")
        tag_tool = _text(oci.get("tag_tool"), f"distributions.{name}.oci.tag_tool")
        if tag_tool not in distribution_tools[name]:
            raise ContractError(f"distributions.{name}.oci.tag_tool is not in that distribution")
        if not isinstance(oci.get("tag_prefix"), str):
            raise ContractError(f"distributions.{name}.oci.tag_prefix must be text")
        _sha256(oci.get("digest"), f"distributions.{name}.oci.digest")

        platforms = _mapping(distribution.get("platforms"), f"distributions.{name}.platforms")
        if not platforms:
            raise ContractError(f"distributions.{name}.platforms must not be empty")
        for platform_key, raw_artifact in platforms.items():
            artifact = _mapping(
                raw_artifact, f"distributions.{name}.platforms.{platform_key}"
            )
            url = _text(
                artifact.get("url"), f"distributions.{name}.platforms.{platform_key}.url"
            )
            if not url.startswith("https://"):
                raise ContractError(f"distribution URL must use HTTPS: {name}/{platform_key}")
            if artifact.get("archive_format") not in {"zip", "tar.gz"}:
                raise ContractError(f"unsupported archive format for {name}/{platform_key}")
            _sha256(
                artifact.get("sha256"),
                f"distributions.{name}.platforms.{platform_key}.sha256",
            )
            executables = _mapping(
                artifact.get("executables"),
                f"distributions.{name}.platforms.{platform_key}.executables",
            )
            if set(executables) != distribution_tools[name]:
                raise ContractError(
                    f"{name}/{platform_key} executable set does not match its tools"
                )
            for tool_name, raw_executable in executables.items():
                executable = _mapping(
                    raw_executable,
                    f"distributions.{name}.platforms.{platform_key}.executables.{tool_name}",
                )
                member = _text(executable.get("member"), f"{tool_name}.member")
                filename = _text(executable.get("filename"), f"{tool_name}.filename")
                if pathlib.PurePosixPath(member).is_absolute() or ".." in pathlib.PurePosixPath(
                    member
                ).parts:
                    raise ContractError(f"{tool_name}.member must be an archive-relative path")
                if pathlib.Path(filename).name != filename:
                    raise ContractError(f"{tool_name}.filename must be a basename")
                _sha256(executable.get("sha256"), f"{tool_name}.sha256")


def current_platform_key() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if machine in {"amd64", "x86_64"}:
        machine = "x86_64"
    return f"{system}-{machine}"


def expected_banner(tool: Mapping[str, Any]) -> str:
    return f'{tool["banner_prefix"]}{tool["version"]}{tool["banner_suffix"]}'


def image_ref(contract: Mapping[str, Any], distribution_name: str) -> str:
    distributions = _mapping(contract["distributions"], "distributions")
    distribution = _mapping(distributions[distribution_name], distribution_name)
    oci = _mapping(distribution["oci"], f"{distribution_name}.oci")
    tools = _mapping(contract["tools"], "tools")
    tag_tool = _text(oci["tag_tool"], f"{distribution_name}.oci.tag_tool")
    version = _text(_mapping(tools[tag_tool], tag_tool)["version"], f"{tag_tool}.version")
    return (
        f'{oci["repository"]}:{oci["tag_prefix"]}{version}'
        f'@sha256:{oci["digest"]}'
    )


def image_environment(contract: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    distributions = _mapping(contract["distributions"], "distributions")
    for name, raw_distribution in distributions.items():
        distribution = _mapping(raw_distribution, name)
        oci = _mapping(distribution["oci"], f"{name}.oci")
        result[str(oci["environment_variable"])] = image_ref(contract, name)
    return result


def _download(url: str, timeout_seconds: float) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "perfectory-toolchain/1"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read()


def _file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(64 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _extract_member(archive: bytes, archive_format: str, member: str) -> bytes:
    try:
        if archive_format == "zip":
            with zipfile.ZipFile(io.BytesIO(archive)) as value:
                return value.read(member)
        if archive_format == "tar.gz":
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as value:
                extracted = value.extractfile(member)
                if extracted is None:
                    raise KeyError(member)
                return extracted.read()
    except (KeyError, OSError, tarfile.TarError, zipfile.BadZipFile) as error:
        raise ContractError(f"declared archive member {member} is unavailable: {error}") from error
    raise ContractError(f"unsupported archive format {archive_format}")


def install_contract(
    contract: Mapping[str, Any],
    destination: pathlib.Path,
    *,
    platform_key: str | None = None,
    downloader: Callable[[str, float], bytes] = _download,
    timeout_seconds: float = 30,
    _expected_tools: frozenset[str] = SUPPORTED_TOOL_NAMES,
) -> list[pathlib.Path]:
    validate_contract(contract, _expected_tools=_expected_tools)
    if not 0 < timeout_seconds <= 300:
        raise ContractError("download timeout must be in 0 < seconds <= 300")
    selected_platform = platform_key or current_platform_key()
    distributions = _mapping(contract["distributions"], "distributions")

    selected: list[tuple[str, Mapping[str, Any]]] = []
    expected_files: dict[str, str] = {}
    for name, raw_distribution in distributions.items():
        distribution = _mapping(raw_distribution, name)
        platforms = _mapping(distribution["platforms"], f"{name}.platforms")
        if selected_platform not in platforms:
            raise ContractError(
                f"unsupported platform {selected_platform} for distribution {name}"
            )
        artifact = _mapping(platforms[selected_platform], selected_platform)
        selected.append((name, artifact))
        executables = _mapping(artifact["executables"], f"{name}.executables")
        for raw_executable in executables.values():
            executable = _mapping(raw_executable, f"{name}.executable")
            filename = str(executable["filename"])
            if filename in expected_files:
                raise ContractError(f"duplicate installed filename {filename}")
            expected_files[filename] = str(executable["sha256"])

    if destination.exists():
        if not destination.is_dir():
            raise ContractError("destination must be absent or a directory")
        existing = list(destination.iterdir())
        if existing:
            if {path.name for path in existing} != set(expected_files):
                raise ContractError("destination contains files outside the exact toolchain")
            for path in existing:
                if path.is_symlink() or not path.is_file():
                    raise ContractError("destination toolchain entries must be regular files")
                if _file_sha256(path) != expected_files[path.name]:
                    raise ContractError("destination contains a mismatched toolchain executable")
                path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            return [destination / filename for filename in expected_files]

    verified: dict[str, bytes] = {}
    for distribution_name, artifact in selected:
        url = str(artifact["url"])
        try:
            archive = downloader(url, timeout_seconds)
        except Exception as error:
            raise ContractError(f"{distribution_name} download failed: {error}") from error
        actual_archive_sha = hashlib.sha256(archive).hexdigest()
        if actual_archive_sha != artifact["sha256"]:
            raise ContractError(f"{distribution_name} archive SHA-256 mismatch")
        executables = _mapping(artifact["executables"], f"{distribution_name}.executables")
        for tool_name, raw_executable in executables.items():
            executable = _mapping(raw_executable, f"{distribution_name}.{tool_name}")
            payload = _extract_member(
                archive, str(artifact["archive_format"]), str(executable["member"])
            )
            if hashlib.sha256(payload).hexdigest() != executable["sha256"]:
                raise ContractError(f"{tool_name} executable SHA-256 mismatch")
            filename = str(executable["filename"])
            if filename in verified:
                raise ContractError(f"duplicate installed filename {filename}")
            verified[filename] = payload

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
    )
    try:
        for filename, payload in verified.items():
            target = staging / filename
            target.write_bytes(payload)
            target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        if destination.exists():
            destination.rmdir()
        os.replace(staging, destination)
    except OSError as error:
        shutil.rmtree(staging, ignore_errors=True)
        destination.mkdir(parents=True, exist_ok=True)
        raise ContractError(f"could not promote verified toolchain atomically: {error}") from error
    return [destination / filename for filename in verified]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=pathlib.Path, default=DEFAULT_CONTRACT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate")
    image_parser = subparsers.add_parser("image-env")
    image_parser.add_argument("--shell", action="store_true")
    install_parser = subparsers.add_parser("install")
    install_parser.add_argument("--destination", type=pathlib.Path, required=True)
    install_parser.add_argument("--platform", dest="platform_key")
    install_parser.add_argument("--timeout-seconds", type=float, default=30)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        contract = load_contract(arguments.contract)
        if arguments.command == "validate":
            print("OK static-release-toolchain-contract")
        elif arguments.command == "image-env":
            environment = image_environment(contract)
            if arguments.shell:
                for name, value in sorted(environment.items()):
                    print(f"export {name}={shlex.quote(value)}")
            else:
                print(json.dumps(environment, sort_keys=True))
        elif arguments.command == "install":
            installed = install_contract(
                contract,
                arguments.destination,
                platform_key=arguments.platform_key,
                timeout_seconds=arguments.timeout_seconds,
            )
            print(f"OK static-release-toolchain-install files={len(installed)}")
        return 0
    except ContractError as error:
        print(f"FAIL static-release-toolchain-contract: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
