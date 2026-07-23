#!/usr/bin/env python3
"""Structurally enforce proprietary/non-publishable package metadata."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


def fail(message: str) -> None:
    print(f"FAIL package-publication-policy: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"cannot parse {path}: {error}")


def tracked_manifests(root: Path) -> list[Path]:
    try:
        output = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            check=True,
            stdout=subprocess.PIPE,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot enumerate tracked manifests: {error}")
    paths = []
    for raw in output.split(b"\0"):
        if not raw:
            continue
        relative = Path(raw.decode("utf-8", errors="strict"))
        if relative.name in {"Cargo.toml", "package.json", "pyproject.toml"}:
            paths.append(root / relative)
    return sorted(paths)


def inherited(value: Any) -> bool:
    return isinstance(value, dict) and value == {"workspace": True}


def validate_workspace(path: Path, document: dict[str, Any], root: Path) -> None:
    workspace = document.get("workspace")
    if not isinstance(workspace, dict):
        return
    package = workspace.get("package")
    if package is None:
        return
    if not isinstance(package, dict):
        fail(f"{path}: [workspace.package] must be a table")
    if "license" in package:
        fail(f"{path}: workspace must not claim an SPDX first-party license")
    expected_license = (path.parent / package.get("license-file", "")).resolve()
    canonical_license = (root / "LICENSES/LicenseRef-Proprietary.txt").resolve()
    if expected_license != canonical_license:
        fail(f"{path}: workspace license-file does not resolve to the canonical proprietary license")
    if package.get("publish") is not False:
        fail(f"{path}: [workspace.package].publish must be false")


def find_workspace(path: Path, root: Path) -> dict[str, Any] | None:
    current = path.parent
    while current == root or root in current.parents:
        candidate = current / "Cargo.toml"
        if candidate.is_file():
            document = load_toml(candidate)
            if isinstance(document.get("workspace"), dict):
                return document
        if current == root:
            break
        current = current.parent
    return None


def validate_cargo(path: Path, root: Path) -> None:
    document = load_toml(path)
    validate_workspace(path, document, root)
    package = document.get("package")
    if not isinstance(package, dict):
        return
    if "license" in package:
        fail(f"{path}: package must not claim an SPDX first-party license")

    license_value = package.get("license-file")
    publish_value = package.get("publish")
    if inherited(license_value) or inherited(publish_value):
        if not inherited(license_value) or not inherited(publish_value):
            fail(f"{path}: license-file and publish must be inherited together")
        workspace = find_workspace(path, root)
        if workspace is None:
            fail(f"{path}: inherits package policy without an enclosing workspace")
        workspace_package = workspace.get("workspace", {}).get("package", {})
        if workspace_package.get("publish") is not False:
            fail(f"{path}: inherited workspace publish policy is not false")
        return

    if not isinstance(license_value, str):
        fail(f"{path}: package requires license-file or workspace inheritance")
    declared = (path.parent / license_value).resolve()
    canonical = (root / "LICENSES/LicenseRef-Proprietary.txt").resolve()
    if declared != canonical:
        fail(f"{path}: package license-file does not resolve to the canonical proprietary license")
    if publish_value is not False:
        fail(f"{path}: package.publish must be false")


def validate_package_json(path: Path) -> None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot parse {path}: {error}")
    if not isinstance(document, dict):
        fail(f"{path}: package.json root must be an object")
    if document.get("private") is not True:
        fail(f"{path}: top-level private must be true")
    if document.get("license") != "UNLICENSED":
        fail(f"{path}: top-level license must be UNLICENSED")


def validate_pyproject(path: Path) -> None:
    document = load_toml(path)
    project = document.get("project")
    if not isinstance(project, dict):
        fail(f"{path}: [project] is required")
    if project.get("license") != "LicenseRef-Proprietary":
        fail(f"{path}: [project].license must be LicenseRef-Proprietary")
    classifiers = project.get("classifiers")
    if not isinstance(classifiers, list) or "Private :: Do Not Upload" not in classifiers:
        fail(f"{path}: [project].classifiers must prevent upload")


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <repository-root>", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    manifests = tracked_manifests(root)
    if not manifests:
        fail("no tracked package manifests found")
    for path in manifests:
        if path.name == "Cargo.toml":
            validate_cargo(path, root)
        elif path.name == "package.json":
            validate_package_json(path)
        else:
            validate_pyproject(path)
    print(f"OK package-publication-policy manifests={len(manifests)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
