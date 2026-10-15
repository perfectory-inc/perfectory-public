#!/usr/bin/env python3
"""The one way CI and the guards download a checksum-pinned release binary (root ADR-0197).

Before this, six scripts each ran their own `curl` against GitHub release assets, most of them
with no retry at all, and one 504 or 500 from the release CDN failed a whole pull request in the
merge queue (2026-10-12: actionlint inside identity's guard run, Martin inside Foundation's
toolchain step). Every download now goes through `fetch`:

* the file lives in a content-addressed cache, `<cache>/<sha256>/<asset name>`, and is checked
  against its pin on the way in and on every read; CI restores that directory with
  actions/cache, so a warm job never touches the network;
* a cold fill retries transient failures (connection errors, timeouts, truncated bodies, HTTP 408,
  425, 429 and 5xx) with a growing wait, and fails at once on anything that retrying cannot fix
  (404, 403, a checksum mismatch);
* a checksum mismatch is never cached and never retried: it is the supply-chain gate.

Where an asset is published is tools/pinned-artifacts.json; the version and the SHA-256 stay in
the env file (or the contract) the entry names, so a pin still has one home.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import pathlib
import platform
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "tools" / "pinned-artifacts.json"
CACHE_ENVIRONMENT_VARIABLE = "PERFECTORY_PINNED_CACHE"
# Waits before the 2nd..5th attempt: about two and a half minutes in all, which outlasts the
# release CDN blips seen so far (single 5xx answers) without hiding a real outage for long.
DEFAULT_BACKOFF_SECONDS: tuple[float, ...] = (5, 15, 45, 90)
RETRYABLE_STATUS = frozenset({408, 425, 429})
USER_AGENT = "perfectory-pinned-fetch/1"


class FetchError(RuntimeError):
    """The pinned file could not be produced."""


class _Transient(Exception):
    """A failure the next attempt may not see."""


def default_cache_dir() -> pathlib.Path:
    configured = os.environ.get(CACHE_ENVIRONMENT_VARIABLE, "").strip()
    if configured:
        return pathlib.Path(configured)
    return pathlib.Path.home() / ".cache" / "perfectory" / "pinned"


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _check_sha(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise FetchError(f"pin must be a lowercase SHA-256, found {value!r}")
    return value


def _asset_name(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https":
        raise FetchError(f"pinned downloads must use HTTPS: {url}")
    name = pathlib.PurePosixPath(urllib.parse.unquote(parsed.path)).name
    if not name or name in {".", ".."}:
        raise FetchError(f"cannot name the asset of {url}")
    return name


def _download_once(url: str, partial: pathlib.Path, timeout: float, opener: Callable[..., Any]) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with opener(request, timeout=timeout) as response, partial.open("wb") as sink:
            expected = response.headers.get("Content-Length") if response.headers else None
            written = 0
            for chunk in iter(lambda: response.read(1 << 20), b""):
                sink.write(chunk)
                written += len(chunk)
            if expected is not None and expected.isdigit() and written != int(expected):
                raise _Transient(f"body ended after {written} of {expected} bytes")
    except urllib.error.HTTPError as error:
        if error.code >= 500 or error.code in RETRYABLE_STATUS:
            raise _Transient(f"HTTP {error.code}") from error
        raise FetchError(f"{url}: HTTP {error.code} {error.reason}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException) as error:
        raise _Transient(f"{type(error).__name__}: {error}") from error


def fetch(
    url: str,
    sha256: str,
    *,
    cache_dir: pathlib.Path | None = None,
    backoff: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
    timeout: float = 60,
    opener: Callable[..., Any] = urllib.request.urlopen,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = lambda message: print(message, file=sys.stderr),
) -> pathlib.Path:
    """Return the cached path of the file `url` names, verified against `sha256`."""
    expected = _check_sha(sha256)
    name = _asset_name(url)
    entry = (cache_dir or default_cache_dir()) / expected
    cached = entry / name
    if cached.is_file():
        if _sha256_file(cached) == expected:
            log(f"pinned-fetch: cache hit {name} sha256={expected[:12]}")
            return cached
        log(f"pinned-fetch: cached {name} does not match its pin; fetching it again")
        cached.unlink()
    entry.mkdir(parents=True, exist_ok=True)

    attempts = len(backoff) + 1
    last = "no attempt was made"
    for attempt in range(1, attempts + 1):
        descriptor, partial_name = tempfile.mkstemp(prefix=f".{name}.", suffix=".partial", dir=entry)
        os.close(descriptor)
        partial = pathlib.Path(partial_name)
        try:
            _download_once(url, partial, timeout, opener)
            actual = _sha256_file(partial)
            if actual != expected:
                raise FetchError(
                    f"{name} SHA-256 mismatch: pinned {expected}, downloaded {actual}; not cached"
                )
            os.replace(partial, cached)
            log(f"pinned-fetch: fetched {name} on attempt {attempt}/{attempts}")
            return cached
        except _Transient as error:
            last = str(error)
        finally:
            partial.unlink(missing_ok=True)
        if attempt < attempts:
            wait = backoff[attempt - 1]
            log(f"pinned-fetch: {name} attempt {attempt}/{attempts} failed ({last}); retrying in {wait:g}s")
            sleep(wait)
    raise FetchError(f"{url}: gave up after {attempts} attempts; last failure: {last}")


# --- Registry -------------------------------------------------------------------------------------


def current_platform() -> tuple[str, str]:
    system = platform.system().lower()
    if system.startswith(("mingw", "msys", "cygwin")):
        system = "windows"
    machine = platform.machine().lower()
    machine = {"amd64": "x86_64", "x64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
    return system, machine


def read_env_file(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    return values


def load_registry(path: pathlib.Path = REGISTRY) -> dict[str, Any]:
    try:
        registry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise FetchError(f"cannot read {path}: {error}") from error
    if registry.get("schema_version") != 1 or not isinstance(registry.get("tools"), dict):
        raise FetchError(f"{path}: schema_version 1 with a tools object is required")
    return registry


def tool_pin(
    registry: Mapping[str, Any],
    name: str,
    system: str,
    machine: str,
    root: pathlib.Path = ROOT,
) -> tuple[str, str] | None:
    """The (url, sha256) of tool `name` for a platform, or None if the tool is not pinned there."""
    tools = registry["tools"]
    if name not in tools:
        raise FetchError(f"unknown pinned tool {name}; tools/pinned-artifacts.json lists {sorted(tools)}")
    spec = tools[name]
    if system not in spec["os"] or machine not in spec["arch"]:
        return None
    pins = read_env_file(root / spec["pins"])
    os_name, arch_name = spec["os"][system], spec["arch"][machine]
    sha_variable = spec["sha256_variable"].format(OS=os_name.upper(), ARCH=arch_name.upper())
    version = pins.get(spec["version_variable"], "")
    sha256 = pins.get(sha_variable, "")
    if not version or not sha256:
        raise FetchError(f"{spec['pins']} must set {spec['version_variable']} and {sha_variable}")
    url = spec["url"].format(
        version=version,
        os=os_name,
        arch=arch_name,
        extension=spec["extension"][system],
        # A release that names its assets by Rust target triple (cargo-nextest) maps each OS to
        # its triple here; the pin variables keep the short OS and ARCH names.
        target=spec.get("target", {}).get(system, ""),
    )
    return url, _check_sha(sha256)


def contract_pins(
    registry: Mapping[str, Any], system: str, machine: str, root: pathlib.Path = ROOT
) -> list[tuple[str, str]]:
    """Every (url, sha256) a static-release toolchain contract pins for a platform."""
    key = f"{system}-{machine}"
    pins: list[tuple[str, str]] = []
    for relative in registry.get("contracts", []):
        contract = json.loads((root / relative).read_text(encoding="utf-8"))
        for distribution in contract["distributions"].values():
            artifact = distribution["platforms"].get(key)
            if artifact is not None:
                pins.append((artifact["url"], _check_sha(artifact["sha256"])))
    return pins


def all_pins(registry: Mapping[str, Any], system: str, machine: str, root: pathlib.Path = ROOT) -> list[tuple[str, str, str]]:
    result: list[tuple[str, str, str]] = []
    for name in sorted(registry["tools"]):
        pin = tool_pin(registry, name, system, machine, root)
        if pin is not None:
            result.append((name, *pin))
    for url, sha256 in contract_pins(registry, system, machine, root):
        result.append((_asset_name(url), url, sha256))
    return result


def prune(cache_dir: pathlib.Path, keep: set[str]) -> int:
    """Remove every cache entry no current pin names, so a restored cache does not grow forever."""
    removed = 0
    if cache_dir.is_dir():
        for entry in cache_dir.iterdir():
            if entry.name not in keep:
                shutil.rmtree(entry) if entry.is_dir() else entry.unlink()
                removed += 1
    return removed


def extract_member(archive: pathlib.Path, member: str, output: pathlib.Path) -> None:
    """Write one named file out of a verified zip or tar.gz archive, executable.

    Python does it rather than `tar`: on Windows the `tar` a shell finds first may be GNU tar,
    which cannot read the zip archives Windows releases ship.
    """
    try:
        if zipfile.is_zipfile(archive):
            with zipfile.ZipFile(archive) as source:
                payload = source.read(member)
        else:
            with tarfile.open(archive, mode="r:*") as source:
                extracted = source.extractfile(member)
                if extracted is None:
                    raise KeyError(member)
                payload = extracted.read()
    except (KeyError, OSError, tarfile.TarError, zipfile.BadZipFile) as error:
        raise FetchError(f"{archive.name} has no member {member}: {error}") from error
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_name(f".{output.name}.{os.getpid()}.partial")
    staging.write_bytes(payload)
    staging.chmod(0o755)
    os.replace(staging, output)


def _copy_out(source: pathlib.Path, output: pathlib.Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_name(f".{output.name}.{os.getpid()}.partial")
    shutil.copyfile(source, staging)
    os.replace(staging, output)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--registry", type=pathlib.Path, default=REGISTRY)
    commands = parser.add_subparsers(dest="command", required=True)
    tool = commands.add_parser("tool", help="fetch one registered tool for this platform")
    tool.add_argument("name")
    tool.add_argument("--output", type=pathlib.Path, required=True)
    tool.add_argument(
        "--member",
        help="write this one file from inside the (zip or tar.gz) archive instead of the archive",
    )
    prefetch = commands.add_parser("prefetch", help="fill the cache with every pin for this platform")
    prefetch.add_argument(
        "--prune", action="store_true", help="also remove cached files no current pin names"
    )
    commands.add_parser("list", help="print every pin for this platform")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    arguments = _parser().parse_args(None if argv is None else list(argv))
    try:
        registry = load_registry(arguments.registry)
        system, machine = current_platform()
        if arguments.command == "tool":
            pin = tool_pin(registry, arguments.name, system, machine)
            if pin is None:
                raise FetchError(f"{arguments.name} is not pinned for {system}-{machine}")
            archive = fetch(*pin)
            if arguments.member:
                extract_member(archive, arguments.member, arguments.output)
            else:
                _copy_out(archive, arguments.output)
        elif arguments.command == "prefetch":
            pins = all_pins(registry, system, machine)
            for _, url, sha256 in pins:
                fetch(url, sha256)
            pruned = prune(default_cache_dir(), {sha256 for _, _, sha256 in pins}) if arguments.prune else 0
            print(
                f"OK pinned-prefetch {system}-{machine} files={len(pins)} pruned={pruned} "
                f"cache={default_cache_dir()}"
            )
        else:
            for name, url, sha256 in all_pins(registry, system, machine):
                print(f"{name}\t{sha256}\t{url}")
        return 0
    except FetchError as error:
        print(f"FAIL pinned-fetch: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
