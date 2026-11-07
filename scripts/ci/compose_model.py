#!/usr/bin/env python3
"""The one way a guard reads Compose: the pinned Docker Compose's normalized model (root ADR-0422).

`model(files, profiles)` runs `docker compose config` on a project's files the way the host starts
them (later files override, includes merge, anchors and `!reset` resolve) and returns its JSON.
Interpolation, env files and path resolution are off, so a document carries `${NAME:?}` exactly as
written and no local secret reaches it. `raw(path)` is one file as the pinned Conftest's YAML parser
reads it, for what normalization hides (where an `include:` came from).

Both binaries come from the one pinned fetcher (scripts/ci/fetch_pinned.py, root ADR-0197) and are
fetched once per process into a private directory.
"""

from __future__ import annotations

import atexit
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

REPOSITORY = pathlib.Path(__file__).resolve().parents[2]
CONFIG = ("config", "--no-interpolate", "--no-env-resolution", "--no-path-resolution", "--format", "json")
_work: pathlib.Path | None = None


class ComposeError(RuntimeError):
    """Compose (or Conftest) refused to read a file; the message is its last line."""


def work() -> pathlib.Path:
    global _work
    if _work is None:
        _work = pathlib.Path(tempfile.mkdtemp(prefix="compose-model-"))
        atexit.register(shutil.rmtree, _work, True)
    return _work


def pinned_tool(tool: str, member: str | None = None) -> pathlib.Path:
    binary = work() / (f"{tool}.exe" if sys.platform == "win32" else tool)
    if not binary.exists():
        command = [sys.executable, str(REPOSITORY / "scripts/ci/fetch_pinned.py"), "tool", tool,
                   "--output", str(binary)]
        if member:
            command += ["--member", member]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, timeout=300)
        binary.chmod(0o755)
    return binary


def compose_binary() -> pathlib.Path:
    return pinned_tool("docker-compose")


def conftest_binary() -> pathlib.Path:
    return pinned_tool("conftest", "conftest.exe" if sys.platform == "win32" else "conftest")


def _last_line(completed: subprocess.CompletedProcess) -> str:
    return ((completed.stderr or "").strip().splitlines() or ["no output"])[-1][:300]


def model(files: list[pathlib.Path], profiles: list[str] | None = None) -> dict:
    """The normalized project; `profiles=["*"]` enables every profile."""
    command = [str(compose_binary())]
    for path in files:
        command += ["--file", str(path)]
    for profile in profiles or []:
        command += ["--profile", profile]
    completed = subprocess.run([*command, *CONFIG], capture_output=True, text=True, timeout=120)
    if completed.returncode != 0:
        raise ComposeError(f"compose config refused it: {_last_line(completed)}")
    return json.loads(completed.stdout)


def raw(path: pathlib.Path) -> dict:
    """One file as YAML, parsed by the pinned Conftest (no Compose semantics applied)."""
    completed = subprocess.run([str(conftest_binary()), "parse", str(path)],
                               capture_output=True, text=True, timeout=60)
    if completed.returncode != 0:
        raise ComposeError(f"cannot parse it as YAML: {_last_line(completed)}")
    return json.loads(completed.stdout)


def judge(policy: pathlib.Path, namespace: str, target: pathlib.Path, data: dict | None = None,
          parser: str | None = None) -> list[str]:
    """The `deny` messages a Conftest namespace returns for one document (`parser`: Conftest's)."""
    command = [str(conftest_binary()), "test", "--policy", str(policy), "--namespace", namespace,
               "--output", "json", "--no-color"]
    if parser:
        command += ["--parser", parser]
    if data is not None:
        # Conftest loads a data file by its extension.
        handle = tempfile.NamedTemporaryFile("w", suffix=".json", dir=work(), delete=False, encoding="utf-8")
        with handle:
            json.dump(data, handle)
        command += ["--data", handle.name]
    completed = subprocess.run([*command, str(target)], capture_output=True, text=True, timeout=60)
    try:
        results = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ComposeError(f"conftest could not judge it: {_last_line(completed)}") from error
    if completed.returncode not in (0, 1):
        raise ComposeError(f"conftest exited {completed.returncode}: {_last_line(completed)}")
    return [failure["msg"] for result in results for failure in result.get("failures") or []]


def write_model(name: str, document: dict) -> pathlib.Path:
    """A normalized model on disk, for `judge`."""
    path = work() / (name.replace("/", "_").replace("\\", "_") + ".json")
    path.write_text(json.dumps(document), encoding="utf-8")
    return path
