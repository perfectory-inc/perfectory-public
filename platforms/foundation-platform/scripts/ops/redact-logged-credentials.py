#!/usr/bin/env python3
"""one-off: until the deploy's `redact-logged-credentials` line reports files_redacted=0 and
journal_lines=0 on the host; the PR after that deploy removes this file, its deploy step and its test.

Blank out every credential of the host that a run log holds, in place:

    redact-logged-credentials.py [--environment-dir DIR]... [--no-journal] <log root>...

2026-11-11 23:34Z..23:38Z Spark printed the lakehouse catalog writer token seven times into
/var/lib/foundation-gold-panel-rebuild/parcel-real-transaction/runs/20261111T231323Z/
real_transaction_silver_to_gold.py.log (mode 0644 under 0755 and 0777 directories: every account on
the host could read it). The settings no longer carry the value
(infra/lakehouse/spark/tests/test_no_credential_enters_the_spark_settings.py); this removes what was
written before that, and looks for every other credential of the host while it is there.

What it looks for is the values themselves, not a pattern: each NAME=VALUE of the environment files
(the runtime-secrets groups, /etc/foundation-platform/*.env, which only root reads) whose name is a
credential's and whose value is at least MIN_LENGTH long (also the releases' copies under
/opt/foundation-platform/config). Each occurrence in a log file under the
roots (*.log, *.log.*, *.out, *.err; links are not followed) is overwritten with `*` of the same
length, through the file's own inode, so a job that is appending to the file keeps its place. A
file that held one also loses its access for other accounts. The journal cannot be edited: its
lines that hold a value are counted, and a count above zero is the operator's to age out
(journalctl --vacuum-time).

It prints paths and counts, never a value or part of one. Exit 0 when it read everything it was
asked to; 1 when a root or the environment directory could not be read (the deploy says so and asks
again at its next run).
"""

from __future__ import annotations

import argparse
import mmap
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

CREDENTIAL_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|PASSPHRASE|CREDENTIAL|_KEY(_|$)|ACCESS_KEY")
LOG_NAME = re.compile(r"\.(log(\.[^/]*)?|out|err)$")
# Shorter values are settings that happen to be named like a credential (a key id prefix, a flag);
# blanking a short string wherever it appears would rewrite ordinary words.
MIN_LENGTH = 20
JOURNAL_SECONDS = 600
# config/runtime-secrets.contract.json's two homes of a group file: the host's and each release's.
ENVIRONMENT_DIRECTORIES = [Path("/etc/foundation-platform"), Path("/opt/foundation-platform/config")]


def say(text: str) -> None:
    print(f"redact-logged-credentials: {text}", flush=True)


def credentials(directories: list[Path]) -> list[bytes]:
    values: set[bytes] = set()
    for path in sorted(found for directory in directories for found in directory.rglob("*.env")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            name, separator, value = line.strip().partition("=")
            if not separator or name.startswith("#") or not CREDENTIAL_NAME.search(name):
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            if len(value) >= MIN_LENGTH:
                values.add(value.encode("utf-8"))
    # Longest first: a value that contains another is blanked whole.
    return sorted(values, key=len, reverse=True)


def log_files(root: Path):
    for directory, _, names in os.walk(root, followlinks=False):
        for name in names:
            if LOG_NAME.search(name):
                path = Path(directory, name)
                info = os.lstat(path)
                if stat.S_ISREG(info.st_mode) and info.st_size:
                    yield path, info


def redact(path: Path, info: os.stat_result, values: list[bytes]) -> int:
    """Occurrences blanked in the file, in place."""
    found = 0
    with open(path, "r+b") as handle, mmap.mmap(handle.fileno(), 0) as view:
        for value in values:
            at = view.find(value)
            while at != -1:
                view[at:at + len(value)] = b"*" * len(value)
                found += 1
                at = view.find(value, at + len(value))
        if found:
            view.flush()
    if found:
        os.chmod(path, stat.S_IMODE(info.st_mode) & ~0o007)
    return found


def journal_lines(values: list[bytes]) -> int | None:
    """Journal lines that hold a value; None when the journal could not be read to its end."""
    try:
        reader = subprocess.Popen(["journalctl", "--no-pager", "-o", "cat"], stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL)
    except OSError:
        return None
    lines = 0
    try:
        with reader:
            for line in reader.stdout:
                if any(value in line for value in values):
                    lines += 1
            return lines if reader.wait(timeout=JOURNAL_SECONDS) == 0 else None
    except subprocess.TimeoutExpired:
        reader.kill()
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment-dir", type=Path, action="append")
    parser.add_argument("--no-journal", action="store_true")
    parser.add_argument("roots", nargs="+", type=Path)
    args = parser.parse_args()
    directories = args.environment_dir or ENVIRONMENT_DIRECTORIES
    try:
        values = credentials(directories)
    except OSError as error:
        say(f"cannot read {error.filename}: {error.strerror}")
        return 1
    if not values:
        say(f"{' '.join(map(str, directories))}: no credential named; nothing to look for")
        return 0
    status = scanned = redacted = occurrences = 0
    for root in args.roots:
        if not root.is_dir():
            say(f"{root}: not on this host")
            continue
        for path, info in log_files(root):
            scanned += 1
            try:
                found = redact(path, info, values)
            except (OSError, ValueError) as error:
                say(f"{path}: could not be read ({type(error).__name__})")
                status = 1
                continue
            if found:
                redacted += 1
                occurrences += found
                say(f"{path}: blanked {found}, mode {stat.S_IMODE(os.lstat(path).st_mode):04o}")
    journal = "not-read" if args.no_journal else journal_lines(values)
    if journal is None:
        journal, status = "unreadable", 1
    say(f"credentials_known={len(values)} files_scanned={scanned} files_redacted={redacted} "
        f"occurrences={occurrences} journal_lines={journal}")
    return status


if __name__ == "__main__":
    sys.exit(main())
