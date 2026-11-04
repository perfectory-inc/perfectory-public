#!/usr/bin/env python3
"""No tracked source writes a 시도 code table by hand (root ADR-0403, ADR-0103 §2).

The incident it prevents: on 2026-07-01 광주(29) and 전남(46) merged into 12. The 시도 prefixes the
address-to-PNU step read were a hand-written list in a source contract, so for four months every
지번 address there named an abolished code: 4,771 of 4,774 court-auction items and every
registered-factory 지번 address in 광주·전남 stayed without a parcel (measured 2026-11-04). A 시도
code comes from the 법정동 code table and its recorded pairs (`address_pnu.sido_prefixes`), which
follow every change.

A file is refused when it holds either shape (the shapes, not a list of codes: this checker must not
become the table it forbids):

- a mapping between two-digit codes and Korean words: eight or more distinct codes, each paired with
  a Hangul string literal next to it (`"11": "서울"`, `"서울": "11"`);
- a list named for 시도 (an identifier with `sido`) of eight or more distinct two-digit literals.

`tools/ci/hardcoded-sido-codes.baseline.json` lists the files that still hold one, each with why and
what removes it. It only shrinks: a listed file that no longer holds a table fails as stale.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

BASELINE = "tools/ci/hardcoded-sido-codes.baseline.json"
SUFFIXES = (".py", ".rs", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".json", ".sql", ".sh", ".toml", ".yaml", ".yml")
MIN_CODES = 8
CODE_TO_WORD = re.compile(r"""["'](\d{2})["']\s*(?::|=>|,)\s*["']([가-힣]{2,12})["']""")
WORD_TO_CODE = re.compile(r"""["']([가-힣]{2,12})["']\s*(?::|=>)\s*["'](\d{2})["']""")
SIDO_LIST = re.compile(r"""(?i)\w*sido\w*[^\[\n]{0,80}[\[(]((?:\s*["']\d{2}["']\s*,?)+)""")
TWO_DIGITS = re.compile(r"""["'](\d{2})["']""")


def table_shapes(text: str) -> list[str]:
    """What hand-written 시도 tables the text holds, described; empty when none."""

    found = []
    mapped = {code for code, _ in CODE_TO_WORD.findall(text)} | {code for _, code in WORD_TO_CODE.findall(text)}
    if len(mapped) >= MIN_CODES:
        found.append(f"a code-to-word mapping of {len(mapped)} two-digit codes")
    for match in SIDO_LIST.finditer(text):
        codes = set(TWO_DIGITS.findall(match.group(1)))
        if len(codes) >= MIN_CODES:
            found.append(f"a sido list of {len(codes)} two-digit codes")
    return found


# A cheap first pass: only files where a two-digit literal touches a Hangul literal, or `sido` appears.
CANDIDATE = (r"""["'][0-9]{2}["'][[:space:]]*(:|=>|,)[[:space:]]*["'][가-힣]"""
             r"""|[가-힣]["'][[:space:]]*(:|=>)[[:space:]]*["'][0-9]{2}["']|[Ss][Ii][Dd][Oo]""")


def tracked(root: Path) -> list[str]:
    """The tracked source files the first pass keeps (`git grep` over the index's files)."""

    result = subprocess.run(["git", "-C", str(root), "grep", "--cached", "-l", "-z", "-I", "-E", CANDIDATE],
                            capture_output=True, check=False)
    if result.returncode not in (0, 1):
        raise SystemExit(f"git grep failed: {result.stderr.decode('utf-8', 'replace')}")
    return [p for p in result.stdout.decode("utf-8").split("\0") if p.endswith(SUFFIXES)]


def main(argv: list[str]) -> int:
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8")
    root = Path(argv[1] if len(argv) > 1 else Path(__file__).resolve().parents[2])
    baseline_doc = json.loads((root / BASELINE).read_text(encoding="utf-8"))
    allowed = {entry["path"]: entry for entry in baseline_doc["files"]}
    problems = []
    for entry in allowed.values():
        if not (str(entry.get("reason", "")).strip() and str(entry.get("removed_by", "")).strip()):
            problems.append(f"{BASELINE}: {entry['path']} names no reason or removed_by")
    holding = set()
    for path in tracked(root):
        if path == BASELINE:
            continue
        try:
            text = (root / path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        shapes = table_shapes(text)
        if not shapes:
            continue
        holding.add(path)
        if path not in allowed:
            problems.append(f"{path}: {', '.join(shapes)}; read 시도 codes from the 법정동 code table "
                            "(address_pnu.sido_prefixes, root ADR-0403)")
    for path in sorted(set(allowed) - holding):
        problems.append(f"{BASELINE}: {path} holds no 시도 table any more; remove it from the list")
    if problems:
        print("FAIL sido-codes-come-from-the-legal-dong-table:\n  " + "\n  ".join(problems), file=sys.stderr)
        return 1
    print(f"OK sido-codes-come-from-the-legal-dong-table (baseline {len(allowed)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
