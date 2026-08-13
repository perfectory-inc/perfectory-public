#!/usr/bin/env python3
"""Keep machine-checked document contracts declared, unique, and referenced.

Contract tests used to assert the *prose* of the thing they guarded. ADR-0009 requires
human-readable narrative to be Korean, so the Korean-first migration broke six such tests at once:
each failed for a policy-mandated edit rather than for a missing mechanism, and each would equally
have passed with the mechanism deleted and its sentence left behind (root ADR-0012 rule 4).

The replacement is a marker on the section itself:

    <!-- contract: external-acquisition-pull -->
    ### 외부 제공기관 수집은 가져오기(Pull)

Nothing translates an HTML comment, it renders as nothing, and it disappears exactly when its
section does. This guard supplies the halves a test cannot:

* a marker names exactly one section within its document, so a reference is never ambiguous;
* a malformed marker fails loudly instead of silently declaring nothing while looking declarative;
* every id a test names is declared by some document, so renaming a section's id without updating
  its test is caught here rather than surviving as an assertion against nothing.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

STRICT_MARKER = re.compile(
    r"^<!--\s*contract:\s*([a-z0-9]+(?:-[a-z0-9]+)*)\s*-->\s*$", re.MULTILINE
)
LOOSE_MARKER = re.compile(r"<!--\s*contract:", re.MULTILINE)
QUOTED_KEBAB = re.compile(r"""["']([a-z0-9]+(?:-[a-z0-9]+)+)["']""")
# References are read only from a `CONTRACT_IDS` collection. The first version scanned every quoted
# kebab-case string in a participating file and flagged `"utf-8"` as a dangling contract; narrowing
# the reference surface to one named constant removes the guesswork instead of denylisting the
# false positives.
CONTRACT_IDS_BLOCK = re.compile(
    r"CONTRACT_IDS\s*(?::[^=]*)?=\s*[\[{(](?P<body>.*?)[\]})]", re.DOTALL
)

DOCUMENT_ROOTS = ("docs", "platforms", "products")
SOURCE_ROOTS = ("platforms", "products", "scripts")
SOURCE_SUFFIXES = (".py", ".rs")
PRUNED = {"target", "node_modules", "__pycache__", ".venv", ".git"}


def walk(root: Path, suffixes: tuple[str, ...]) -> list[Path]:
    found: list[Path] = []
    for path in root.rglob("*"):
        if any(part in PRUNED for part in path.parts):
            continue
        if path.is_file() and path.suffix in suffixes:
            found.append(path)
    return found


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


def main(argv: list[str]) -> int:
    repo_root = Path(argv[1]).resolve() if len(argv) > 1 else Path(__file__).resolve().parents[2]

    failures: list[str] = []
    declared: dict[str, list[Path]] = {}

    for root_name in DOCUMENT_ROOTS:
        root = repo_root / root_name
        if not root.is_dir():
            continue
        for document in walk(root, (".md",)):
            text = read(document)
            strict = STRICT_MARKER.findall(text)
            loose = len(LOOSE_MARKER.findall(text))
            if loose != len(strict):
                failures.append(
                    f"{document.relative_to(repo_root).as_posix()} has a malformed contract "
                    "marker; the form is '<!-- contract: kebab-case-id -->' alone on its line"
                )
            for identifier, count in Counter(strict).items():
                if count > 1:
                    failures.append(
                        f"{document.relative_to(repo_root).as_posix()} declares "
                        f"{identifier!r} {count} times; a contract id must name one section"
                    )
                declared.setdefault(identifier, []).append(document)

    for identifier, documents in sorted(declared.items()):
        if len(documents) > 1:
            paths = ", ".join(d.relative_to(repo_root).as_posix() for d in documents)
            failures.append(
                f"{identifier!r} is declared by more than one document ({paths}); a reference "
                "must resolve to one section"
            )

    referenced: set[str] = set()
    for root_name in SOURCE_ROOTS:
        root = repo_root / root_name
        if not root.is_dir():
            continue
        for source in walk(root, SOURCE_SUFFIXES):
            text = read(source)
            if "CONTRACT_IDS" not in text:
                continue
            for block in CONTRACT_IDS_BLOCK.finditer(text):
                names = QUOTED_KEBAB.findall(block.group("body"))
                if not names:
                    failures.append(
                        f"{source.relative_to(repo_root).as_posix()} has an empty CONTRACT_IDS "
                        "collection; remove it or name the contracts it asserts"
                    )
                for identifier in names:
                    referenced.add(identifier)
                    if identifier not in declared:
                        failures.append(
                            f"{source.relative_to(repo_root).as_posix()} names contract "
                            f"{identifier!r}, which no document declares"
                        )

    if failures:
        for failure in failures:
            print(f"FAIL document-contract-markers: {failure}", file=sys.stderr)
        return 1

    print(
        f"OK document-contract-markers (declared={len(declared)}, referenced={len(referenced)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
