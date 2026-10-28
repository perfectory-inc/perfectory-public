#!/usr/bin/env python3
"""Documentation debt that may shrink but never grow, and the drift worth a warning.

What failing this prevents, per check (root ADR-0001 guard policy):

- `dead-reference`: on 2026-10-28 a docs audit found references to files that no longer exist,
  and five runbooks whose operating steps were wrong — an operator following them during an
  incident would fail. A backticked repository path in a maintained document must resolve. The
  entry is the missing path, not the document, so moving text between documents is not new debt.
- `unindexed-runbook`: all six runbooks found wrong on 2026-10-28 were missing from their
  folder's README, so nobody reading the index ever reviewed them.
- `proposed-adr-cited-by-code`: on 2026-10-28 eleven root ADRs were `Proposed` although code
  cites them, and agents and the owner read them as undecided. Code may cite a Proposed ADR
  only when the ADR says, dated, what is still missing (`YYYY-MM-DD: ... Proposed 유지 ...` or
  `missing (YYYY-MM-DD): ...`).
- `supersession-unrecorded`: an ADR that says it supersedes ADR-X while ADR-X's status line
  never names it (ADR-0291 superseded part of ADR-0148, whose status still read Accepted).

Each check is a ratchet: today's findings are in `documentation-debt.baseline.json`, a finding
not in it fails, and a baseline entry that is gone is reported so the next change removes it.
`--shrink` rewrites the baseline to the entries that are still found; it cannot add one.

Warnings, never failures: an Accepted root ADR naming a `scripts/...` file that exists nowhere
(Accepted text is immutable, so the fix is a dated path-move footnote with `→`; ADR-0291 named
two guard scripts that were never written), and a `status: current` runbook whose
`last_reviewed` is older than 90 days (four of the six wrong runbooks were last reviewed
2026-07-29). Only runbooks: they are the documents an operator executes, and warning on every
page would bury these under two hundred lines nobody reads.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import posixpath
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
BASELINE = Path(__file__).with_name("documentation-debt.baseline.json")
CHECKS = ("dead-reference", "unindexed-runbook", "proposed-adr-cited-by-code", "supersession-unrecorded")
STALE_AFTER_DAYS = 90

# A backticked token with a directory and a code or document extension. `<`, `*`, `{` and `...`
# cannot match, so placeholders and globs are not paths.
PATH_TOKEN = re.compile(r"`([\w.][\w./-]*/[\w.-]+\.(?:sh|py|rs|ts|tsx|md|json|toml|yml|yaml|sql|mjs|js|avsc))`")
STATUS_LINE = re.compile(r"^\s*-\s*(?:Status|상태)\s*:\s*(.*)$", re.MULTILINE)
SUPERSEDES = re.compile(r"\bsupersedes:?\s+(?:the\s+)?(?:root\s+)?ADR[- ](\d{4})\b", re.IGNORECASE)
MISSING_NOTE = re.compile(
    r"^\d{4}-\d{2}-\d{2}:.*(?:Proposed 유지|missing:)|\bmissing\s*\(\d{4}-\d{2}-\d{2}\)\s*:",
    re.MULTILINE | re.IGNORECASE,
)
CODE_CITATION = re.compile(r"(?:^|[^A-Za-z-])ADR[- ](\d{4})")


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


def area_dirs(root: Path) -> list[str]:
    contract = json.loads((root / "tools/areas.contract.json").read_text(encoding="utf-8"))
    return [area["dir"] for area in contract["areas"]]


def frontmatter(text: str) -> dict[str, str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    values: dict[str, str] = {}
    for line in lines[1:]:
        if line.strip() == "---":
            break
        match = re.match(r"^([A-Za-z0-9_-]+):\s*(.*?)\s*$", line)
        if match:
            values[match.group(1)] = match.group(2).strip("'\"")
    return values


class Tree:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.files = {p for p in git(root, "ls-files", "-z").split("\0") if p}
        self.dirs = {str(parent) for f in self.files for parent in PurePosixPath(f).parents}
        self.areas = area_dirs(root)
        self._texts: dict[str, str] = {}

    def text(self, path: str) -> str:
        if path not in self._texts:
            self._texts[path] = (self.root / path).read_text(encoding="utf-8-sig")
        return self._texts[path]

    def resolves(self, base: str, token: str) -> bool:
        candidate = posixpath.normpath(posixpath.join(base, token)) if base else posixpath.normpath(token)
        return candidate in self.files or candidate in self.dirs

    def markdown(self) -> list[str]:
        return sorted(f for f in self.files if f.endswith(".md") and (self.root / f).is_file())


def root_adrs(tree: Tree) -> dict[str, tuple[str, str]]:
    """ADR number -> (path, status line) for every root ADR."""
    adrs = {}
    for path in tree.markdown():
        name = PurePosixPath(path)
        if str(name.parent) != "docs/adr" or not re.match(r"^\d{4}-", name.name):
            continue
        text = tree.text(path)
        match = STATUS_LINE.search(text)
        status = match.group(1).strip() if match else frontmatter(text).get("status", "")
        adrs[name.name[:4]] = (path, status)
    return adrs


def dead_references(tree: Tree) -> dict[str, str]:
    """Each backticked repository path a maintained document names that exists nowhere, with the
    documents naming it. ADRs are records of their day and are left out (their scripts are a
    warning). A token whose first directory exists at none of the bases is not a repository path
    (an object key, a crate-relative fragment) and is skipped."""
    found: dict[str, list[str]] = {}
    for doc in tree.markdown():
        if "adr" in PurePosixPath(doc).parts:
            continue
        here = str(PurePosixPath(doc).parent)
        bases = ["", here, *tree.areas]
        for token in set(PATH_TOKEN.findall(tree.text(doc))):
            first = token.split("/")[0]
            if not any(tree.resolves(base, first) for base in bases):
                continue
            if not any(tree.resolves(base, token) for base in bases):
                found.setdefault(token, []).append(doc)
    return {token: "named by " + ", ".join(docs) for token, docs in found.items()}


def unindexed_runbooks(tree: Tree) -> set[str]:
    found = set()
    for doc in tree.markdown():
        path = PurePosixPath(doc)
        if path.parent.name != "runbooks" or path.name == "README.md":
            continue
        index = f"{path.parent}/README.md"
        if index not in tree.files:
            continue
        link = re.compile(r"\]\((?:\./)?" + re.escape(path.name) + r"(?:#[^)]*)?\)")
        if not link.search(tree.text(index)):
            found.add(doc)
    return found


def code_cited_adrs(tree: Tree) -> set[str]:
    try:
        out = git(tree.root, "grep", "-I", "-h", "-o", "-E", r"(^|[^A-Za-z-])ADR[- ][0-9]{4}", "--", ".", ":(exclude)*.md")
    except subprocess.CalledProcessError as error:
        if error.returncode == 1:  # no match
            return set()
        raise
    return set(CODE_CITATION.findall(out))


def proposed_adrs_cited_by_code(tree: Tree, adrs: dict[str, tuple[str, str]]) -> set[str]:
    cited = code_cited_adrs(tree)
    return {
        path
        for number, (path, status) in adrs.items()
        if status.lower().startswith("proposed") and number in cited and not MISSING_NOTE.search(tree.text(path))
    }


def unrecorded_supersessions(tree: Tree, adrs: dict[str, tuple[str, str]]) -> set[str]:
    found = set()
    for number, (path, _status) in adrs.items():
        for older in set(SUPERSEDES.findall(tree.text(path))):
            if older in adrs and older != number and not re.search(rf"ADR[- ]{number}\b", adrs[older][1]):
                found.add(f"ADR-{number} supersedes ADR-{older}")
    return found


def findings(tree: Tree) -> dict[str, dict[str, str]]:
    """Check -> {entry: where it was found}."""
    adrs = root_adrs(tree)
    return {
        "dead-reference": dead_references(tree),
        "unindexed-runbook": dict.fromkeys(unindexed_runbooks(tree), ""),
        "proposed-adr-cited-by-code": dict.fromkeys(proposed_adrs_cited_by_code(tree, adrs), ""),
        "supersession-unrecorded": dict.fromkeys(unrecorded_supersessions(tree, adrs), ""),
    }


def warnings(tree: Tree, today: dt.date) -> list[str]:
    out = []
    for number, (path, status) in sorted(root_adrs(tree).items()):
        if not status.lower().startswith(("accepted", "채택")):
            continue
        text = tree.text(path)
        # A dated path-move footnote (`old` → `new`) answers for every mention of the old path.
        moved = {t for line in text.splitlines() if "→" in line for t in re.findall(r"`([^`]+)`", line)}
        for token in sorted(set(re.findall(r"`(scripts/[\w./-]+\.(?:sh|py))`", text)) - moved):
            if not any(tree.resolves(base, token) for base in ["", *tree.areas]):
                out.append(f"{path}: Accepted ADR-{number} names `{token}`, which exists nowhere")
    for doc in tree.markdown():
        meta = frontmatter(tree.text(doc))
        if meta.get("doc_type") != "runbook" or meta.get("status") != "current" or "last_reviewed" not in meta:
            continue
        try:
            reviewed = dt.date.fromisoformat(meta["last_reviewed"])
        except ValueError:
            out.append(f"{doc}: last_reviewed {meta['last_reviewed']!r} is not a YYYY-MM-DD date")
            continue
        age = (today - reviewed).days
        if age > STALE_AFTER_DAYS:
            out.append(f"{doc}: last_reviewed {reviewed} is {age} days old; re-read it against the code")
    return sorted(set(out))


def load_baseline(path: Path) -> dict[str, set[str]]:
    if not path.is_file():
        return {check: set() for check in CHECKS}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {check: set(data.get(check, [])) for check in CHECKS}


def write_baseline(path: Path, entries: dict[str, set[str]]) -> None:
    data = {"$comment": "Shrink-only (scripts/catalog/documentation_ratchets.py). Fix an entry and delete its line; never add one."}
    data.update({check: sorted(entries[check]) for check in CHECKS})
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")


def check(root: Path, baseline_path: Path, today: dt.date) -> tuple[list[str], list[str]]:
    """(errors, warnings)."""
    tree = Tree(root)
    found = findings(tree)
    baseline = load_baseline(baseline_path)
    errors, notes = [], []
    for name in CHECKS:
        errors += [f"{name}: {entry} {found[name][entry]}".rstrip() for entry in sorted(found[name].keys() - baseline[name])]
        notes += [f"{name}: fixed, delete it from the baseline: {entry}" for entry in sorted(baseline[name] - found[name].keys())]
    return errors, notes + warnings(tree, today)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument("--baseline", type=Path, default=BASELINE, help=argparse.SUPPRESS)
    parser.add_argument("--shrink", action="store_true", help="drop baseline entries that are no longer found")
    args = parser.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    if args.shrink:
        found = findings(Tree(args.root))
        baseline = load_baseline(args.baseline)
        write_baseline(args.baseline, {c: baseline[c] & found[c].keys() for c in CHECKS})
        return 0
    errors, notes = check(args.root, args.baseline, dt.date.today())
    for note in notes:
        print(f"warning: documentation-ratchets: {note}", file=sys.stderr)
    for error in errors:
        print(f"FAIL documentation-ratchets: {error}", file=sys.stderr)
    if errors:
        print("    fix the document; the baseline only shrinks (python3 scripts/catalog/documentation_ratchets.py --shrink)", file=sys.stderr)
        return 1
    print(f"OK documentation-ratchets ({len(notes)} warning(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
