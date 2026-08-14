#!/usr/bin/env python3
"""Render a deterministic repository-wide document inventory."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs/document-catalog.md"
AUDIT_OUTPUT = ROOT / "docs/document-audit.md"

DOC_SUFFIXES = {".md", ".mdx", ".rst", ".adoc", ".json", ".yaml", ".yml"}
ROOT_DOC_NAMES = {
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "THIRD_PARTY_NOTICES.md",
}
OWNER_NAMES = {
    "foundation-platform": "Foundation Platform",
    "identity-platform": "Identity Platform",
    "intelligence-platform": "Intelligence Platform",
    "gongzzang": "Gongzzang 제품",
}


def tracked_paths() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
    )
    paths = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        path = Path(raw.decode("utf-8"))
        if path in {
            OUTPUT.relative_to(ROOT),
            AUDIT_OUTPUT.relative_to(ROOT),
        } or "docs/superpowers" in path.as_posix():
            continue
        if not (ROOT / path).is_file():
            continue
        if path.suffix.lower() not in DOC_SUFFIXES:
            continue
        if "node_modules" in path.parts or "target" in path.parts:
            continue
        if "docs" in path.parts or path.name in ROOT_DOC_NAMES or path.name == "README.md":
            paths.append(path)
    return sorted(set(paths), key=lambda path: path.as_posix().lower())


def owner_for(path: Path) -> str:
    parts = path.parts
    if len(parts) >= 2 and parts[0] == "platforms":
        return OWNER_NAMES.get(parts[1], parts[1])
    if len(parts) >= 2 and parts[0] == "products":
        return OWNER_NAMES.get(parts[1], parts[1])
    if parts[0] == "tools":
        return "Repository tooling"
    if parts[0] == ".github":
        return "Repository CI"
    return "Monorepo"


def type_for(path: Path) -> str:
    name = path.name.lower()
    parts = {part.lower() for part in path.parts}
    if name == "agents.md":
        return "agent rules"
    if name == "readme.md":
        return "README"
    if ".example." in name:
        return "fixture"
    if ".draft." in name:
        return "draft"
    if "adr" in parts:
        return "ADR"
    if ".proof." in name or "evidence" in name:
        return "evidence"
    if "runbooks" in parts:
        return "runbook"
    if "architecture" in parts:
        return "architecture"
    if "roadmap" in parts:
        return "roadmap"
    if "guides" in parts:
        return "guide"
    if "conventions" in parts:
        return "convention"
    if "governance" in parts:
        return "governance"
    if "compliance" in parts:
        return "compliance"
    if "openapi" in parts or "schemas" in parts or "contracts" in parts:
        return "contract"
    if any(part in parts for part in {"catalog", "reference", "data-quality", "db", "events", "observability", "security"}):
        return "reference"
    return "documentation"


def status_for(path: Path) -> str:
    name = path.name.lower()
    parts = {part.lower() for part in path.parts}
    if ".example." in name:
        return "fixture"
    if ".draft." in name:
        return "review required"
    if ".proof." in name or ("evidence" in name and "adr" not in parts):
        return "evidence"
    try:
        text = (ROOT / path).read_text(encoding="utf-8-sig")
    except OSError:
        return "unknown"
    for line in text.splitlines()[:12]:
        if line.strip().lower().startswith("status:"):
            return clean_status(line.split(":", 1)[1].strip().strip("'\""))
        if line.strip().lower().startswith("- status:"):
            return clean_status(line.split(":", 1)[1].strip().strip("'\""))
    return "current"


def clean_status(value: str) -> str:
    return re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", value)


def md(value: str) -> str:
    return (
        value.replace("|", "\\|")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("\n", " ")
        .strip()
    )


def render() -> str:
    paths = tracked_paths()
    owner_counts = Counter(owner_for(path) for path in paths)
    type_counts = Counter(type_for(path) for path in paths)
    by_owner: dict[str, list[Path]] = defaultdict(list)
    for path in paths:
        by_owner[owner_for(path)].append(path)

    lines = [
        "<!-- GENERATED FILE. Do not edit by hand. -->",
        "<!-- Render with: python3 scripts/catalog/render-document-catalog.py -->",
        "",
        "# perfectory 전체 문서 색인",
        "",
        "> 이 문서는 현재 작업 트리의 문서 파일을 자동으로 세어 만든 탐색용 색인입니다. 문서 내용의 정본이 아닙니다.",
        "> 새 문서는 설명하는 코드 또는 책임 영역 가까이에 두고, README에는 이 색인과 정본 문서 링크만 추가합니다.",
        "",
        "## 문서 규모",
        "",
        f"- 문서 파일: **{len(paths)}개**",
        f"- 소유 영역: **{len(owner_counts)}개**",
        "",
        "### 소유 영역별",
        "",
        "| 소유 영역 | 문서 수 |",
        "|---|---:|",
    ]
    for owner in sorted(owner_counts):
        lines.append(f"| {md(owner)} | {owner_counts[owner]} |")
    lines += [
        "",
        "### 유형별",
        "",
        "| 유형 | 문서 수 |",
        "|---|---:|",
    ]
    for kind in sorted(type_counts):
        lines.append(f"| {md(kind)} | {type_counts[kind]} |")

    lines += ["", "## 책임별 문서 트리", ""]
    for owner in sorted(by_owner):
        lines += [f"### {owner}", "", "```text"]
        for path in by_owner[owner]:
            lines.append(path.as_posix())
        lines += ["```", ""]

    lines += [
        "## 전체 문서 목록",
        "",
        "| 경로 | 소유자 | 유형 | 상태 |",
        "|---|---|---|---|",
    ]
    for path in paths:
        lines.append(
            f"| `{md(path.as_posix())}` | {md(owner_for(path))} | "
            f"{md(type_for(path))} | {md(status_for(path))} |"
        )
    lines += [
        "",
        "## 유지 규칙",
        "",
        "1. 이 파일을 직접 편집하지 않습니다. `render-document-catalog.py`가 생성합니다.",
        "2. 계약·fixture·코드가 읽는 경로는 참조를 확인하지 않고 이동하거나 삭제하지 않습니다.",
        "3. 같은 사실을 여러 문서에 복사하지 말고 소유 영역의 SSOT를 링크합니다.",
        "4. `review required`·`evidence` 문서는 현재 운영 지침으로 사용하지 않고 정리 상태를 확인합니다.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            print(f"stale document catalog: {OUTPUT}", file=sys.stderr)
            return 1
        return 0
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8", newline="\n")
    print(OUTPUT.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
