#!/usr/bin/env python3
"""Audit repository documentation without changing its source files."""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs/document-audit.md"
REQUIRED_METADATA = ("status", "owner", "doc_type", "last_reviewed")
LEGAL_NAMES = {"LICENSE", "LICENSE.md", "LICENSE.txt", "THIRD_PARTY_NOTICES.md"}
NARRATIVE_SUFFIXES = {".md", ".mdx", ".rst", ".adoc"}
ENGLISH_SENTENCE_STARTS = (
    "The ", "This ", "These ", "When ", "If ", "For ", "To ", "Use ",
    "Run ", "Before ", "After ", "Only ", "Each ", "A ", "An ", "Do ",
    "Never ", "Keep ", "Ensure ", "With ", "Where ", "Once ", "It ",
    "By ", "As ", "All ", "No ",
    "Consumers ", "Producers ", "Partition ", "Runtime ", "Canonical ",
    "Dynamic ", "Static ", "Artifact ", "Request ", "Every ", "Any ",
)
ENGLISH_PROSE_WORDS = {
    "the", "this", "these", "that", "those", "and", "or", "but", "for", "from", "with",
    "without", "into", "through", "before", "after", "when", "while", "where", "which",
    "who", "what", "must", "should", "may", "can", "cannot", "only", "every", "each",
    "all", "any", "one", "two", "three", "new", "old", "use", "uses", "used", "keep",
    "keeps", "require", "requires", "required", "return", "returns", "read", "write", "writes",
    "select", "selects", "contains", "include", "includes", "remain", "remains", "does", "not",
    "is", "are", "was", "were", "be", "being", "has", "have", "will", "would", "because",
    "so", "than", "then", "also", "only", "same", "different", "each", "their", "its",
    "product", "products", "service", "services", "consume", "consumes", "published", "owner",
    "data", "contract", "write", "writes", "read", "reads", "state", "source", "system",
}
INTENTIONAL_DUPLICATE_REASONS = {
    "agents.md": "루트 규칙을 영역별로 상속하는 에이전트 라우터",
    "claude.md": "루트 규칙을 영역별 도구에 연결하는 라우터",
    "glossary.md": "전역 용어집과 제품 도메인 용어집의 책임 분리",
    "pipeline-graph.v1.json": "Foundation 카탈로그 정본과 공개 OpenAPI 계약의 분리",
    "traffic-auth-policy-registry.v1.json": "Foundation·Gongzzang이 각자 소유하는 정책 계약",
}


def load_catalog_module() -> ModuleType:
    path = ROOT / "scripts/catalog/render-document-catalog.py"
    spec = importlib.util.spec_from_file_location("render_document_catalog", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_frontmatter(text: str) -> dict[str, str]:
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


def prose_only(text: str) -> str:
    lines = text.splitlines()
    if lines and lines[0].strip() == "---":
        for index in range(1, len(lines)):
            if lines[index].strip() == "---":
                lines = lines[index + 1 :]
                break
    in_fence = False
    prose: list[str] = []
    for line in lines:
        if line.strip().startswith("```") or line.strip().startswith("~~~"):
            in_fence = not in_fence
            continue
        if not in_fence:
            prose.append(line)
    value = "\n".join(prose)
    value = re.sub(r"<!--.*?-->", " ", value, flags=re.DOTALL)
    value = re.sub(r"`[^`]*`", " ", value)
    value = re.sub(r"\[[^\]]*\]\([^)]*\)", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    return value


def classify_language(text: str) -> str:
    prose = prose_only(text)
    hangul = len(re.findall(r"[가-힣]", prose))
    latin = len(re.findall(r"[A-Za-z]", prose))
    if hangul == 0 and latin == 0:
        return "none"
    if hangul == 0:
        return "english"
    if latin == 0 or latin < hangul * 1.2:
        return "korean"
    return "mixed"


def duplicate_basenames(paths: list[Path]) -> dict[str, list[Path]]:
    grouped: defaultdict[str, list[Path]] = defaultdict(list)
    for path in paths:
        if path.name.lower() == "readme.md":
            continue
        grouped[path.name.lower()].append(path)
    return {
        name: values
        for name, values in sorted(grouped.items())
        if len(values) > 1 and name not in INTENTIONAL_DUPLICATE_REASONS
    }


def intentional_duplicate_basenames(paths: list[Path]) -> dict[str, list[Path]]:
    grouped: defaultdict[str, list[Path]] = defaultdict(list)
    for path in paths:
        name = path.name.lower()
        if name in INTENTIONAL_DUPLICATE_REASONS:
            grouped[name].append(path)
    return {
        name: values
        for name, values in sorted(grouped.items())
        if len(values) > 1
    }


def metadata_status(path: Path, text: str) -> str:
    """Return metadata audit status, respecting ADR and machine-contract rules."""
    narrative = prose_only(text)
    if path.suffix.lower() not in NARRATIVE_SUFFIXES:
        return "not applicable: machine contract"
    if path.name in LEGAL_NAMES:
        return "not applicable: legal text"
    if ".draft." in path.name.lower() or re.search(
        r"^\s*(?:status|상태)\s*$|^\s*(?:[-|]\s*)?(?:\*\*)?(?:status|상태)(?:\*\*)?\s*[:|]\s*draft\b",
        narrative,
        flags=re.IGNORECASE | re.MULTILINE,
    ):
        return "not applicable: draft"
    if path.name.lower() != "readme.md" and "adr" in {part.lower() for part in path.parts} and re.search(
        r"^\s*(?:[-|]\s*)?(?:\*\*)?(?:status|상태)(?:\*\*)?\s*[:|]|^##+\s+(?:status|상태)\s*$",
        narrative,
        flags=re.IGNORECASE | re.MULTILINE,
    ):
        return "not applicable: ADR fields"
    if path.name.lower() in {"agents.md", "claude.md"}:
        return "not applicable: agent router"
    frontmatter = parse_frontmatter(text)
    missing = [key for key in REQUIRED_METADATA if key not in frontmatter]
    return "ok" if not missing else "missing: " + ", ".join(missing)


def local_targets(source: Path, text: str, known: set[Path]) -> list[Path]:
    targets: list[Path] = []
    for raw in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
        target = raw.strip().split("#", 1)[0].split("?", 1)[0]
        if not target or target.startswith(("#", "http:", "https:", "mailto:")):
            continue
        candidate = (ROOT / target.lstrip("/")) if target.startswith("/") else (ROOT / source.parent / target)
        candidate = candidate.resolve()
        try:
            relative = candidate.relative_to(ROOT).as_posix()
        except ValueError:
            continue
        normalised = Path(relative)
        if normalised in known:
            targets.append(normalised)
    return targets


def broken_local_links(source: Path, text: str) -> list[str]:
    """Return relative Markdown links whose target does not exist."""
    broken: list[str] = []
    for raw in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
        target = raw.strip().split("#", 1)[0].split("?", 1)[0]
        if not target or target.startswith(("#", "http:", "https:", "mailto:")):
            continue
        candidate = (ROOT / target.lstrip("/")) if target.startswith("/") else (ROOT / source.parent / target)
        if not candidate.exists():
            broken.append(raw.strip())
    return broken


def policy_violations() -> list[tuple[Path, str]]:
    """Find pseudo-path references and broken local Markdown links."""
    catalog = load_catalog_module()
    output_paths = {
        REPORT.relative_to(ROOT),
        catalog.OUTPUT.relative_to(ROOT),
    }
    paths = [path for path in catalog.tracked_paths() if path not in output_paths]
    violations: list[tuple[Path, str]] = []
    for path in paths:
        text = (ROOT / path).read_text(encoding="utf-8-sig")
        if path.name.lower() != "claude.md":
            for match in re.finditer(r"@(?:docs/|README)", text, flags=re.IGNORECASE):
                violations.append((path, f"비표준 문서 참조: {match.group(0)}"))
        for target in broken_local_links(path, text):
            violations.append((path, f"깨진 상대 링크: {target}"))
    return violations


def audit_rows() -> list[dict[str, object]]:
    catalog = load_catalog_module()
    output_paths = {
        REPORT.relative_to(ROOT),
        catalog.OUTPUT.relative_to(ROOT),
    }
    paths = [path for path in catalog.tracked_paths() if path not in output_paths]
    known = set(paths)
    inbound = Counter()
    texts: dict[Path, str] = {}
    for path in paths:
        text = (ROOT / path).read_text(encoding="utf-8-sig")
        texts[path] = text
        inbound.update(local_targets(path, text, known))
    rows: list[dict[str, object]] = []
    for path in paths:
        rows.append(
            {
                "path": path,
                "owner": catalog.owner_for(path),
                "doc_type": catalog.type_for(path),
                "status": catalog.status_for(path),
                "language": classify_language(texts[path]),
                "metadata": metadata_status(path, texts[path]),
                "inbound": inbound[path],
            }
        )
    return rows


def human_language_violations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return maintained narrative documents that contain no Korean prose."""
    return [
        row
        for row in rows
        if row["language"] == "english" and row["metadata"] == "ok"
    ]


def english_sentence_violations(rows: list[dict[str, object]]) -> list[tuple[Path, int, str]]:
    """Find full English narrative sentences/paragraphs in maintained Markdown.

    Technical identifiers, tables, quotes, and fenced examples are excluded. A
    paragraph is prose when it has enough ordinary English function words and no
    Hangul; this catches English bullets even when they do not start with a fixed
    sentence prefix.
    """
    violations: list[tuple[Path, int, str]] = []
    for row in rows:
        if row["metadata"] != "ok":
            continue
        path = row["path"]
        assert isinstance(path, Path)
        text = (ROOT / path).read_text(encoding="utf-8-sig")
        in_fence = False
        in_frontmatter = text.splitlines()[:1] == ["---"]
        paragraph: list[tuple[int, str]] = []

        def flush_paragraph() -> None:
            if not paragraph:
                return
            number, raw = paragraph[0]
            raw_candidate = " ".join(item.strip() for _, item in paragraph)
            candidate = raw_candidate
            candidate = re.sub(r"^(?:[-*+]\s+|\d+[.)]\s+|>\s+)+", "", candidate)
            candidate = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", candidate)
            code_heavy = len(re.findall(r"`[^`]*`|https?://\S+|[A-Za-z0-9_./:-]+\.(?:rs|ts|tsx|md|json|yaml|yml|toml|sql)", candidate))
            candidate = re.sub(r"`[^`]*`", "", candidate)
            candidate = re.sub(r"[*_]+", "", candidate)
            if candidate.startswith(("**Files", "Files:", "Files ")):
                return
            words = re.findall(r"[A-Za-z]+(?:'[A-Za-z]+)?", candidate)
            lowered = {word.lower() for word in words}
            common = len(lowered & ENGLISH_PROSE_WORDS)
            ascii_letters = sum(char.isalpha() and ord(char) < 128 for char in candidate)
            starts_like_english = candidate.startswith(ENGLISH_SENTENCE_STARTS)
            sentence_like = candidate.rstrip().endswith((".", "!", "?", ":")) or (starts_like_english and len(words) >= 14)
            is_prose = (
                len(words) >= 10
                and common >= 3
                and ascii_letters >= 24
                and not re.search(r"[가-힣]", candidate)
                and (starts_like_english or common >= 5)
                and sentence_like
            )
            if is_prose and (code_heavy == 0 or len(words) >= 18) and len(re.findall(r"[/=:{}<>]", candidate)) <= max(8, len(words) // 2):
                violations.append((path, number, raw.strip()))

        for number, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if in_frontmatter:
                if number > 1 and stripped == "---":
                    in_frontmatter = False
                continue
            if stripped.startswith(("```", "~~~")):
                flush_paragraph()
                paragraph.clear()
                in_fence = not in_fence
                continue
            if in_fence or not stripped or stripped.startswith(("|", ">", "#")):
                flush_paragraph()
                paragraph.clear()
                continue
            paragraph.append((number, stripped))
        flush_paragraph()
    return violations


def review_reference_violations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return proposed/draft documents that have no inbound documentation reference."""
    return [
        row
        for row in rows
        if (
            str(row["status"]).lower() in {"proposed", "review required"}
            or ".draft." in str(row["path"]).lower()
        )
        and int(row["inbound"]) == 0
    ]


def render(rows: list[dict[str, object]]) -> str:
    language_counts = Counter(str(row["language"]) for row in rows)
    metadata_counts = Counter(
        "ok" if row["metadata"] == "ok" else
        "not_applicable" if str(row["metadata"]).startswith("not applicable:") else
        "missing"
        for row in rows
    )
    human_english = sum(
        1
        for row in rows
        if row["language"] == "english" and row["metadata"] == "ok"
    )
    human_mixed = sum(
        1
        for row in rows
        if row["language"] == "mixed" and row["metadata"] == "ok"
    )
    english_sentences = english_sentence_violations(rows)
    review_orphans = review_reference_violations(rows)
    paths = [row["path"] for row in rows]
    duplicates = duplicate_basenames(paths)  # type: ignore[arg-type]
    intentional_duplicates = intentional_duplicate_basenames(paths)  # type: ignore[arg-type]
    lines = [
        "<!-- GENERATED FILE. Do not edit by hand. -->",
        "<!-- Render with: python3 scripts/catalog/audit-documentation.py --write -->",
        "",
        "# perfectory 문서 감사 보고서",
        "",
        "> 이 파일은 문서 정리 작업용 자동 보고서입니다. 문서의 정본이 아닙니다.",
        "",
        "## 요약",
        "",
        f"- 감사 문서: **{len(rows)}개**",
        f"- 언어 분류: **{dict(sorted(language_counts.items()))}**",
        f"- 한글화 대상 유지 문서: **영문 전용 {human_english}개 / 혼합 표기 {human_mixed}개** (기계 계약·라우터·법률 예외 제외)",
        f"- 유지 문서의 명백한 영문 문장: **{len(english_sentences)}개**",
        f"- 메타데이터: **{metadata_counts['ok']}개 정상 / {metadata_counts['missing']}개 누락 / {metadata_counts['not_applicable']}개 해당 없음**",
        f"- 중복 파일명 후보: **{len(duplicates)}개**",
        f"- 링크·참조 위반: **{len(policy_violations())}개**",
        f"- 승인 전 문서 유입 링크 0건: **{len(review_orphans)}개**",
        "",
        "## 언어·메타데이터별 목록",
        "",
        "| 경로 | 소유자 | 유형 | 상태 | 언어 | 메타데이터 | 유입 링크 |",
        "|---|---|---|---|---|---|---:|",
    ]
    for row in rows:
        path = row["path"].as_posix()  # type: ignore[union-attr]
        values = [
            f"`{path}`",
            str(row["owner"]),
            str(row["doc_type"]),
            str(row["status"]),
            str(row["language"]),
            str(row["metadata"]),
            str(row["inbound"]),
        ]
        lines.append("| " + " | ".join(value.replace("|", "\\|") for value in values) + " |")
    lines += ["", "## 중복 파일명 후보", ""]
    if duplicates:
        for name, values in duplicates.items():
            lines.append(f"- `{name}`")
            lines.extend(f"  - `{value.as_posix()}`" for value in values)
    else:
        lines.append("- 없음")
    lines += [
        "",
        "## 해석 규칙",
        "",
        "- `korean`: 한글 설명 중심",
        "- `english`: 한글 문장이 없는 영문 설명 중심",
        "- `mixed`: 한글 설명과 원래 표기를 보존한 기술 표현이 함께 존재",
        "- 코드 블록·명령어·URL·식별자는 언어 판정에서 제외",
        "- README 파일명 중복은 영역별 진입점이므로 중복 후보에서 제외",
        "- `CLAUDE.md`의 도구 전용 `@AGENTS.md` 지시는 허용",
        "",
    ]
    lines += ["## 명백한 영문 서술 문장", ""]
    if english_sentences:
        lines.extend(
            f"- `{path.as_posix()}:{line}` — {sentence}"
            for path, line, sentence in english_sentences
        )
    else:
        lines.append("- 없음")
    lines.append("")
    violations = policy_violations()
    lines += ["## 링크·참조 위반", ""]
    if violations:
        lines.extend(f"- `{path.as_posix()}` — {message}" for path, message in violations)
    else:
        lines.append("- 없음")
        lines.append("")
    lines.extend(["## 의도적으로 범위를 나눈 같은 파일명", ""])
    if intentional_duplicates:
        for name, duplicate_paths in intentional_duplicates.items():
            lines.append(f"- `{name}` — {INTENTIONAL_DUPLICATE_REASONS[name]}")
            for path in duplicate_paths:
                lines.append(f"  - `{path.as_posix()}`")
    else:
        lines.append("- 없음")
    lines.append("")
    lines.extend(["## 승인 전 문서 참조 위반", ""])
    if review_orphans:
        lines.extend(
            f"- `{row['path'].as_posix()}` — 제안/초안 문서는 README·설계 문서에서 참조되어야 합니다."
            for row in review_orphans
        )
    else:
        lines.append("- 없음")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    rendered = render(audit_rows())
    if args.strict:
        rows = audit_rows()
        failures = [row for row in rows if row["metadata"] != "ok"]
        if failures:
            print(f"documentation metadata missing in {len(failures)} file(s)", file=sys.stderr)
            return 1
    if args.check:
        if not REPORT.exists() or REPORT.read_text(encoding="utf-8") != rendered:
            print(f"stale document audit: {REPORT}", file=sys.stderr)
            return 1
        violations = policy_violations()
        if violations:
            for path, message in violations:
                print(f"documentation policy violation: {path}: {message}", file=sys.stderr)
            return 1
        language_failures = human_language_violations(audit_rows())
        if language_failures:
            for row in language_failures:
                print(
                    f"documentation language violation: {row['path']}: 유지 문서는 한글 설명이 필요합니다.",
                    file=sys.stderr,
                )
            return 1
        sentence_failures = english_sentence_violations(audit_rows())
        if sentence_failures:
            for path, line, _sentence in sentence_failures:
                print(
                    f"documentation language violation: {path}:{line}: 영문 서술 문장은 한글로 작성해야 합니다.",
                    file=sys.stderr,
                )
            return 1
        review_orphans = review_reference_violations(audit_rows())
        if review_orphans:
            for row in review_orphans:
                print(
                    f"unreferenced proposed/draft document: {row['path']}",
                    file=sys.stderr,
                )
            return 1
        return 0
    if args.write or not args.check:
        REPORT.write_text(rendered, encoding="utf-8", newline="\n")
        print(REPORT.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
