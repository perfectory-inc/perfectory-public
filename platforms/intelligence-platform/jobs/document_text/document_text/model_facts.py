"""The local model where the rules failed, and the vision cross-check for OCR'd values (root ADR-0477 §5).

- Land use: a 토지이용계획 page whose table no rule closed is given to the model with the next
  page (a table may go on). The model copies each row's line; a row is kept only when that line
  is what its page says (`cite`) and its area is in it, and the table only when its 용지 rows
  sum to its 합계 row — the same table-total check as the rule's, so an OCR'd page may pass it.
- OCR'd values with no total to check them (an industry code, a lot row): the page is drawn as
  a picture and the vision model must read the same digits there; otherwise the value is dropped.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path

from .facts import Fact, close, decimals, to_number
from .model_port import Port, clean, image_part, parse_json
from .pages import Page, cite

ROW = {
    "type": "object",
    "properties": {
        "page": {"type": "integer"},
        "line": {"type": "string"},
        "name": {"type": "string"},
        "area": {"type": "string"},
        "ratio": {"type": ["string", "null"]},
        "level": {"type": "string", "enum": ["total", "category", "sub"]},
        "parent": {"type": ["string", "null"]},
    },
    "required": ["page", "line", "name", "area", "ratio", "level", "parent"],
    "additionalProperties": False,
}
LAND_USE_SCHEMA = {
    "type": "object",
    "properties": {"rows": {"type": "array", "items": ROW}},
    "required": ["rows"],
    "additionalProperties": False,
}
SYSTEM = (
    "너는 한국 산업단지 고시문의 표를 읽는다. 문서에 없는 값은 만들지 않는다. "
    "각 행의 line 에는 그 행이 있는 줄을 문서에서 글자 그대로 옮기고, area 에는 그 줄에 적힌 면적 숫자를 그대로 옮긴다."
)


def land_use_prompt(pages: list[Page]) -> str:
    shown = "\n\n".join(f"[쪽 {p.page_number}]\n{clean(p.text)}" for p in pages)
    return (
        "아래 쪽의 토지이용계획 표에서 행을 뽑아라. 기정·변경이 함께 있으면 변경(변경 후)의 면적과 구성비만.\n"
        "- level: 합계 행은 total, 이름이 '용지'로 끝나는 구분(산업시설용지·복합용지·지원시설용지·공공시설용지 등)은 모두 category, "
        "그 아래 세부(도로·공원 등)는 sub.\n"
        "- name: 구분 이름(소계 행이면 그것이 속한 용지 이름). parent: sub 이면 그 위 category 의 name, 아니면 null.\n"
        "- page: 그 줄이 있는 쪽 번호. 표가 없으면 rows 를 비워라.\n\n" + shown
    )


def checked_land_use(rows: list[dict], pages: dict[int, Page], model: str) -> list[Fact]:
    """The model's rows that cite and close, as facts; [] when the table does not close."""
    cited = []
    for row in rows:
        page = pages.get(row.get("page"))
        line, area = str(row.get("line") or "").strip(), str(row.get("area") or "").strip()
        if page is None or not line or not area or area not in line or not re.fullmatch(r"[\d,]*\d(?:\.\d+)?", area):
            continue
        at = cite([page], line)
        if at is None:
            continue
        cited.append((row, page, at, to_number(area), area))
    totals = [c for c in cited if c[0]["level"] == "total"]
    if len(totals) != 1:
        return []
    total = totals[0][3]
    places = max(decimals(c[4]) for c in cited)
    # The model's own levels first; then every 용지 row as a category (a sample answer put
    # 복합용지 and 지원시설용지 under 산업시설용지). Either reading must close on the total.
    readings = [
        [c for c in cited if c[0]["level"] == "category"],
        [c for c in cited if c[0]["level"] != "total" and str(c[0].get("name", "")).replace(" ", "").endswith("용지")],
    ]
    categories = next((r for r in readings if len(r) >= 2 and close(sum(c[3] for c in r), total, places, len(r))), None)
    if categories is None:
        return []
    for c in categories:
        c[0]["level"], c[0]["parent"] = "category", None
    for row, _, _, area, _ in categories:
        ratio = row.get("ratio")
        if ratio and re.fullmatch(r"\d+(?:\.\d+)?", ratio.strip()) and abs(area / total * 100 - float(ratio)) > 0.051 + 10 ** (-decimals(ratio)) / 2:
            return []
    keep = totals + categories
    for parent_row, _, _, parent_area, _ in categories:
        subs = [c for c in cited if c[0]["level"] == "sub" and c[0].get("parent") == parent_row["name"]]
        if subs and close(sum(c[3] for c in subs), parent_area, places, len(subs)):
            keep.extend(subs)
    facts = []
    for row, page, (number, start, end), area, _ in keep:
        level = row["level"]
        ratio = row.get("ratio")
        value = {
            "category": "합계" if level == "total" else (row["name"] if level == "category" else None),
            "label": row["name"],
            "parent": row.get("parent") if level == "sub" else None,
            "area_m2": area,
            "ratio_pct": float(ratio) if ratio and re.fullmatch(r"\d+(?:\.\d+)?", ratio.strip()) else None,
            "table_total_m2": total,
        }
        facts.append(Fact("land_use_area", value, number, start, end, page.text[start - page.char_start : end - page.char_start], "table_total", "local_llm", model))
    return facts


def land_use_by_model(port: Port, pages: list[Page], unsolved: list[int], stats: dict) -> list[Fact]:
    by_number = {p.page_number: p for p in pages}
    facts: list[Fact] = []
    for number in unsolved:
        shown = [by_number[n] for n in (number, number + 1) if n in by_number]
        try:
            content, seconds = port.chat(
                [{"role": "system", "content": SYSTEM}, {"role": "user", "content": land_use_prompt(shown)}],
                schema=LAND_USE_SCHEMA,
            )
        except Exception as error:  # the model being away is a counted miss, not the run
            stats["model_errors"] = stats.get("model_errors", 0) + 1
            stats.setdefault("model_error_examples", []).append(f"{type(error).__name__}: {error}"[:200])
            continue
        stats["model_calls"] = stats.get("model_calls", 0) + 1
        stats["model_seconds"] = round(stats.get("model_seconds", 0.0) + seconds, 1)
        answer = parse_json(content) or {}
        got = checked_land_use(answer.get("rows") or [], {p.page_number: p for p in shown}, port.model)
        stats["model_tables_closed" if got else "model_tables_rejected"] = stats.get("model_tables_closed" if got else "model_tables_rejected", 0) + 1
        facts.extend(got)
    return facts


# --- vision cross-check -----------------------------------------------------------------------

VISION_SCHEMA = {
    "type": "object",
    "properties": {"seen": {"type": "string"}},
    "required": ["seen"],
    "additionalProperties": False,
}


def page_png(pdf: Path, page_number: int, timeout: int) -> bytes:
    """The page drawn at 150 dpi by Ghostscript, which ocrmypdf already brings into the image."""
    with tempfile.TemporaryDirectory(prefix="document-fact-") as tmp:
        out = Path(tmp) / "page.png"
        subprocess.run(
            ["gs", "-q", "-dSAFER", "-dBATCH", "-dNOPAUSE", "-sDEVICE=png16m", "-r150",
             f"-dFirstPage={page_number}", f"-dLastPage={page_number}", f"-sOutputFile={out}", str(pdf)],
            check=True, timeout=timeout, capture_output=True,
        )
        return out.read_bytes()


def digits(text: str) -> str:
    return re.sub(r"[^0-9]", "", text)


def vision_confirms(port: Port, vision_model: str, png: bytes, fact: Fact) -> bool:
    question = (
        "이 쪽 그림에서 아래 글과 같은 자리의 글을 그림에 보이는 그대로 옮겨라. 숫자와 코드는 그림의 것을 한 자씩 읽어라. "
        f"그런 글이 없으면 빈 글자로 답하라.\n글: {fact.quote}"
    )
    content, _ = port.chat(
        [{"role": "user", "content": [{"type": "text", "text": question}, image_part(png)]}],
        model=vision_model, schema=VISION_SCHEMA, max_tokens=512,
    )
    seen = (parse_json(content) or {}).get("seen", "")
    return bool(digits(fact.quote)) and digits(seen) == digits(fact.quote)
