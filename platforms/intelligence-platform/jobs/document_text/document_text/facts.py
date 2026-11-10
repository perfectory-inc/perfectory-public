"""Facts from positioned page text by rules, each with the check that makes it a fact (root ADR-0477 §5).

A rule reads one page's lines and proposes values; a value becomes a fact only when its quote
holds on that page (`citation_holds`) and its kind's check passes:

- `land_use_area` (토지이용계획 용지별 면적): the 용지 rows of one table sum to its 합계 row in
  the chosen column, and where the table has a 구성비 column each ratio equals area / total.
  The chosen column is the rightmost one passing both, so a 기정·변경 table yields 변경.
- `invited_industry` (유치업종): the document's KSIC division code is a KSIC-10 division and the
  name written next to it is that division's name (config/ksic-divisions.json).
- `parcel_lot` (토지조서 지번): a numbered row with a 지목 of the cadastral act, its included
  area at most its parcel area, and the rows of the page numbered without a gap. 지번 stays as
  written: place_id is foundation's resolver's (ADR-0477 §2).
- `sale_price` (분양가): a labelled price with its won-per-area unit on the same line.

The table-total check is the one check an OCR'd number can pass by itself (ADR-0477 §5: OCR
numbers were 85 % right); every other OCR'd value needs the vision cross-check in `fact_run`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .pages import Page, citation_holds

KSIC = Path(__file__).resolve().parents[3] / "config" / "ksic-divisions.json"

TOTAL_LABELS = {"계", "합계", "총계", "총면적"}
# 공간정보관리법 시행령 제58조 지목 28종의 부호(약칭)와 정식 이름.
JIMOK = set("전답과목임광염대장학차주창도철제구유양수공체원종사묘잡") | {
    "임야", "대지", "공장용지", "학교용지", "주차장", "주유소용지", "창고용지", "도로", "철도용지", "제방", "구거",
    "유지", "양어장", "수도용지", "공원", "체육용지", "유원지", "종교용지", "사적지", "묘지", "잡종지", "과수원",
    "목장용지", "광천지", "염전", "하천",
}

NUMBER = re.compile(r"^[\d,]*\d(?:\.\d+)?$")
DELTA = re.compile(r"^[감증]\)\s*[\d,]*\d(?:\.\d+)?$")
HEADING = re.compile(r"^\s*(?:\d+(?:의\d+)?\.|[나-하]\.)\s*\S")


@dataclass(frozen=True)
class Fact:
    fact_kind: str
    value: dict
    page_number: int
    char_start: int
    char_end: int
    quote: str
    check: str
    method: str = "rule"
    model: str | None = None


@dataclass(eq=False)
class Line:
    page: Page
    text: str  # stripped
    start: int  # document offset of `text`


def lines_of(page: Page) -> list[Line]:
    out, cursor = [], page.char_start
    for raw in page.text.split("\n"):
        stripped = raw.strip()
        if stripped:
            out.append(Line(page, stripped, cursor + raw.index(stripped)))
        cursor += len(raw) + 1
    return out


def quoted(line: Line, kind: str, value: dict, check: str) -> Fact | None:
    """The fact whose quote is the whole line, if that line is what its page says there."""
    end = line.start + len(line.text)
    if not citation_holds([line.page], line.page.page_number, line.start, end, line.text):
        return None
    return Fact(kind, value, line.page.page_number, line.start, end, line.text, check)


def to_number(token: str) -> float:
    return float(token.replace(",", ""))


def decimals(token: str) -> int:
    return len(token.split(".")[1]) if "." in token else 0


# --- land use -------------------------------------------------------------------------------

# The words a merged 구분 cell prints down the side of its rows: 산업 / 시설 / 용지.
PARENT_WORDS = {"산업", "시설", "용지", "지원", "공공", "주거", "상업", "유통", "복합", "녹지", "기타", "관리", "물류"}


def side_fragment(tokens: list[str]) -> bool:
    """Whether the first word is the merged cell's, not the row's own: alone, or before more words."""
    return bool(tokens) and tokens[0] in PARENT_WORDS and (len(tokens) == 1 or not (number_token(tokens[1]) or tokens[1] == "-" or DELTA.match(tokens[1])))
TOP_LEVEL = re.compile(r"용지|소계|녹지|기타")


@dataclass
class Row:
    line: Line
    tokens: list[str]  # the label's words
    cells: list[str | None]  # number tokens, None for '-'; a delta (감)·증)) cell is None too
    cut: bool = False  # text stopped the cells but more numbers follow: the row is not all read

    @property
    def label(self) -> str:
        return "".join(self.tokens)

    @property
    def own_tokens(self) -> list[str]:
        """The label without a leading side fragment of its group's merged cell."""
        return self.tokens[1:] if len(self.tokens) > 1 and side_fragment(self.tokens) else self.tokens


def number_token(token: str) -> str | None:
    token = token.rstrip("%")
    return token if NUMBER.match(token) else None


def split_row(line: Line) -> Row | None:
    tokens = line.text.split()
    first = next((i for i, t in enumerate(tokens) if number_token(t) or t == "-" or DELTA.match(t)), None)
    if first is None or (first == 0 and tokens[0] == "-"):
        return None  # no numbers, or a page footer "- 20 -"
    cells: list[str | None] = []
    rest = tokens[first:]
    i = 0
    while i < len(rest):
        t = rest[i]
        if number_token(t):
            cells.append(number_token(t))
        elif t == "-" or DELTA.match(t):
            cells.append(None)
        elif t in ("감)", "증)", "감", "증", "(감)", "(증)") and i + 1 < len(rest) and number_token(rest[i + 1]):
            cells.append(None)
            i += 1
        else:
            break  # 비고 text ends the numbers
        i += 1
    cut = any(number_token(t) for t in rest[i:])
    return Row(line, tokens[:first], cells, cut) if cells else None


def category_of(label: str) -> str | None:
    if label in TOTAL_LABELS:
        return "합계"
    match = re.search(r"([가-힣·]+?용지)", label)
    return match.group(1) if match else None


@dataclass
class Table:
    total: Row
    rows: list[Row]
    lines: list[Line]  # every line from the total on, for the names of 소계 groups


def land_use_tables(pages: list[Page]) -> list[Table]:
    """Each run of rows after a 합계 row inside a 토지이용계획 section; a table may go on over a page."""
    tables: list[Table] = []
    inside, current = False, None
    for page in pages:
        for line in lines_of(page):
            compact = line.text.replace(" ", "")
            if "토지이용계획" in compact and "토지이용계획도" not in compact:
                inside, current = True, None
                continue
            if not inside:
                continue
            if current is not None and HEADING.match(line.text) and "토지이용" not in compact:
                inside, current = False, None
                continue
            row = split_row(line)
            if row is not None and category_of(row.label) == "합계" and (current is None or row.label != "계"):
                current = Table(row, [], [])
                tables.append(current)
            elif current is not None:
                current.lines.append(line)
                if row is not None:
                    current.rows.append(row)
    return tables


def close(a: float, b: float, places: int, rows: int) -> bool:
    return abs(a - b) <= 10 ** (-places) * (rows + 1) + 1e-9


Item = tuple[int, list["Item"]]  # a row index and the items under it


def tree(rows: list[Row], j: int, target: str, budget: int = 20_000) -> list[Item] | None:
    """Rows from the start read as top-level items summing to `target`, each item followed by
    the items under it (summing to it, up to three levels: 소계 › 계 › rows); None if no reading.

    Only 용지·소계·녹지·기타 rows stand at the top. A '-' under an item is zero. The search
    backtracks and gives up after `budget` steps, which leaves the table to the model.
    """
    values = [None if r.cells[j] is None else to_number(r.cells[j]) for r in rows]
    places = max([decimals(target)] + [decimals(r.cells[j]) for r in rows if r.cells[j] is not None])
    steps = [budget]

    def walk(i: int, remaining: float, count: int, depth: int) -> tuple[list[Item], int] | None:
        steps[0] -= 1
        if steps[0] < 0:
            return None
        if count >= (2 if depth == 0 else 1) and close(remaining, 0.0, places, count):
            return [], i
        if i >= len(rows) or remaining < -1:
            return None
        row, value = rows[i], values[i]
        if depth == 0 and value is not None and not TOP_LEVEL.search(row.label):
            return None
        value = value or 0.0
        if value > remaining + 1:
            return None
        options: list[tuple[list[Item], int]] = []
        if depth < 2 and value > 0 and i + 1 < len(rows):
            under = walk(i + 1, value, 0, depth + 1)
            if under is not None and under[0]:
                options.append(under)
        if row.label not in ("소계", "계"):
            options.append(([], i + 1))  # a row may stand alone; a 소계 always has rows under it
        for kids, end in options:
            rest = walk(end, remaining - value, count + 1, depth)
            if rest is not None:
                return [(i, kids), *rest[0]], rest[1]
        return None

    found = walk(0, to_number(target), 0, 0)
    return found[0] if found else None


def flatten(items: list[Item]) -> list[int]:
    return [index for i, kids in items for index in (i, *flatten(kids))]


def group_name(table: Table, row: Row, kids: list[Row]) -> str | None:
    """A 소계 row's 용지 name from the label fragments printed beside its children (산업 / 시설 / 용지)."""
    lo = table.lines.index(row.line)
    hi = table.lines.index(kids[-1].line) if kids else lo
    words = []
    for line in table.lines[lo : hi + 2]:
        tokens = line.text.split()
        if side_fragment(tokens):
            words.append(tokens[0])
    name = "".join(dict.fromkeys(words))  # each fragment once, in order
    return name if name.endswith("용지") and len(name) > 2 else None


def land_use_facts(pages: list[Page]) -> tuple[list[Fact], list[int]]:
    """Facts of every table whose rows close on its total, and the pages of tables none closed."""
    facts: list[Fact] = []
    unsolved: list[int] = []
    for table in land_use_tables(pages):
        found = None
        if table.total.cut:
            unsolved.append(table.total.line.page.page_number)
            continue  # a total read only in part would close the sums on the wrong column
        width = len(table.total.cells)
        ratio_columns = [k for k, c in enumerate(table.total.cells) if c is not None and to_number(c) == 100.0]
        rows = [r for r in table.rows if len(r.cells) == width and not r.cut]
        for j in reversed(range(width)):
            target = table.total.cells[j]
            if target is None or j in ratio_columns or to_number(target) <= 100.0:
                continue
            items = tree(rows, j, target)
            if items is None:
                continue
            ratio = next((k for k in reversed(ratio_columns) if all(ratio_holds(rows[i], j, k, target) for i, _ in items if rows[i].cells[j] is not None)), None)
            if ratio_columns and ratio is None:
                continue
            found = (j, ratio, items)
            break
        if found is None:
            unsolved.append(table.total.line.page.page_number)
            continue
        j, k, items = found
        total_m2 = to_number(table.total.cells[j])

        def fact(row: Row, category: str | None, parent: str | None) -> None:
            if not row.tokens:
                return  # a row whose name is on another line: it closes the sum but shows nothing
            value = {
                "category": category,
                "label": " ".join(row.own_tokens if parent else row.tokens),
                "parent": parent,
                "area_m2": to_number(row.cells[j]),
                "ratio_pct": to_number(row.cells[k]) if k is not None and row.cells[k] is not None else None,
                "table_total_m2": total_m2,
                "column": j,
                "columns": width,
            }
            made = quoted(row.line, "land_use_area", value, "table_total")
            if made:
                facts.append(made)

        def emit(level: list[Item], parent: str | None) -> None:
            for i, kids in level:
                row = rows[i]
                if row.cells[j] is None:
                    continue  # a '-' row says nothing to show
                name = category_of(row.label) if row.label not in ("소계", "계") else group_name(table, row, [rows[c] for c in flatten(kids)])
                if row.label not in ("소계", "계") and parent:
                    name = category_of("".join(row.own_tokens))
                fact(row, name, parent)
                emit(kids, name or parent)

        fact(table.total, "합계", None)
        emit(items, None)
    return facts, sorted(set(unsolved))


def ratio_holds(row: Row, j: int, k: int, target: str) -> bool:
    value, ratio = row.cells[j], row.cells[k]
    if value is None or ratio is None:
        return False
    return abs(to_number(value) / to_number(target) * 100 - to_number(ratio)) <= 0.051 + 10 ** (-decimals(ratio)) / 2


def land_use_pages_unread(pages: list[Page], tables: list[Table]) -> list[int]:
    """토지이용계획 pages with 용지 numbers but no total row the rule could split (glued numbers)."""
    with_table = {t.total.line.page.page_number for t in tables}
    out = []
    for page in pages:
        lines = lines_of(page)
        if page.page_number in with_table:
            continue
        if any("토지이용계획" in l.text.replace(" ", "") for l in lines) and sum(1 for l in lines if "용지" in l.text and re.search(r"\d,\d{3}", l.text)) >= 2:
            out.append(page.page_number)
    return out


# --- invited industries -----------------------------------------------------------------------

def load_ksic(path: Path = KSIC) -> dict[str, tuple[str, str]]:
    return {code: (section, name) for code, (section, name) in json.loads(path.read_text(encoding="utf-8"))["divisions"].items()}


def normalized(name: str) -> str:
    name = re.sub(r"[;:].*?제외", "", name)
    return re.sub(r"[^가-힣0-9]", "", name).replace("제조업", "").replace("업", "")


def bigrams(text: str) -> set[str]:
    return {text[i : i + 2] for i in range(len(text) - 1)}


def same_division_name(written: str, official: str) -> bool:
    a, b = bigrams(normalized(written)), bigrams(normalized(official))
    if not a or not b:
        return False
    return 2 * len(a & b) / (len(a) + len(b)) >= 0.6


NAME = r"[가-힣0-9][가-힣0-9 ,·․⋅및]*?"
INDUSTRY_PATTERNS = [
    re.compile(rf"(?P<name>{NAME})\s*(?:[;:]\s*[가-힣 ]+?제외\s*)?\(\s*(?P<section>[A-U])?(?P<code>\d{{2}})\s*\)"),
    re.compile(rf"(?:^|\s)(?P<section>[A-U])(?P<code>\d{{2}})\s+(?P<name>{NAME}(?:업|제조업))(?=$|[\s;:(/])"),
    re.compile(rf"(?:^|\s)(?P<code>\d{{2}})\.\s?(?P<name>{NAME}(?:업|제조업)?)(?=$|[\s;:(/]|\d)"),
]
EXCLUDED = re.compile(r"제외|불허|제한|금지")
# "음료(11), … 폐기물(38)을 제외한 업종" names what is not invited, often lines after the list began.
EXCLUDING = re.compile(r"[을를]\s*제외한|제외\s*업종|불허|제한\s*업종|입주\s*제한")
ITEM_START = re.compile(r"^\s*(?:[◦○●·•※▷\-]|\d+[.)]|[가-하][.)])")


PRIOR = re.compile(r"^\W*기\s*정\s*[:：]")
CURRENT = re.compile(r"^\W*변\s*경\s*[:：]")


def excluded_lines(lines: list[Line]) -> set[int]:
    """Lines whose codes are not the notice's invited industries.

    - A list that ends in an excluding phrase: that line and up to eight above it, back to the
      line that starts the list item (a sample list ran six lines before "…을 제외한업종").
    - A "기정 :" list that a "변경 :" list follows, and a 기정 row of a page that has a 변경: the
      기정 one is the plan being replaced.
    """
    out: set[int] = set()
    changed = any("변경" in line.text for line in lines)
    for i, line in enumerate(lines):
        if changed and "기정" in line.text.split():
            out.add(i)  # a 기정 cell of a 기정·변경 table
        if PRIOR.match(line.text):
            end = next((k for k in range(i + 1, min(len(lines), i + 9)) if CURRENT.match(lines[k].text)), None)
            if end is not None:
                out.update(range(i, end))
        if not EXCLUDING.search(line.text):
            continue
        out.add(i)
        for back in range(i - 1, max(-1, i - 9), -1):
            if ITEM_START.match(lines[back].text) or HEADING.match(lines[back].text):
                if not EXCLUDING.search(lines[back].text):
                    break
            out.add(back)
    return out


def invited_industry_facts(page: Page, ksic: dict[str, tuple[str, str]], seen: set[str]) -> list[Fact]:
    lines = lines_of(page)
    if not any("유치업종" in l.text.replace(" ", "") or "유치시설" in l.text.replace(" ", "") for l in lines):
        return []
    facts: list[Fact] = []
    excluded = excluded_lines(lines)
    for number, line in enumerate(lines):
        if number in excluded:
            continue
        for pattern in INDUSTRY_PATTERNS:
            for match in pattern.finditer(line.text):
                code, name = match.group("code"), match.group("name").strip(" ,·")
                section = match.groupdict().get("section")
                if code in seen or code not in ksic:
                    continue
                tail = line.text[match.end() : match.end() + 6]
                if EXCLUDED.search(tail) and not re.search(r"[;:]\s*[가-힣 ]+제외", match.group(0)):
                    continue  # "C34 를 제외한" lists what is not invited
                official_section, official = ksic[code]
                if section and section != official_section:
                    continue
                if not same_division_name(name, official):
                    continue
                quote = match.group(0).strip()
                start = line.start + line.text.index(quote)
                if not citation_holds([page], page.page_number, start, start + len(quote), quote):
                    continue
                seen.add(code)
                facts.append(Fact("invited_industry", {"ksic_division": code, "ksic_section": official_section, "name_as_written": name, "ksic_name": official}, page.page_number, start, start + len(quote), quote, "ksic_name"))
    return facts


# --- parcel lots (토지조서) --------------------------------------------------------------------

LOT_ROW = re.compile(
    r"^(?P<seq>\d{1,4})\s+(?P<dong>[가-힣]+?(?:\d가)?(?:동|리|가|읍|면))\s+(?P<lot>(?:산\s?)?\d{1,5}(?:-\d{1,4})?)\s+"
    r"(?P<jimok>[가-힣]{1,5})\s+(?P<area>[\d,]*\d(?:\.\d+)?)\s+(?P<included>[\d,]*\d(?:\.\d+)?)(?=\s|$)"
)


def parcel_lot_facts(page: Page) -> list[Fact]:
    lines = lines_of(page)
    if not any(k in l.text.replace(" ", "") for l in lines for k in ("토지조서", "편입면적", "편입면", "지번")):
        return []
    candidates = []
    for line in lines:
        m = LOT_ROW.match(line.text)
        if not m or m.group("jimok") not in JIMOK:
            continue
        area, included = to_number(m.group("area")), to_number(m.group("included"))
        if included > area or area <= 0:
            continue
        candidates.append((int(m.group("seq")), line, m))
    seqs = [seq for seq, _, _ in candidates]
    if len(candidates) < 2 or seqs != list(range(seqs[0], seqs[0] + len(seqs))):
        return []  # a gap means a row the rule did not read, so the page's rows are not all known
    facts = []
    for seq, line, m in candidates:
        value = {
            "row_number": seq,
            "legal_dong_name": m.group("dong"),
            "lot": m.group("lot").replace(" ", ""),
            "land_category": m.group("jimok"),
            "parcel_area_m2": to_number(m.group("area")),
            "included_area_m2": to_number(m.group("included")),
        }
        # The quote ends at the included area: the 소유자·주소 columns after it are a person's.
        quote = m.group(0)
        if citation_holds([page], page.page_number, line.start, line.start + len(quote), quote):
            facts.append(Fact("parcel_lot", value, page.page_number, line.start, line.start + len(quote), quote, "row_sequence"))
    return facts


# --- sale price -------------------------------------------------------------------------------

PRICE = re.compile(
    r"(?P<label>분양가격?|조성원가|분양예정가격?|공급가격)[^\n]{0,30}?(?P<amount>[\d,]*\d(?:\.\d+)?)\s*(?P<unit>천원|만원|원)\s*/\s*(?P<per>㎡|m2|㎥|평)"
)
UNIT = {"원": 1, "천원": 1_000, "만원": 10_000}


def sale_price_facts(page: Page) -> list[Fact]:
    facts = []
    for line in lines_of(page):
        for m in PRICE.finditer(line.text):
            won = to_number(m.group("amount")) * UNIT[m.group("unit")]
            if not 1_000 <= won <= 100_000_000:
                continue
            quote = m.group(0)
            start = line.start + m.start()
            if citation_holds([page], page.page_number, start, start + len(quote), quote):
                facts.append(Fact("sale_price", {"label": m.group("label"), "won_per_unit": won, "per": m.group("per"), "as_written": f"{m.group('amount')}{m.group('unit')}/{m.group('per')}"}, page.page_number, start, start + len(quote), quote, "unit_and_label"))
    return facts


# --- one document -----------------------------------------------------------------------------

@dataclass
class DocumentFacts:
    facts: list[Fact] = field(default_factory=list)
    land_use_unsolved_pages: list[int] = field(default_factory=list)


def rule_facts(pages: list[Page], ksic: dict[str, tuple[str, str]]) -> DocumentFacts:
    out = DocumentFacts()
    seen_codes: set[str] = set()
    land, unsolved = land_use_facts(pages)
    out.facts.extend(land)
    out.land_use_unsolved_pages = sorted(set(unsolved) | set(land_use_pages_unread(pages, land_use_tables(pages))))
    for page in pages:
        out.facts.extend(invited_industry_facts(page, ksic, seen_codes))
        out.facts.extend(parcel_lot_facts(page))
        out.facts.extend(sale_price_facts(page))
    return out


def land_use_rows_close(rows: list[dict]) -> bool:
    """The table-total check over proposed rows (a model's, say): 용지 rows sum to the 합계 row."""
    totals = [r for r in rows if r.get("category") == "합계"]
    parts = [r for r in rows if r.get("category") not in (None, "합계")]
    if len(totals) != 1 or len(parts) < 2:
        return False
    total = totals[0]["area_m2"]
    places = max(decimals(str(r["area_text"])) for r in rows)
    return close(sum(r["area_m2"] for r in parts), total, places, len(parts))
