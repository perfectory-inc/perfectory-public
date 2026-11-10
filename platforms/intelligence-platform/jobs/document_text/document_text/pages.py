"""Pages joined into one document text, each page keeping where it sits in it.

A citation is (sha256, page_number, char_start, char_end) and is valid only when that span of
the document text equals the quote, so a value no page says cannot be cited.
"""

from __future__ import annotations

from dataclasses import dataclass

PAGE_SEPARATOR = "\f"


@dataclass(frozen=True)
class Page:
    page_number: int
    char_start: int
    char_end: int
    method: str
    text: str


def assemble(page_texts: list[tuple[str, str]]) -> list[Page]:
    """`page_texts` is (method, text) per page in order; pages are numbered from 1."""
    pages: list[Page] = []
    cursor = 0
    for number, (method, text) in enumerate(page_texts, start=1):
        if number > 1:
            cursor += len(PAGE_SEPARATOR)
        pages.append(Page(number, cursor, cursor + len(text), method, text))
        cursor += len(text)
    return pages


def document_text(pages: list[Page]) -> str:
    return PAGE_SEPARATOR.join(page.text for page in pages)


def cite(pages: list[Page], quote: str, *, start_at: int = 0) -> tuple[int, int, int] | None:
    """The first (page_number, char_start, char_end) at or after `start_at` whose text is `quote`.

    A quote never spans pages: a span crossing a separator is not something one page says.
    """
    if not quote:
        return None
    for page in pages:
        if page.char_end <= start_at:
            continue
        local = page.text.find(quote, max(0, start_at - page.char_start))
        if local >= 0:
            start = page.char_start + local
            return page.page_number, start, start + len(quote)
    return None


def citation_holds(pages: list[Page], page_number: int, char_start: int, char_end: int, quote: str) -> bool:
    for page in pages:
        if page.page_number == page_number:
            if not (page.char_start <= char_start <= char_end <= page.char_end):
                return False
            return page.text[char_start - page.char_start : char_end - page.char_start] == quote
    return False
