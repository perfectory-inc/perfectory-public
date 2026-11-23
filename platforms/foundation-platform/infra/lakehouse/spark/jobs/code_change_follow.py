"""What a 법정동 code change reaches (root ADR-0329 §3, ADR-0416): the PNU prefixes under every code
that differs between two reads of `reference.legal_dong_code_change`.

The Silver follows that re-link rows under those prefixes run in `DuckDB` inside the publisher now
(`data_go_kr_file_data_stage::changed_prefixes`); what is left here is the Gold reader's
(`place_id_release_to_gold.code_moved_scopes`).
"""

from __future__ import annotations

from typing import Any, Iterable

# The change table's columns a change is read by (the Gold build's, `views.changed_codes`).
CHANGE_COLUMNS = ("kind", "old_code", "new_code", "level")


def prefixes_from_changes(before: Iterable[dict[str, Any]], after: Iterable[dict[str, Any]]) -> set[str]:
    """The PNU prefixes under every code that differs between two reads of the change table."""

    import legal_dong_code_change_views as views  # noqa: PLC0415

    return {views.code_prefix(code) for code in views.changed_codes(before, after, CHANGE_COLUMNS)}
