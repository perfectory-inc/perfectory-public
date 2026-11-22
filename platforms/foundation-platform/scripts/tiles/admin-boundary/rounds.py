#!/usr/bin/env python3
"""Pick the newest collection round of the legal-dong boundary ZIPs Bronze holds (root ADR-0236).

Each ZIP holds one sido's shapefile whose layer name ends in `_<sido>_<YYYYMM>`
(LSMD_ADM_SECT_UMD_41_202609.shp). Bronze keeps every round side by side, so the round is read from
the members, never from a ZIP's key or upload time. Prints one JSON line:

    {"round": "202609", "files": {"11": "30603-2.zip", ...}}

or `{"round": null, "files": {}}` when no ZIP holds a boundary layer. Whether the round covers every
sido of the official code list is merge.py's check, not this one's: that list lives in a table.
"""

from __future__ import annotations

import json
import re
import sys
import zipfile
from pathlib import Path

MEMBER = re.compile(r"(?:^|/)LSMD_ADM_SECT_UMD_(\d{2})_(\d{6})\.shp$", re.IGNORECASE)


def rounds(zips: list[Path]) -> dict[str, dict[str, list[str]]]:
    """round -> sido -> the ZIP names holding that sido's layer of that round."""

    found: dict[str, dict[str, list[str]]] = {}
    for path in sorted(zips):
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                match = MEMBER.search(name)
                if match:
                    found.setdefault(match.group(2), {}).setdefault(match.group(1), []).append(path.name)
    return found


def newest(zips: list[Path]) -> dict:
    """The newest round and its one ZIP per sido; refuses a round naming a sido twice."""

    found = rounds(zips)
    if not found:
        return {"round": None, "files": {}}
    round_ = max(found)
    twice = sorted(sido for sido, names in found[round_].items() if len(set(names)) > 1)
    if twice:
        raise SystemExit(f"round {round_} has more than one ZIP for sido {twice}; refusing to guess")
    return {"round": round_, "files": {sido: names[0] for sido, names in sorted(found[round_].items())}}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: rounds.py <directory of boundary ZIPs>")
    print(json.dumps(newest(sorted(Path(sys.argv[1]).glob("*.zip")))))
