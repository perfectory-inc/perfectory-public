#!/usr/bin/env python3
"""Merge one collection round of the legal-dong boundary files into the Silver input.

Reads what `convert.sh` wrote (`out/*.geojson`, one per collected zip, EPSG:4326) and keeps only the
files of `--round` (the YYYYMM in each layer name, e.g. LSMD_ADM_SECT_UMD_41_202609): Bronze keeps
every round side by side, and mixing two rounds would serve half the country from each.

Sigungu names come from the official 법정동코드 전체자료 (`--code-list`) when given: a sido that was
renumbered (29/46 -> 12, 2026-07-01) has no name in the older topographic sigungu file. The official
list is also the coverage check: every existing sido must have a file in the round, otherwise the
round is incomplete and nothing is written.

Writes `official-administrative-boundary.geojson` (EMD_CD, EMD_NM, SIGUNGU_CD, SIGUNGU_NM) and
`merge-report.json` with every excluded row and why.
"""

from __future__ import annotations

import argparse
import collections
import glob
import hashlib
import io
import json
import re
import zipfile
from pathlib import Path

LAYER_ROUND = re.compile(r"_(\d{2})_(\d{6})$")


def read_code_list(path: Path) -> dict[str, tuple[str, str]]:
    raw = path.read_bytes()
    if zipfile.is_zipfile(io.BytesIO(raw)):
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            raw = archive.read([n for n in archive.namelist() if not n.endswith("/")][0])
    text = raw.decode("utf-8") if raw[:3] == b"\xef\xbb\xbf" else raw.decode("cp949")
    codes = {}
    for line in text.splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) == 3:
            codes[parts[0].strip()] = (parts[1].strip(), parts[2].strip())
    return codes


def sigungu_names_from_codes(codes: dict[str, tuple[str, str]]) -> dict[str, str]:
    """`1211000000 전남광주통합특별시 목포시` -> {"12110": "목포시"}; the sido name is dropped.

    A single-tier city (세종특별자치시, `3611000000`) has no sigungu under its sido name, so its
    sigungu-level row carries the city name alone, and that name is the sigungu name.
    """

    names = {}
    for code, (name, status) in codes.items():
        if status == "존재" and code.endswith("00000") and not code.endswith("00000000"):
            parts = name.split()
            names[code[:5]] = " ".join(parts[1:]) if len(parts) >= 2 else name
    return names


def sigungu_names_from_topographic(path: Path) -> dict[str, str]:
    names = {}
    if path.exists():
        for feat in json.loads(path.read_text(encoding="utf-8"))["features"]:
            p = feat.get("properties") or {}
            code, name = (p.get("BJCD") or "")[:5], p.get("NAME") or ""
            if len(code) == 5 and name:
                names.setdefault(code, name)
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--round", required=True, help="YYYYMM of the collection round, e.g. 202609")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--code-list", help="법정동코드 전체자료 (.zip/.txt); required when a sido was renumbered")
    args = parser.parse_args()
    if not re.fullmatch(r"\d{6}", args.round):
        raise SystemExit("--round must be YYYYMM")

    codes = read_code_list(Path(args.code_list)) if args.code_list else {}
    names = sigungu_names_from_topographic(Path(args.out_dir) / "sgg-attrs.geojson")
    names.update(sigungu_names_from_codes(codes))

    files_by_sido: dict[str, list[str]] = collections.defaultdict(list)
    for path in sorted(glob.glob(f"{args.out_dir}/30603-*.geojson")):
        layer = json.loads(Path(path).read_text(encoding="utf-8")).get("name", "")
        m = LAYER_ROUND.search(layer)
        if m and m.group(2) == args.round:
            files_by_sido[m.group(1)].append(path)
    if codes:
        sidos = {c[:2] for c, (_, s) in codes.items() if s == "존재" and c.endswith("00000000")}
        missing = sorted(sidos - set(files_by_sido))
        if missing:
            raise SystemExit(f"round {args.round} has no file for sido {missing}; refusing a partial country")
    duplicated = {s: f for s, f in files_by_sido.items() if len(f) > 1}
    if duplicated:
        raise SystemExit(f"round {args.round} has more than one file for sido {sorted(duplicated)}")

    features, excluded = [], collections.defaultdict(list)
    for sido, paths in sorted(files_by_sido.items()):
        for feat in json.loads(Path(paths[0]).read_text(encoding="utf-8"))["features"]:
            p = feat.get("properties") or {}
            emd_cd = (p.get("EMD_CD") or "").strip()
            emd_nm = (p.get("EMD_NM") or "").strip()
            parent = (p.get("COL_ADM_SE") or "").strip()
            if len(emd_cd) != 8 or not emd_cd.isdigit() or not emd_nm or len(parent) != 5:
                excluded["not a legal dong row (malformed code or name)"].append(emd_cd)
                continue
            if codes and codes.get(f"{emd_cd}00", ("", ""))[1] != "존재":
                excluded["code not existing in the official list"].append(emd_cd)
                continue
            parent_nm = names.get(parent)
            if not parent_nm:
                excluded["no sigungu name"].append(emd_cd)
                continue
            feat["properties"] = {"EMD_CD": emd_cd, "EMD_NM": emd_nm, "SIGUNGU_CD": parent, "SIGUNGU_NM": parent_nm}
            features.append(feat)

    data = json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, separators=(",", ":")).encode()
    Path("official-administrative-boundary.geojson").write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    report = {
        "round": args.round,
        "files": {s: Path(p[0]).name for s, p in sorted(files_by_sido.items())},
        "features": len(features),
        "excluded": {reason: {"count": len(v), "sample": v[:20]} for reason, v in excluded.items()},
        "sha256": digest,
    }
    Path("merge-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("round", "features", "sha256")}, ensure_ascii=False))
    print("excluded:", {k: v["count"] for k, v in report["excluded"].items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
