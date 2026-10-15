#!/usr/bin/env python3
"""Merge one collection round of the legal-dong boundary files into the Silver input.

Reads what `convert.sh` wrote (`out/*.geojson`, one per collected zip, EPSG:4326) and keeps only the
files of `--round` (the YYYYMM in each layer name, e.g. LSMD_ADM_SECT_UMD_41_202609): Bronze keeps
every round side by side, and mixing two rounds would serve half the country from each.

The official code list is the newest snapshot of `reference.legal_dong_code_snapshot` (the region
code record the daily code.go.kr collector fills, root ADR-0143), read the way the administrative
matching gate reads it — never a downloaded file. Sigungu names come from it: a sido that was
renumbered (29/46 -> 12, 2026-07-01) has no name in the older topographic sigungu file. It is also
the coverage check: every existing sido must have a file in the round, otherwise the round is
incomplete and nothing is written.

Runs in the lakehouse Spark container (it reads an Iceberg table), so `--out-dir` is a path the
container sees. Writes `official-administrative-boundary.geojson` (EMD_CD, EMD_NM, SIGUNGU_CD,
SIGUNGU_NM) and `merge-report.json` with every excluded row and why, next to `--out-dir`.
"""

from __future__ import annotations

import argparse
import collections
import glob
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "infra" / "lakehouse" / "spark" / "jobs"))

from lakehouse_engine import apply_catalog_settings, assert_catalog_env, iceberg_packages  # noqa: E402
from legal_dong_code_snapshot_to_reference import newest_snapshot_codes  # noqa: E402

LAYER_ROUND = re.compile(r"_(\d{2})_(\d{6})$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def merge_round(round_: str, out_dir: Path, codes: dict[str, tuple[str, str]]) -> tuple[bytes, dict[str, Any]]:
    """The merged GeoJSON bytes and the report. Refuses a round missing a sido the code list has."""

    if not codes:
        raise SystemExit("the official code list snapshot is empty; refusing to merge without it")
    names = sigungu_names_from_topographic(out_dir / "sgg-attrs.geojson")
    names.update(sigungu_names_from_codes(codes))

    files_by_sido: dict[str, list[str]] = collections.defaultdict(list)
    for path in sorted(glob.glob(str(out_dir / "30603-*.geojson"))):
        layer = json.loads(Path(path).read_text(encoding="utf-8")).get("name", "")
        m = LAYER_ROUND.search(layer)
        if m and m.group(2) == round_:
            files_by_sido[m.group(1)].append(path)
    sidos = {c[:2] for c, (_, s) in codes.items() if s == "존재" and c.endswith("00000000")}
    missing = sorted(sidos - set(files_by_sido))
    if missing:
        raise SystemExit(f"round {round_} has no file for sido {missing}; refusing a partial country")
    duplicated = {s: f for s, f in files_by_sido.items() if len(f) > 1}
    if duplicated:
        raise SystemExit(f"round {round_} has more than one file for sido {sorted(duplicated)}")

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
            if codes.get(f"{emd_cd}00", ("", ""))[1] != "존재":
                excluded["code not existing in the official list"].append(emd_cd)
                continue
            parent_nm = names.get(parent)
            if not parent_nm:
                excluded["no sigungu name"].append(emd_cd)
                continue
            feat["properties"] = {"EMD_CD": emd_cd, "EMD_NM": emd_nm, "SIGUNGU_CD": parent, "SIGUNGU_NM": parent_nm}
            features.append(feat)

    data = json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, separators=(",", ":")).encode()
    report = {
        "round": round_,
        "files": {s: Path(p[0]).name for s, p in sorted(files_by_sido.items())},
        "features": len(features),
        "excluded": {reason: {"count": len(v), "sample": v[:20]} for reason, v in excluded.items()},
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    return data, report


def read_official_codes(args: argparse.Namespace) -> tuple[str | None, dict[str, tuple[str, str]]]:
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env()
    builder = SparkSession.builder.appName("foundation-platform-admin-boundary-merge")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).config("spark.jars.packages", args.iceberg_packages).getOrCreate()
    try:
        namespace, table = args.code_list_table.split(".")
        return newest_snapshot_codes(spark, f"`{args.iceberg_catalog_name}`.`{namespace}`.`{table}`")
    finally:
        spark.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    # No abbreviations: the retired `--code-list <file>` would otherwise name a table called `<file>`.
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    parser.add_argument("--round", required=True, help="YYYYMM of the collection round, e.g. 202609")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--code-list-table", default="reference.legal_dong_code_snapshot", help="namespace.table of the official code list snapshots")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--iceberg-packages", default=iceberg_packages())
    args = parser.parse_args(argv)
    if not re.fullmatch(r"\d{6}", args.round):
        raise SystemExit("--round must be YYYYMM")
    parts = args.code_list_table.split(".")
    if len(parts) != 2 or not all(IDENTIFIER.fullmatch(p) for p in parts) or not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        raise SystemExit("--code-list-table must be namespace.table and --iceberg-catalog-name an identifier")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    snapshot_date, codes = read_official_codes(args)
    out_dir = Path(args.out_dir)
    data, report = merge_round(args.round, out_dir, codes)
    report["code_list"] = {"table": args.code_list_table, "snapshot_date": snapshot_date}
    (out_dir.parent / "official-administrative-boundary.geojson").write_bytes(data)
    (out_dir.parent / "merge-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("round", "features", "sha256", "code_list")}, ensure_ascii=False))
    print("excluded:", {k: v["count"] for k, v in report["excluded"].items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
