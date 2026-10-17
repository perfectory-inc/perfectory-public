#!/usr/bin/env python3
"""Synthetic, production-shaped Silver inputs for the building panel Gold build (spike, ADR-0192).

Every value is synthetic: PNUs sit in the reserved 99999 prefix, register keys in an `S9-` namespace.
Row counts scale from the 2026-10 Silver tables (titles 7.6e6, units 1.98e7, areas 1.14e8,
prices 2e8 at --scale 1); floors are assumed ~3 per title. Skew follows the real shape: a few
percent of titles are apartment blocks holding almost every unit, grouped several to one parcel.
Columns are the lakehouse contract's (the production Spark job refuses a missing one); columns the
build never reads hold short constants.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

JOBS = Path(__file__).resolve().parents[3] / "platforms/foundation-platform/infra/lakehouse/spark/jobs"
sys.path.insert(0, str(JOBS))
from platform_contracts import load_lakehouse_contract  # noqa: E402

FULL = {"titles": 7_600_000}
ARROW = {"string": pa.string(), "double": pa.float64(), "int": pa.int32(), "long": pa.int64(),
         "timestamp": pa.timestamp("us", tz="UTC")}
USAGE = ["아파트", "근린생활시설", "계단실", "주차장", "복도", "기계실", "업무시설", "공용"]
STRUCTURE = ["철근콘크리트구조", "철골구조", "벽돌구조", "철골철근콘크리트구조"]


def s(values):
    return pa.array(values, pa.string())


def cat(*parts):
    return pc.binary_join_element_wise(*parts, "")


def num(a, width=0):
    text = pc.cast(pa.array(a), pa.string())
    return pc.utf8_lpad(text, width, "0") if width else text


def pnu_of(index):
    # 99999 + 5-digit block + land kind 1 + 8-digit lot: matches the gateway's PNU pattern.
    index = np.asarray(index)
    return cat("99999", num(index // 100_000, 5), "1", num(index % 100_000, 8))


def nullify(array, mask):
    return pc.if_else(pa.array(mask), pa.scalar(None, array.type), array)


def write(out: Path, table_name: str, columns: dict, rows: int):
    contract = load_lakehouse_contract(f"silver.{table_name}")
    arrays, fields = [], []
    for col in contract["columns"]:
        name, kind = col["name"], col["logical_type"]
        value = columns.get(name)
        if value is None:
            value = pa.nulls(rows, ARROW[kind]) if not col.get("required") else (
                pa.array(np.zeros(rows, dtype="datetime64[us]"), ARROW[kind]) if kind == "timestamp"
                else pa.array(np.zeros(rows), ARROW[kind]) if kind != "string" else
                pa.repeat(pa.scalar("synthetic", pa.string()), rows))
        arrays.append(pa.array(value).cast(ARROW[kind]))
        fields.append(pa.field(name, ARROW[kind]))
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    target = out / table_name
    target.mkdir(parents=True, exist_ok=True)
    per_file = 2_000_000
    for i, start in enumerate(range(0, max(rows, 1), per_file)):
        pq.write_table(table.slice(start, per_file), target / f"part-{i:05d}.parquet",
                       compression="zstd", row_group_size=500_000)
    print(f"{table_name}: {rows:,} rows", flush=True)


def generate(out: Path, scale: float, seed: int, with_prices: bool):
    rng = np.random.default_rng(seed)
    n_t = int(FULL["titles"] * scale)

    # Titles: 6% apartment blocks grouped ~8 to a parcel; the rest on a shared parcel pool.
    apartment = rng.random(n_t) < 0.06
    n_apt = int(apartment.sum())
    complexes = max(n_apt // 8, 1)
    pool = int(n_t * 0.8)
    title_pnu_index = np.where(apartment, pool + rng.integers(0, complexes, n_t), rng.integers(0, pool, n_t))
    title_pk = cat("S9-T", num(np.arange(n_t), 9))
    ground = rng.integers(1, 30, n_t).astype(np.int32)
    titles = {
        "title_row_id": cat("TR", num(np.arange(n_t), 9)),
        "mgm_bldrgst_pk": title_pk,
        "pnu": nullify(pnu_of(title_pnu_index), rng.random(n_t) < 0.003),
        "purpose_code_raw": s(rng.choice(["02000", "01000", "03000", " ", "14000"], n_t)),
        "structure_code_raw": s(rng.choice(["21", "11", "31", ""], n_t)),
        "floor_area_m2": nullify(pa.array(np.round(rng.uniform(0, 90_000, n_t), 2)), rng.random(n_t) < 0.02),
        "ground_floor_count": nullify(pa.array(ground), rng.random(n_t) < 0.01),
        "basement_floor_count": nullify(pa.array(rng.integers(0, 6, n_t).astype(np.int32)), rng.random(n_t) < 0.05),
        "approval_year": nullify(pa.array(rng.integers(1960, 2026, n_t).astype(np.int32)), rng.random(n_t) < 0.04),
        "source_snapshot_id": pa.repeat(pa.scalar("synthetic-titles-1"), n_t),
    }
    write(out, "building_register_titles", titles, n_t)

    # Floors: ~3 per title, 2% naming a register no title has.
    per = 1 + rng.poisson(2.0, n_t)
    owner = np.repeat(np.arange(n_t), per)
    n_f = len(owner)
    kind = rng.choice(["ground", "basement", "rooftop"], n_f, p=[0.85, 0.12, 0.03])
    number = rng.integers(1, 30, n_f).astype(np.int32)
    floor_pk = pa.array(title_pk).take(pa.array(owner))
    floor_pk = pc.if_else(pa.array(rng.random(n_f) < 0.02), cat("S9-X", num(owner, 9)), floor_pk)
    floors = {
        "floor_row_id": cat("FR", num(np.arange(n_f), 10)),
        "mgm_bldrgst_pk": floor_pk,
        "floor_kind": s(kind),
        "floor_number": pa.array(number),
        "floor_index": pa.array(np.where(kind == "basement", -number, number).astype(np.int32)),
        "floor_display_ko": cat(num(number), "층"),
        "source_snapshot_id": pa.repeat(pa.scalar("synthetic-floors-1"), n_f),
    }
    write(out, "building_register_floors", floors, n_f)

    # Units: apartment titles hold lognormal(mean ~42) units, a few general titles hold 1-5.
    count = np.where(apartment, np.minimum(rng.lognormal(3.24, 1.0, n_t), 3000),
                     np.where(rng.random(n_t) < 0.03, rng.integers(1, 6, n_t), 0)).astype(np.int64)
    parent = np.repeat(np.arange(n_t), count)
    n_u = len(parent)
    u = np.arange(n_u)
    unit_pk = cat("S9-U", num(u, 10))
    pnu_index = np.where(rng.random(n_u) < 0.03, rng.integers(0, pool, n_u), title_pnu_index[parent])
    roll = rng.random(n_u)
    parent_pk = pa.array(title_pk).take(pa.array(parent))
    parent_pk = pc.if_else(pa.array(roll < 0.01), cat("S9-X", num(parent, 9)), parent_pk)  # names no title
    parent_pk = nullify(parent_pk, (roll >= 0.01) & (roll < 0.03))
    invalid = (roll >= 0.03) & (roll < 0.04)  # a claim without evidence: nulled by the build
    approved = (roll >= 0.04) & (roll < 0.045)
    digest = _hex_rows(rng, n_u)
    floor_no = rng.integers(1, 40, n_u).astype(np.int32)
    ho = floor_no * 100 + rng.integers(1, 9, n_u)
    dong = cat(num(101 + (parent % 20)), "동")
    units = {
        "unit_row_id": cat("UR", num(u, 10)),
        "mgm_bldrgst_pk": unit_pk,
        "pnu": nullify(pnu_of(pnu_index), rng.random(n_u) < 0.002),
        "dong_join_name": nullify(dong, rng.random(n_u) < 0.3),
        "dong_name_raw": nullify(dong, rng.random(n_u) < 0.2),
        "unit_name_raw": num(ho),
        "unit_label_ko": nullify(cat(num(ho), "호"), rng.random(n_u) < 0.05),
        "floor_number": nullify(pa.array(floor_no), rng.random(n_u) < 0.02),
        "floor_kind": s(rng.choice(["ground", "basement", "rooftop"], n_u, p=[0.95, 0.04, 0.01])),
        "building_mgm_bldrgst_pk": parent_pk,
        "building_link_method": s(np.where(approved, "steward_approval", "parent_key")),
        "building_link_source_record_id": nullify(cat("synthetic-src-", num(u)), approved),
        "building_link_input_sha256": nullify(pc.if_else(pa.array(invalid), pa.scalar("not-a-digest"), digest), approved),
        "normalization_application_id": nullify(cat("00000000-0000-4000-8000-", num(u % 10**12, 12)), ~approved),
        "source_snapshot_id": pa.repeat(pa.scalar("synthetic-units-1"), n_u),
    }
    write(out, "building_register_units", units, n_u)

    # Areas: per unit 1-2 exclusive and ~4.4 common rows; ~0.5 common rows per title.
    excl = 1 + (rng.random(n_u) < 0.1)
    common = rng.poisson(4.4, n_u)
    title_common = rng.poisson(0.5, n_t)
    owner_pk = pa.concat_arrays([
        pa.array(unit_pk).take(pa.array(np.repeat(u, excl))),
        pa.array(unit_pk).take(pa.array(np.repeat(u, common))),
        pa.array(title_pk).take(pa.array(np.repeat(np.arange(n_t), title_common))),
    ])
    n_e, n_c = int(excl.sum()), int(common.sum())
    n_a = len(owner_pk)
    area_kind = np.array(["exclusive"] * n_e + ["common"] * (n_a - n_e))
    area = np.where(area_kind == "exclusive", rng.uniform(10, 200, n_a), rng.uniform(0.5, 50, n_a))
    areas = {
        "area_row_id": cat("AR", num(np.arange(n_a), 11)),
        "mgm_bldrgst_pk": owner_pk,
        "area_kind": s(area_kind),
        "area_m2": nullify(pa.array(np.round(area, 2)), rng.random(n_a) < 0.01),
        "usage_name_raw": nullify(s(rng.choice(USAGE, n_a)), rng.random(n_a) < 0.05),
        "structure_name_raw": nullify(s(rng.choice(STRUCTURE, n_a)), rng.random(n_a) < 0.05),
        "floor_kind": s(np.where((area_kind == "common") & (rng.random(n_a) < 0.03), "rooftop", "ground")),
        "source_snapshot_id": pa.repeat(pa.scalar("synthetic-areas-1"), n_a),
    }
    write(out, "building_register_unit_areas", areas, n_a)

    # Prices: 80% of units carry 5-20 yearly reference dates, newest 2025; 2% name no unit.
    history = np.where(rng.random(n_u) < 0.8, rng.integers(5, 21, n_u), 0) if with_prices else np.zeros(n_u, int)
    owner = np.repeat(u, history)
    n_p = len(owner)
    offset = np.arange(n_p) - np.repeat(np.cumsum(history) - history, history)
    price_pk = pa.array(unit_pk).take(pa.array(owner))
    price_pk = pc.if_else(pa.array(rng.random(n_p) < 0.02), cat("S9-Y", num(owner, 10)), price_pk)
    prices = {
        "mgm_bldrgst_pk": price_pk,
        "pnu": pnu_of(pnu_index[owner]) if n_p else s([]),
        "dong_name": pa.repeat(pa.scalar("101동"), n_p),
        "ho_name": num(ho[owner]) if n_p else s([]),
        "base_date": cat(num(2025 - offset), "0101") if n_p else s([]),
        "price_won": pa.array(rng.integers(50_000_000, 3_000_000_000, n_p, dtype=np.int64)),
        "sido": pa.repeat(pa.scalar("99"), n_p),
        "source_snapshot_id": pa.repeat(pa.scalar("synthetic-price-batch-1"), n_p),
        "source_record_id": pa.repeat(pa.scalar("synthetic"), n_p),
    }
    write(out, "unit_official_price", prices, n_p)


def _hex_rows(rng, n):
    raw = rng.bytes(32 * n).hex()
    return s([raw[i * 64:(i + 1) * 64] for i in range(n)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--scale", required=True, type=float, help="fraction of production row counts, e.g. 0.05")
    parser.add_argument("--seed", type=int, default=20261012)
    parser.add_argument("--no-prices", action="store_true", help="an empty price table (the panel without price history)")
    args = parser.parse_args()
    generate(args.out, args.scale, args.seed, not args.no_prices)


if __name__ == "__main__":
    main()
