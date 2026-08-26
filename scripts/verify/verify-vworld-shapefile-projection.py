#!/usr/bin/env python3
"""Compare the Rust shapefile projection with pyproj over a real ZIP bbox."""

from __future__ import annotations

import argparse
import json
import math
import random
import struct
import subprocess
import tempfile
import zipfile
from pathlib import Path

from pyproj import CRS, Transformer

EARTH_RADIUS_METRES = 6_371_008.8


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", required=True, type=Path, dest="zip_path")
    parser.add_argument("--rust-probe", required=True, type=Path)
    parser.add_argument("--sample-count", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=5_186)
    parser.add_argument("--max-error-metres", type=float, default=0.001)
    args = parser.parse_args()
    if args.sample_count < 1_000:
        parser.error("--sample-count must be at least 1000")
    if args.max_error_metres < 0 or not math.isfinite(args.max_error_metres):
        parser.error("--max-error-metres must be finite and non-negative")
    return args


def one_member(names: list[str], extension: str) -> str:
    matches = [name for name in names if name.lower().endswith(extension)]
    if len(matches) != 1:
        raise ValueError(
            f"ZIP must contain exactly one {extension} member; found {len(matches)}"
        )
    return matches[0]


def source_facts(zip_path: Path) -> tuple[str, tuple[float, float, float, float]]:
    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        prj_name = one_member(names, ".prj")
        shp_name = one_member(names, ".shp")
        prj = archive.read(prj_name).decode("utf-8-sig").strip()
        with archive.open(shp_name) as shp:
            header = shp.read(100)
    if len(header) != 100:
        raise ValueError("SHP member is shorter than its 100-byte header")
    bbox = struct.unpack("<4d", header[36:68])
    if not all(math.isfinite(value) for value in bbox):
        raise ValueError("SHP bbox contains non-finite coordinates")
    min_x, min_y, max_x, max_y = bbox
    if max_x <= min_x or max_y <= min_y:
        raise ValueError(f"SHP bbox is not ordered: {bbox}")
    return prj, bbox


def rust_coordinates(
    probe: Path, prj: str, coordinates: list[tuple[float, float]]
) -> list[tuple[float, float]]:
    payload = "".join(json.dumps(pair, separators=(",", ":")) + "\n" for pair in coordinates)
    with tempfile.TemporaryDirectory(prefix="perfectory-t32-projection-") as directory:
        prj_path = Path(directory) / "source.prj"
        prj_path.write_bytes(prj.encode("utf-8"))
        completed = subprocess.run(
            [str(probe), str(prj_path)],
            input=payload,
            text=True,
            capture_output=True,
            check=False,
        )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Rust projection probe exited {completed.returncode}: {completed.stderr.strip()}"
        )
    rows = [json.loads(line) for line in completed.stdout.splitlines()]
    if len(rows) != len(coordinates):
        raise ValueError(
            f"Rust projection row count mismatch: expected {len(coordinates)}, got {len(rows)}"
        )
    return [(float(row[0]), float(row[1])) for row in rows]


def distance_metres(
    rust: tuple[float, float], oracle: tuple[float, float]
) -> float:
    rust_lon, rust_lat = rust
    oracle_lon, oracle_lat = oracle
    mean_latitude = math.radians((rust_lat + oracle_lat) / 2.0)
    delta_x = (
        math.radians(rust_lon - oracle_lon)
        * EARTH_RADIUS_METRES
        * math.cos(mean_latitude)
    )
    delta_y = math.radians(rust_lat - oracle_lat) * EARTH_RADIUS_METRES
    return math.hypot(delta_x, delta_y)


def main() -> int:
    args = parse_args()
    prj, bbox = source_facts(args.zip_path)
    min_x, min_y, max_x, max_y = bbox
    random_source = random.Random(args.seed)
    coordinates = [
        (
            random_source.uniform(min_x, max_x),
            random_source.uniform(min_y, max_y),
        )
        for _ in range(args.sample_count)
    ]
    rust = rust_coordinates(args.rust_probe, prj, coordinates)
    transformer = Transformer.from_crs(
        CRS.from_wkt(prj), CRS.from_epsg(4326), always_xy=True
    )
    oracle = [transformer.transform(x, y) for x, y in coordinates]
    degree_errors = [
        max(abs(rust_lon - py_lon), abs(rust_lat - py_lat))
        for (rust_lon, rust_lat), (py_lon, py_lat) in zip(rust, oracle, strict=True)
    ]
    metre_errors = [
        distance_metres(rust_coordinate, oracle_coordinate)
        for rust_coordinate, oracle_coordinate in zip(rust, oracle, strict=True)
    ]
    result = {
        "schema_version": "perfectory.vworld_shapefile_projection_oracle.v1",
        "sample_count": args.sample_count,
        "seed": args.seed,
        "source_bbox": {
            "min_x": min_x,
            "min_y": min_y,
            "max_x": max_x,
            "max_y": max_y,
        },
        "max_error_degrees": max(degree_errors),
        "max_error_metres": max(metre_errors),
        "allowed_error_metres": args.max_error_metres,
        "passed": max(metre_errors) <= args.max_error_metres,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
