#!/usr/bin/env python3
"""Run one engine over one generated dataset; record wall time, peak RSS and CPU of its process tree.

    measure.py spark|datafusion|duckdb --data DIR --out DIR [--results FILE] [--label TEXT] [--no-prices]
    measure.py compare SPARK_OUT OTHER_OUT

Spark runs the production job unchanged (`--input-mode parquet --write-mode parquet`), or with
`--no-prices` the job of PR #430 (`--job`, the panel without unit prices); DataFusion and DuckDB
run this spike's release binary. Peak RSS is the largest sum of the tree's resident sets, sampled
every 0.2 s; CPU is user+system seconds summed over every process the tree ever held.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import shutil

import psutil
import pyarrow.compute as pc
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "platforms/foundation-platform/infra/lakehouse/spark/jobs/building_panel_silver_to_gold.py"
PUBLISHED = "2026-10-12T00:00:00Z"
COMPARED = ("pnu", "buildings_json", "unlinked_units_json", "row_digest", "source_snapshot_id")


def command(engine, data: Path, out: Path, args):
    if engine == "spark":
        if args.no_prices and args.job == JOB:
            raise SystemExit("--no-prices needs --job: the PR #430 job, which reads no price table")
        env = {**os.environ, "PYSPARK_PYTHON": sys.executable, "PYTHONIOENCODING": "utf-8",
               "PYSPARK_SUBMIT_ARGS": f"--master local[{args.cores}] --driver-memory {args.spark_memory} "
                                      "--conf spark.sql.shuffle.partitions=400 pyspark-shell"}
        return [sys.executable, str(args.job), "--input-mode", "parquet", "--input-root", str(data), "--output", str(out),
                "--write-mode", "parquet", "--iceberg-snapshot-id", "synthetic", "--published-at-utc", PUBLISHED], env
    cmd = [args.binary, str(data), str(out), PUBLISHED, str(args.cores)]
    if args.df_memory_gib:
        cmd.append(str(args.df_memory_gib))
    env = {**os.environ, "SPIKE_ENGINE": engine, "SPIKE_REPO_ROOT": str(ROOT)}
    if args.no_prices:
        env["SPIKE_NO_PRICES"] = "1"
    return cmd, env


def run(engine, args):
    for stale in (args.out, args.out.with_name(args.out.name + ".spill")):  # a scratch output of an earlier run
        shutil.rmtree(stale, ignore_errors=True)
    cmd, env = command(engine, args.data, args.out, args)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    log = args.out.with_name(args.out.name + ".log")
    started = time.perf_counter()
    with open(log, "wb") as sink:
        proc = subprocess.Popen(cmd, env=env, stdout=sink, stderr=subprocess.STDOUT)
        root = psutil.Process(proc.pid)
        peak, cpu = 0, {}
        while proc.poll() is None:
            try:
                tree = [root, *root.children(recursive=True)]
            except psutil.NoSuchProcess:
                break
            rss = 0
            for p in tree:
                try:
                    rss += p.memory_info().rss
                    t = p.cpu_times()
                    cpu[p.pid] = t.user + t.system
                except psutil.NoSuchProcess:
                    pass
            peak = max(peak, rss)
            time.sleep(0.2)
        proc.wait()  # the loop also ends when the process exits between poll() and children()
    wall = time.perf_counter() - started
    result = {"engine": engine, "label": args.label, "data": str(args.data), "exit": proc.returncode,
              "wall_s": round(wall, 1), "peak_rss_gib": round(peak / 2**30, 2), "cpu_s": round(sum(cpu.values()), 1),
              "cores": args.cores, "spark_driver_memory": args.spark_memory if engine == "spark" else None,
              "memory_limit_gib": args.df_memory_gib if engine != "spark" else None, "no_prices": args.no_prices}
    print(json.dumps(result), flush=True)
    if args.results:
        with open(args.results, "a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")
    return proc.returncode


def table(path: Path):
    hive = "hive" if any(p.name.startswith("source_snapshot_id=") for p in path.iterdir()) else None
    return ds.dataset(path, format="parquet", partitioning=hive).to_table(columns=list(COMPARED))


def compare(spark_out: Path, df_out: Path):
    left, right = (table(p) for p in (spark_out, df_out))
    left, right = (t.cast(left.schema).sort_by("pnu") for t in (left, right))
    print(f"rows spark={left.num_rows} other={right.num_rows}")
    if left.num_rows != right.num_rows:
        return 1
    for name in COMPARED:
        same = pc.equal(left[name], right[name])
        mismatched = left.num_rows - pc.sum(same.fill_null(False)).as_py()
        if mismatched:
            i = pc.index(same.fill_null(False), False).as_py()
            a, b = left[name][i].as_py(), right[name][i].as_py()
            at = next((k for k in range(min(len(a), len(b))) if a[k] != b[k]), min(len(a), len(b))) if isinstance(a, str) and isinstance(b, str) else 0
            print(f"MISMATCH {name}: {mismatched} rows; first pnu={left['pnu'][i]} at char {at}\n"
                  f"  spark={str(a)[max(at-200,0):at+200]}\n  other={str(b)[max(at-200,0):at+200]}")
            return 1
    print("IDENTICAL: every row, every compared column")
    return 0


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "compare":
        return compare(Path(sys.argv[2]), Path(sys.argv[3]))
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=("spark", "datafusion", "duckdb"))
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--results", type=Path)
    parser.add_argument("--label", default="")
    parser.add_argument("--cores", type=int, default=8)
    parser.add_argument("--spark-memory", default="8g")
    parser.add_argument("--df-memory-gib", type=float, help="memory limit of DataFusion's pool or DuckDB's memory_limit")
    parser.add_argument("--no-prices", action="store_true", help="the panel without unit prices (PR #430)")
    parser.add_argument("--job", type=Path, default=JOB, help="the Spark job file (default: this tree's)")
    parser.add_argument("--binary", default=r"C:\ct\release\foundation-datafusion-gold-spike.exe")
    args = parser.parse_args()
    return run(args.engine, args)


if __name__ == "__main__":
    raise SystemExit(main())
