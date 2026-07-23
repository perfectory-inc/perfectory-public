"""전유부↔전유공용면적 층충돌의 3증인 다수결 교정표 산출.

두 대장이 같은 호(mgm_bldrgst_pk)의 층을 다르게 주장하는 행(전유행 단일,
양쪽 층 존재)에 대해, 제3증인인 호명 층(명시 층접두=strong, 호수 앞자리
관행=weak)이 어느 쪽 편인지로 2/3 다수결을 낸다. 원본 Silver는 불변 —
산출물은 별도 교정표(parquet)이며 서빙에서 조인한다.

의사결정 규칙은 이 잡과 테스트가 실행 가능한 정본이다. 시점별 분포와 운영 증거는
루트 ADR-0007에 따른 비공개 운영 증거 저장소에서 관리한다.
"""
import argparse
import json
import re

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql import types as T

EXPLICIT = re.compile(r"^(?:제)?(지하)?(\d{1,2})층")
PURE_NUM = re.compile(r"^(\d{3,5})호?$")

SCHEMA_VERSION = "foundation-platform.building_register_unit_floor_resolution.v1"


def designation_floor(desig):
    """호명에서 층 추출 → (floor_index, strength) 또는 (None, None)."""
    if not desig:
        return None, None
    m = EXPLICIT.match(desig)
    if m:
        n = int(m.group(2))
        return (-n if m.group(1) else n), "strong"
    m = PURE_NUM.match(desig)
    if m:
        floor = int(m.group(1)[:-2])
        if floor >= 1:
            return floor, "weak"
    return None, None


def resolve(u_kind, u_idx, a_kind, a_idx, desig):
    """2/3 다수결. 반환: (resolved_kind, resolved_idx, resolution)."""
    d_floor, strength = designation_floor(desig)
    if d_floor is None:
        return None, None, "unresolved_no_signal"
    if d_floor == a_idx and d_floor != u_idx:
        return a_kind, a_idx, f"area_majority_{strength}"
    if d_floor == u_idx and d_floor != a_idx:
        return u_kind, u_idx, f"unit_majority_{strength}"
    return None, None, "unresolved_three_way"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--units-parquet", required=True)
    parser.add_argument("--areas-parquet", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary-output", required=True)
    parser.add_argument("--resolved-at", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName(
        "building-register-unit-floor-resolution"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    units = spark.read.parquet(args.units_parquet).select(
        "mgm_bldrgst_pk", "pnu", "register_parcel_key", "unit_designation",
        F.col("floor_kind").alias("unit_floor_kind"),
        F.col("floor_index").alias("unit_floor_index"),
    )
    w = Window.partitionBy("mgm_bldrgst_pk")
    areas = (
        spark.read.parquet(args.areas_parquet)
        .where(F.col("area_kind") == "exclusive")
        .select(
            "mgm_bldrgst_pk",
            F.col("floor_kind").alias("area_floor_kind"),
            F.col("floor_index").alias("area_floor_index"),
        )
        .withColumn("exclusive_rows", F.count("*").over(w))
        .where(F.col("exclusive_rows") == 1)
        .drop("exclusive_rows")
    )
    conflicts = (
        areas.join(units, "mgm_bldrgst_pk")
        .where(
            F.col("area_floor_index").isNotNull()
            & F.col("unit_floor_index").isNotNull()
            & (
                (F.col("area_floor_kind") != F.col("unit_floor_kind"))
                | (F.col("area_floor_index") != F.col("unit_floor_index"))
            )
        )
    )

    resolve_udf = F.udf(
        resolve,
        T.StructType([
            T.StructField("resolved_floor_kind", T.StringType()),
            T.StructField("resolved_floor_index", T.IntegerType()),
            T.StructField("resolution", T.StringType()),
        ]),
    )
    resolved = (
        conflicts.withColumn(
            "r",
            resolve_udf(
                "unit_floor_kind", "unit_floor_index",
                "area_floor_kind", "area_floor_index", "unit_designation",
            ),
        )
        .select(
            "mgm_bldrgst_pk", "pnu", "register_parcel_key", "unit_designation",
            "unit_floor_kind", "unit_floor_index",
            "area_floor_kind", "area_floor_index",
            F.col("r.resolved_floor_kind").alias("resolved_floor_kind"),
            F.col("r.resolved_floor_index").alias("resolved_floor_index"),
            F.col("r.resolution").alias("resolution"),
            F.lit(args.resolved_at).alias("resolved_at"),
            F.lit(SCHEMA_VERSION).alias("schema_version"),
        )
        .cache()
    )

    counts = {
        row["resolution"]: row["count"]
        for row in resolved.groupBy("resolution").agg(
            F.count("*").alias("count")
        ).collect()
    }
    total = sum(counts.values())
    resolved_count = sum(
        v for k, v in counts.items() if k.startswith(("area_", "unit_"))
    )
    unresolved_count = total - resolved_count

    resolved.coalesce(4).write.mode("overwrite").parquet(args.output)

    summary = {
        "schema_version": SCHEMA_VERSION,
        "conflict_rows": total,
        "resolved_count": resolved_count,
        "unresolved_count": unresolved_count,
        "resolution_counts": counts,
        "output": args.output,
        "resolved_at": args.resolved_at,
        "note": "원본 Silver 불변 — 서빙 조인용 교정표. 결정적 해소 규칙과 분포 불변식 검증 필수.",
    }
    with open(args.summary_output, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    print("floor-resolution-summary-json " + json.dumps(summary, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
