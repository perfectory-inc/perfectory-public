"""호실 관계 승격 경계. 정책은 기존 호실 핸드오프 계약에서 읽는다."""
from functools import lru_cache
import json
from pathlib import Path


@lru_cache(maxsize=1)
def evidence_policy():
    path = Path(__file__).resolve().parents[2] / "contracts" / "building-unit-handoff.json"
    return json.loads(path.read_text(encoding="utf-8"))["relationship_evidence_policy"]


def evidence_expressions():
    """적재와 하류 승격이 공유하는 관계 근거 판정이다."""
    from pyspark.sql import functions as F

    policy = evidence_policy()
    def text(name):
        value = F.trim(F.col(name))
        return F.when(F.length(value) > 0, value)

    values = {name: text(name) for name in policy["columns"]}
    method = values["building_link_method"]
    source = values["building_link_source_record_id"]
    digest = values["building_link_input_sha256"]
    reason = values["building_link_reason"]
    application = values["normalization_application_id"]
    source_valid = ((method == policy["source_method"]) & source.isNotNull()
                    & digest.rlike(policy["sha256_pattern"]) & application.isNull())
    approved = (application.rlike(policy["application_id_pattern"])
                & (application != "00000000-0000-0000-0000-000000000000")
                & source.isNull() & digest.isNull())
    parent = F.col("building_mgm_bldrgst_pk")
    canonical_parent = parent.isNull() | ((F.length(parent) > 0) & (parent == F.trim(parent)))
    valid = (canonical_parent & values["unit_row_id"].isNotNull() & method.isNotNull() & reason.isNull()
             & F.coalesce(source_valid | approved, F.lit(False)))
    return values, valid


def invalid_building_link_evidence():
    from pyspark.sql import functions as F

    values, valid = evidence_expressions()
    claim = (F.col("building_mgm_bldrgst_pk").isNotNull()
             | values["normalization_application_id"].isNotNull())
    return claim & ~F.coalesce(valid, F.lit(False))


def verified_building_links(frame):
    """근거 없는 과거 관계만 NULL로 만들고 행·거부 사유를 보존한다.

    승인은 producer가 원장에서 읽고, Rust 소비 경계에서 활성 원장과 다시 대조한다.
    """
    from pyspark.sql import functions as F

    policy = evidence_policy()
    for column in policy["columns"]:
        if column not in frame.columns:
            frame = frame.withColumn(column, F.lit(None).cast("string"))
    values, valid = evidence_expressions()
    reason = values["building_link_reason"]
    values["building_link_reason"] = F.when(
        F.col("building_mgm_bldrgst_pk").isNotNull() & ~valid,
        F.coalesce(reason, F.lit("unverified_building_link_evidence")),
    ).otherwise(reason)
    return (frame.withColumn("building_link_reason", values["building_link_reason"])
            .withColumn("building_link_evidence", F.struct(
                *[values[name].alias(name) for name in policy["columns"]]))
            .withColumn("building_mgm_bldrgst_pk", F.when(valid, F.col("building_mgm_bldrgst_pk"))))
