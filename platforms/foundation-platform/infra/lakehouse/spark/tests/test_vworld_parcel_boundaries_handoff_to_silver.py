"""필지 핸드오프 잡의 적재 후 읽기 범위를 고정한다.

이 잡은 쓰고 나서 표를 다시 읽어 확인한다. 그 읽기가 무엇을 걸러내느냐가 중요하다.
읽기 범위가 이번 실행보다 넓으면, 앞선 실행이 넣은 행까지 딸려 와 유일성 검사가
"같은 PNU 가 여러 개"라고 보고한다. 제대로 쓴 실행이 실패로 보고되고, 재시도가
같은 파일을 두 번 넣어 **진짜** 중복을 만든다. 2026-08-27 에 실제로 그렇게 됐다.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

JOB_PATH = (
    Path(__file__).resolve().parents[1] / "jobs" / "vworld_parcel_boundaries_handoff_to_silver.py"
)


def load_job_module():
    spec = importlib.util.spec_from_file_location("vworld_parcel_job", JOB_PATH)
    if spec is None or spec.loader is None:
        pytest.skip(f"cannot load {JOB_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # pragma: no cover - pyspark 없는 환경
        pytest.skip(f"job module needs its runtime dependencies: {exc}")
    return module


def test_readback_filters_on_the_source_object_not_the_provider_snapshot() -> None:
    """읽기는 Bronze 객체 단위여야 한다.

    `source_snapshot_id` 는 제공자 스냅숏 이름이라 한 시기의 전국 추출 255개가 모두 같다.
    그것으로 거르면 두 번째 적재가 첫 번째 적재의 행을 함께 읽는다.
    `source_record_id` 는 행이 온 Bronze 객체를 가리키므로 실행끼리 겹치지 않는다.
    """
    source = JOB_PATH.read_text(encoding="utf-8")
    start = source.index("def read_iceberg_snapshot_for_batch")
    end = source.index("\ndef ", start + 1)
    body = source[start:end]

    assert 'F.col("source_record_id").isin(' in body, (
        "적재 후 읽기는 source_record_id 로 걸러야 한다"
    )
    assert 'F.col("source_snapshot_id").isin(' not in body, (
        "source_snapshot_id 로 거르면 앞선 적재의 행까지 읽는다"
    )


def test_readback_bounds_how_many_objects_one_run_may_append() -> None:
    """한 실행이 담을 수 있는 Bronze 객체 수에 상한이 있어야 한다.

    상한이 없으면 읽기 조건이 무한정 길어진다. 전국 필지는 255개 객체라 한 실행에
    다 담기지 않으며, 이 상한은 묶음 하나를 제한하는 것이지 데이터셋을 제한하지 않는다.
    """
    module = load_job_module()
    limit = getattr(module, "MAX_READBACK_SOURCE_RECORDS", None)
    assert isinstance(limit, int) and limit > 0, "상한이 정수로 선언돼 있어야 한다"
    assert limit < 255, (
        "상한이 전국 객체 수 이상이면 한 실행에 다 넣으라는 뜻이 되어 상한의 의미가 없다"
    )
