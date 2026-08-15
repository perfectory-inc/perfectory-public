#!/usr/bin/env python3
"""Render the human-readable Foundation public-data catalog from JSON SSOT files."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ENDPOINT_CATALOG = ROOT / "platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json"
LANE_REGISTRY = ROOT / "platforms/foundation-platform/docs/catalog/public-data-bronze-lane-registry.v1.json"
OUTPUT = ROOT / "platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def md(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def status_for(endpoint: dict[str, Any], default_lanes: set[str]) -> str:
    lane = endpoint["source_acquisition_lane"]
    if lane in default_lanes and endpoint["national_collection_allowed"]:
        return "기본 실행"
    if lane == "provider_inventory_missing":
        return "제공기관 목록 없음"
    if lane == "disabled_api_duplicate":
        return "중복 API 비활성"
    if lane == "manual_approval_bulk":
        return "수동 승인"
    if lane == "open_api_only":
        return "API 예정"
    if lane == "provider_dataset_file":
        return "파일 실행"
    return "확인 필요"


def render(endpoint_doc: dict[str, Any], lane_doc: dict[str, Any]) -> str:
    endpoints = endpoint_doc["endpoints"]
    lanes = lane_doc["lanes"]
    default_lanes = {
        source_lane
        for lane in lanes
        if lane["status"] == "enabled" and lane["include_by_default"]
        for source_lane in lane["source_acquisition_lanes"]
    }
    lane_by_source = {
        source_lane: lane
        for lane in lanes
        for source_lane in lane["source_acquisition_lanes"]
    }

    provider_counts = Counter(endpoint["provider"] for endpoint in endpoints)
    lane_counts = Counter(endpoint["source_acquisition_lane"] for endpoint in endpoints)
    group_counts = Counter(endpoint["group"] for endpoint in endpoints)
    allowed_count = sum(endpoint["national_collection_allowed"] for endpoint in endpoints)
    unique_dataset_count = len(
        {endpoint["dataset_slug"] for endpoint in endpoints if endpoint.get("dataset_slug")}
    )
    unique_source_count = len(
        {endpoint["bronze"]["source_slug"] for endpoint in endpoints}
    )

    lines: list[str] = [
        "---",
        "status: current",
        "owner: foundation-platform",
        "doc_type: catalog",
        "last_reviewed: 2026-07-30",
        "---",
        "",
        "<!-- GENERATED FILE. Do not edit by hand. -->",
        "<!-- Render with: python3 scripts/catalog/render-public-data-catalog.py -->",
        "",
        "# 공공데이터 수집 카탈로그",
        "",
        "> 이 문서는 사람이 읽는 색인입니다. 정확한 endpoint, source slug, 수집 레인, 허용 여부의 기준은 아래 JSON SSOT입니다.",
        "> 숫자와 표는 JSON에서 자동 생성되므로 이 문서에 직접 목록을 추가하지 않습니다.",
        "",
        "## 기준 문서",
        "",
        "- [공공 소스 엔드포인트 카탈로그](./public-source-endpoint-catalog.v1.json) — 무엇을 수집할 수 있는지",
        "- [Bronze 수집 레인 레지스트리](./public-data-bronze-lane-registry.v1.json) — 어떤 방식으로 실행하는지",
        "- [Bronze source slug 매핑](./bronze-source-slug-rename.v1.md) — 제공기관·데이터명·슬러그 참고표",
        "- [Bronze 레인 실행 런북](../runbooks/public-data-bronze-lane-orchestration.md) — 실행 게이트와 운영 절차",
        "",
        "## 현재 카탈로그 규모",
        "",
        f"- 엔드포인트 정의: **{len(endpoints)}개**",
        f"- 고유 dataset slug: **{unique_dataset_count}개**",
        f"- 고유 Bronze source slug: **{unique_source_count}개**",
        f"- 국가 수집 허용 endpoint: **{allowed_count}개**",
        f"- 기본 실행 레인에 포함되는 endpoint: **{sum(status_for(e, default_lanes) == '기본 실행' for e in endpoints)}개**",
        "",
        "## 제공기관별 정리",
        "",
        "| 제공기관 | endpoint 수 | 국가 수집 허용 | 기본 실행 | 주요 상태 |",
        "|---|---:|---:|---:|---|",
    ]
    for provider in sorted(provider_counts):
        provider_endpoints = [e for e in endpoints if e["provider"] == provider]
        statuses = Counter(status_for(e, default_lanes) for e in provider_endpoints)
        status_text = ", ".join(f"{key} {statuses[key]}" for key in sorted(statuses))
        lines.append(
            f"| {md(provider)} | {len(provider_endpoints)} | "
            f"{sum(e['national_collection_allowed'] for e in provider_endpoints)} | "
            f"{sum(status_for(e, default_lanes) == '기본 실행' for e in provider_endpoints)} | {md(status_text)} |"
        )

    lines += [
        "",
        "## 수집 레인",
        "",
        "| 레인 | 상태 | 기본 포함 | 제공기관 | endpoint 그룹 |",
        "|---|---|---:|---|---|",
    ]
    for lane in lanes:
        lines.append(
            f"| `{md(lane['lane_id'])}` | {md(lane['status'])} | "
            f"{str(lane['include_by_default']).lower()} | {md(lane['provider'])} | "
            f"{md(', '.join(lane['endpoint_groups']))} |"
        )

    lines += [
        "",
        "## 취급 데이터 묶음",
        "",
        "| endpoint 그룹 | 수 | 설명 |",
        "|---|---:|---|",
    ]
    group_descriptions = {
        "building_hub_bulk": "hub.go.kr 건축물·허가·에너지·점검 벌크 파일",
        "vworld_dataset": "vworld.kr 제공기관 데이터 파일",
        "vworld_ned_open_api": "vworld.kr NED API (현재 기본 실행 제외)",
        "building_register_open_api": "data.go.kr 건축물대장 API 중복 경로",
        "real_transaction_open_api": "data.go.kr 실거래 API 보조·검증 경로",
        "juso_electronic_map_bulk": "juso.go.kr 주소정보 전자지도 벌크 (수동 승인)",
        "other_bulk": "학교·공장·인구·교통 등 추가 벌크 (수동 승인)",
    }
    for group in sorted(group_counts):
        lines.append(
            f"| `{md(group)}` | {group_counts[group]} | {md(group_descriptions.get(group, '설명 필요'))} |"
        )

    group_names = {
        "building_hub_bulk": "건축물·허가·에너지·점검",
        "vworld_dataset": "공간·토지·산업단지",
        "vworld_ned_open_api": "수치표고(NED)",
        "building_register_open_api": "건축물대장 API",
        "real_transaction_open_api": "부동산 실거래",
        "juso_electronic_map_bulk": "주소정보 전자지도",
        "other_bulk": "기타 공공데이터",
    }
    acquisition_names = {
        "provider_dataset_file": "제공기관 파일",
        "open_api_only": "공개 API",
        "disabled_api_duplicate": "중복 API",
        "manual_approval_bulk": "벌크(수동 승인)",
        "provider_inventory_missing": "제공기관 목록 없음",
    }

    lines += [
        "",
        "## Foundation 건축물 데이터 세부 분류",
        "",
        "| Hub 작업 그룹 | endpoint 수 | 의미 |",
        "|---|---:|---|",
    ]
    task_group_names = {
        "01": "건축허가",
        "02": "주택허가",
        "03": "건축물대장",
        "04": "폐쇄말소대장",
        "05": "연간 에너지",
        "06": "건축물 유지관리",
        "08": "월별 에너지",
    }
    hub_endpoints = [e for e in endpoints if e["provider"] == "hub.go.kr"]
    task_groups: dict[str, list[dict[str, Any]]] = {}
    for endpoint in hub_endpoints:
        selector = endpoint.get("provider_inventory_selector") or {}
        code = selector.get("task_group_code", "-")
        task_groups.setdefault(code, []).append(endpoint)
    for code in sorted(task_groups):
        lines.append(
            f"| `{md(code)}` | {len(task_groups[code])} | "
            f"{md(task_group_names.get(code, '추가 확인 필요'))} |"
        )

    lines += [
        "",
        "### 폐쇄말소대장",
        "",
        "폐쇄말소대장은 Hub group `04`의 10개 벌크 endpoint로 Bronze 수집 대상입니다. "
        "현재 catalog의 `product_semantics`는 `forbidden`, Silver 상태는 `planned`이므로, "
        "원시 보관과 상품 제공을 구분합니다.",
        "",
        "### 연간 에너지",
        "",
        "연간 전기·가스 2개 endpoint는 카탈로그에는 남아 있지만 현재 제공기관 inventory 행이 없어 "
        "`provider_inventory_missing`으로 비활성화되어 있습니다.",
        "",
        "## 수집 데이터 한글 목록",
        "",
        "> 사람이 보는 목록입니다. endpoint·dataset slug·Bronze source slug 같은 기술 식별자는 [공공 소스 엔드포인트 카탈로그](./public-source-endpoint-catalog.v1.json)에서 확인합니다.",
        "",
        "| 제공기관 | 데이터 종류 | 수집 데이터(한글명) | 수집 방식 | 국가 수집 허용 | 현재 상태 |",
        "|---|---|---|---|---:|---|",
    ]
    for endpoint in sorted(endpoints, key=lambda item: (item["provider"], item["group"], item["endpoint_slug"])):
        lines.append(
            f"| {md(endpoint['provider'])} | {md(group_names.get(endpoint['group'], endpoint['group']))} | "
            f"{md(endpoint['display_name_ko'])} | "
            f"{md(acquisition_names.get(endpoint['source_acquisition_lane'], endpoint['source_acquisition_lane']))} | "
            f"{str(endpoint['national_collection_allowed']).lower()} | "
            f"{status_for(endpoint, default_lanes)} |"
        )

    lines += [
        "",
        "## 문서 유지 규칙",
        "",
        "1. 정확한 데이터 목록과 상태는 JSON SSOT만 수정합니다.",
        "2. 변경 후 `python3 scripts/catalog/render-public-data-catalog.py`를 실행합니다.",
        "3. CI는 `--check`로 생성 결과가 커밋된 문서와 같은지 검사합니다.",
        "4. Bronze에 수집된다는 사실만으로 Silver/Gold 또는 제품 공개가 완료된 것으로 보지 않습니다.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the generated file is stale")
    args = parser.parse_args()

    rendered = render(load_json(ENDPOINT_CATALOG), load_json(LANE_REGISTRY))
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            print(f"stale generated catalog: {OUTPUT}", file=sys.stderr)
            return 1
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8", newline="\n")
    print(OUTPUT.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
