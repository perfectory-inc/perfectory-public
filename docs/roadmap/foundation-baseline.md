---
status: current
owner: repository-maintainers
doc_type: catalog
last_reviewed: 2026-08-06
---

<!-- GENERATED FILE. Do not edit by hand. -->
<!-- Render with: python3 scripts/catalog/render-foundation-baseline.py -->

# 기반 지표

> [기반 목표](./foundation-goals.md)가 판정에 쓰는 수치입니다. 목표와 근거는 그 문서가
> 소유하고, 이 파일은 수만 소유합니다.

정적으로 재생산되는 수만 있습니다. 가드 통과 수와 레인 실행 테스트 수는 **실행해야 나오는**
수이므로 실행 로그가 소유하고, 기록된 부채의 열림/닫힘 구분은 판정이므로 사람이 소유합니다.

## G1 — 생산자 없는 canonical 표

- canonical 표: **61개**
- 그중 생산자 없음: **21개**

테스트와 시드를 뺀 `INSERT INTO`가 하나도 없는 표입니다. 시드를 세지 않는 이유는, 표를
채우는 fixture가 바로 시스템이 그 표를 채우지 않는다는 사실을 가리기 때문입니다.

| 표 |
|---|
| `catalog.administrative_unit_transition` |
| `catalog.allowed_industry` |
| `catalog.building` |
| `catalog.building_unit` |
| `catalog.complex_attachment` |
| `catalog.complex_notice` |
| `catalog.digital_twin_asset` |
| `catalog.industry_group` |
| `catalog.industry_group_member` |
| `catalog.lakehouse_access_policy` |
| `catalog.lakehouse_lineage_edge` |
| `catalog.lakehouse_quality_check` |
| `catalog.manufacturer` |
| `catalog.notice_attachment` |
| `catalog.outbox_quarantine` |
| `catalog.parcel` |
| `catalog.parcel_administrative_unit` |
| `catalog.parcel_industry_assignment` |
| `catalog.spatial_layer` |
| `catalog.vector_tile_build_job` |
| `catalog.vector_tile_refresh_observation` |

## G4 — 쓰이지 않는 상태값

- 상태 CHECK: **3개**
- 그중 쓰는 경로가 없는 값: **7개**

값 리터럴을 **그 표를 언급하는 자리 근처에서만** 찾습니다. 저장소 전체에서 찾으면 다른
표에 쓰이는 같은 이름의 값이 대신 세어져, 모든 상태가 도달 가능하다는 답이 나옵니다.
변수를 거쳐 쓰이거나 표 이름에서 멀리 떨어진 값은 도달 불가로 읽힙니다 — 과대 보고 쪽으로
틀리며, 그것이 안전한 방향입니다.

| 표 | 제약 | 값 |
|---|---|---|
| `catalog.administrative_boundary_revision` | `administrative_boundary_revision_status_check` | `superseded` |
| `catalog.vector_tile_build_job` | `vector_tile_build_job_status_check` | `planned` |
| `catalog.vector_tile_build_job` | `vector_tile_build_job_status_check` | `running` |
| `catalog.vector_tile_build_job` | `vector_tile_build_job_status_check` | `validated` |
| `catalog.vector_tile_build_job` | `vector_tile_build_job_status_check` | `promoted` |
| `catalog.vector_tile_build_job` | `vector_tile_build_job_status_check` | `superseded` |
| `catalog.vector_tile_build_job` | `vector_tile_build_job_status_check` | `failed` |

## 기록 규모

- ADR: **49개**
- `남은 부채` 항목: **62개**

항목 수는 남은 일의 수가 아닙니다. 열림/닫힘은
[운영 준비 작업 목록](./production-readiness.md)의 표가 소유합니다.
