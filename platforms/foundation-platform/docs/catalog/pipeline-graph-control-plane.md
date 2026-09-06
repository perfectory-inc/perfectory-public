---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-09-06
---

# 파이프라인 그래프 계약과 운영 상태

[ADR-0086](../../../../docs/adr/0086-the-pipeline-graph-names-every-dataset-once.md)에 따라
데이터 연결의 정본은 [pipeline-graph.v1.json](./pipeline-graph.v1.json)이며
`schema_version`은 정수 `2`입니다. 파일명과 HTTP 경로는 기존 배포 경로를 유지합니다.
이 문서의 이전 버전은 실행 작업별 노드를 제안했던 초안이며 현재 계약으로 대체되었습니다.

사람이 읽는 전체 목록은 [자동 생성 데이터 지도](../../../../docs/data-pipeline-map.md)에 있습니다.
여기에 노드·테이블 목록을 다시 적지 않습니다.

## 정본의 책임

- 원천 그룹은 [endpoint 카탈로그](./public-source-endpoint-catalog.v1.json)의 `group`을
  `endpoint_catalog_group`으로 참조합니다. 그룹 노드에 endpoint 목록을 복제하지 않습니다.
- `silver_table`·`gold_table`은 [레이크하우스 계약](../../infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json)의
  `table_name`을 참조합니다.
- `serving_group.tables`는 [마이그레이션](../../migrations)이 선언한 `catalog`·`serving_postgis`
  표를 중복 없이 나눕니다. 사용자 데이터와 수집·품질·발행 원장을 모두 포함합니다.
- `serving_surface`는 타일·조회 API·게이트웨이·패널과 운영 접점입니다.
  운영 세부 종류는 `surface_kind`이며 미완성 운영 능력은 `missing_capability`입니다.
- 엣지의 `from`·`to`는 노드 ID이고 `via`는 실행 명령 또는 저장소의 실행 파일 경로 배열입니다.
  원천에서 나오는 `feeds` 엣지는 `source_slugs`로 실제로 읽는 원천 부분집합을 지정합니다.
  이 선택은 endpoint 카탈로그에 존재하고 해당 그룹에 속해야 합니다.

## 상태와 운영 증거

`implemented`는 코드에 경로가 있다는 뜻이며 전국 적재·운영 배포 완료를 주장하지 않습니다.
`contract_only`는 계약은 있지만 생산 레인이 없다는 뜻입니다. 원천 그룹의
`unmapped_status: collected_only`는 수집 이후 연결이 없음을 나타내며, 실제 수집 여부는
endpoint의 수집 설정과 수집 실행 원장으로 판단합니다. 비활성 API를 수집 완료로 표시하지 않습니다.

`GET /catalog/v1/pipeline-graph`는 [OpenAPI 계약](../openapi/pipeline-graph.v1.json)에 따라
정적 그래프에 `runtime`을 덧붙입니다. `runtime_bindings`의 수집 원천·레이크하우스 계약·outbox
범위를 실제 실행 지표에 연결하며, 정적 노드 상태 자체를 변경하지 않습니다.
이전 버전에서 실제 API가 사용한 운영 ID와 binding은 유지합니다.

`viewer_policy.canonical_store`는 Foundation이고 `ui_state_store`는 제품 viewer입니다.
화면 배치·확대·접힘 상태는 제품 소유이며 데이터 연결이나 실행 증거를 대신하지 않습니다.
소비자는 모르는 상태나 접점 종류도 일반 항목으로 표시할 수 있어야 합니다.

## 자동 검증과 생성

[화해 가드](../../../../scripts/guard/pipeline-graph-covers-every-dataset.sh)는 각 정본에서
그룹·표 집합을 직접 수집해 그래프와 대조하고 중복·엣지 끝점·실행 참조를 검사합니다.
SQL 스캐너는 전체 마이그레이션의 선언을 읽으며 지원하지 않는 표 삭제·이동이 생기면
성공으로 숨기지 않고 거부합니다. [자기 검사](../../../../scripts/guard/pipeline-graph-covers-every-dataset-self-test.sh)는
그래프와 정본 양쪽을 변조해 실제 거부를 증명합니다.

[렌더러](../../../../scripts/catalog/render-pipeline-map.py)는 한국어 지도와
[API 예제](./pipeline-graph.v1.example.json)를 함께 생성합니다. 예제는 정본의 생성된 복사본이며
독립적으로 편집하지 않습니다. docs CI의 `--check`는 두 산출물의 바이트가 일치하는지 확인합니다.
