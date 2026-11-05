---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-11-05
---

# Bronze source slug 규칙 (v1, 참고 문서)

> 전체 데이터 목록의 현재 기준은 [공공데이터 수집 카탈로그](./public-data-collection-catalog.md)와
> [공공 소스 endpoint 카탈로그](./public-source-endpoint-catalog.v1.json)입니다.
> 이 문서는 slug를 만드는 규칙과 각 사실의 정본 위치만 설명합니다. 데이터셋별 slug 목록은 여기에
> 다시 적지 않습니다.

## 규칙

- `source_slug = {providerid}__{dataset_slug}` (소문자, 이중 언더스코어). 근거는
  [ADR 0014](../adr/0014-bronze-source-slug-canonical-naming.md)다.
- **`operation`(API 호출용 식별자) != `dataset_slug`(의미 식별자)** — 생성기는 `dataset_slug`로
  만든다(ADR 0014 D3). 예: data.go.kr `getBrTitleInfo` → `building_register_main`.
- hub.go.kr·juso.go.kr·mois.go.kr 처럼 operation이 이미 의미 있는 snake_case인 제공기관은
  `dataset_slug == operation`이다. juso.go.kr은 중복 `juso_` 접두를 뗀 이름이 `dataset_slug`다.
- bare 슬러그(예: 실행 operation 없이 쓰던 건축물대장 기본값)는 금지한다. 실행 범위 접미사
  (`-national-pilot`, `-local-proof`)는 데이터 정체성이 아니므로 slug에 넣지 않는다.
- 카탈로그에 `dataset_slug`가 등록되지 않은 hub.go.kr bulk 작업은 opaque
  `hub-go-kr-public-bulk-task-*` slug를 만들지 않고 계획 단계에서 거부한다(fail-closed).

## 정본 위치

| 사실 | 정본 |
|---|---|
| 승인 제공기관과 파생 `providerid` | [`APPROVED_PROVIDER_DOMAINS`](../../crates/collection/collection-domain/src/source_slug.rs) |
| 데이터셋별 `operation`·`dataset_slug`·`bronze.source_slug` | [`public-source-endpoint-catalog.v1.json`](./public-source-endpoint-catalog.v1.json) |
| operation → dataset_slug 코드 맵(카탈로그와 parity 시험) | `crates/collection/collection-domain/src/operation_dataset_slug.rs` |
| 이 규칙을 정한 이유와 개명 이전 slug | [ADR 0014](../adr/0014-bronze-source-slug-canonical-naming.md)와 이 파일의 git 이력 |
