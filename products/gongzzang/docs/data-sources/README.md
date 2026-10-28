---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-10-28
---

# 데이터 원천

이 폴더에는 원천 경계 규칙만 기록하며 로컬 Catalog client 구현은 두지 않는다.

## 등록된 원천

| 원천 | 소유자 | Gongzzang 진입점 | 문서 |
|---|---|---|---|
| V-World | Foundation Platform Catalog | Foundation Platform 계약만 | 아래 [Catalog 원천 경계](#catalog-원천-경계) |
| data.go.kr Catalog API | Foundation Platform Catalog | Foundation Platform 계약만 | 아래 [Catalog 원천 경계](#catalog-원천-경계) |
| 법제처 법령 API | 제품 기능이 필요할 때만 Gongzzang | breaker·audit·raw lineage 를 갖춘 직접 호출 | [korean-law.md](./korean-law.md) |
| NICE 본인인증 | Gongzzang 인증·컴플라이언스 | 제공기관 직접 연동 | [nice-identity.md](./nice-identity.md) |
| Naver Maps | Gongzzang 프런트엔드·지도 | 승인된 지도 연동 | [naver-maps.md](./naver-maps.md) |

원천별 상세(엔드포인트, 수집 레인, 실행 허용 여부)는 Foundation Platform 이 소유한다:
[공공데이터 수집 카탈로그](../../../../platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md),
[V-World 데이터 카탈로그 참고](../../../../platforms/foundation-platform/docs/catalog/vworld-data-catalog-reference.md).

## Catalog 원천 경계

V-World 와 data.go.kr 의 Catalog 연동은 Foundation Platform 이 소유한다
([GZ-ADR-0034](../adr/0034-catalog-ownership-handover-to-foundation-platform.md),
[GZ-ADR-0048](../adr/0048-horizontal-platform-redefinition.md)). Gongzzang 은 client·예약 수집·원자료 보관·변경
감시를 추가하지 않고 Catalog 사실을 Foundation Platform 공개 계약으로만 소비한다.

허용:

- Catalog HTTP API 고정 계약 `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- 이벤트 고정 계약 `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`
- Gongzzang 읽기 모델로 가져오는 불변 PNU anchor 산출물, Foundation 건물 응답을 Gongzzang 응답 형태로 바꾸는 route 번역

금지:

- Catalog 데이터를 위한 V-World·data.go.kr 직접 HTTP 호출
- Catalog client 나 대체 ACL crate, building-register sync 작업
- `parcel_external_data` 쓰기, raw capture 바이너리, R2 원자료 보관 writer
- 원천 전용 drift smoke workflow

Foundation Platform 이 credential·quota, 요청·응답 파싱, raw response lineage, 스키마 변경 감시, 정본 필지·건물
사실과 공개·기준 공간 레이어를 소유한다. Gongzzang 은 매물 의미, 매물 마커 제공, 그에 필요한 PNU anchor 읽기
모델 사본을 소유한다.

원천 동작이 바뀌면 Foundation Platform 을 먼저 갱신하고, Gongzzang 은 바뀐 공개 계약에 맞춰 고정 계약만 갱신한다.

가드:

- `scripts/lefthook/foundation-ownership-boundary.sh` — Catalog 도메인 crate 재도입, Catalog 표 직접 쓰기,
  Catalog 변경 route 를 거부한다(CI `gongzzang-ci.yml`).
- `docs/architecture/foundation-platform-boundary.v1.json` — 경로 소유권 목록. `cargo run -p repo-guard -- capability-layout`
  이 도메인 crate 와 대조한다.
- `crates/foundation-platform-client/tests/catalog_contract_pin.rs` — 고정 계약을 crate 의 Foundation OpenAPI 스냅숏(`openapi/catalog.v1.json`)과 대조한다.

## 기본 시스템 규칙

Gongzzang 이 소유한 외부 호출은 timeout·retry·circuit breaker·관측성·audit/logging 규칙을 사용한다.

## 에이전트 전용 규칙

MCP 도구는 개발 탐색에 사용할 수 있지만 `apps/`, `services/`, `crates/`, `packages/`가 MCP/LLM SDK를
import하면 안 된다([GZ-ADR-0010](../adr/0010-scope-information-platform-option-a.md)). 탐색용 MCP:
[korean-land-mcp](./korean-land-mcp.md), 법령 MCP 는 [korean-law.md](./korean-law.md)의 에이전트 경로 절.
