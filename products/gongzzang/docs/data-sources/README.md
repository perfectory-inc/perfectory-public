---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-10-28
---

# 데이터 원천

이 폴더에는 원천 경계 규칙만 기록하며 로컬 카탈로그 client 구현은 두지 않는다.

## 등록된 원천

| 원천 | 소유자 | 공짱 진입점 | 문서 |
|---|---|---|---|
| V-World | 파운데이션 카탈로그 | 파운데이션 계약만 | 아래 [카탈로그 원천 경계](#카탈로그-원천-경계) |
| data.go.kr 카탈로그 API | 파운데이션 카탈로그 | 파운데이션 계약만 | 아래 [카탈로그 원천 경계](#카탈로그-원천-경계) |
| 법제처 법령 API | 제품 기능이 필요할 때만 공짱 | 회로 차단기·감사·원문 계보를 갖춘 직접 호출 | [korean-law.md](./korean-law.md) |
| NICE 본인인증 | 공짱 인증·규정 준수 | 제공기관 직접 연동 | [nice-identity.md](./nice-identity.md) |
| Naver Maps | 공짱 프런트엔드·지도 | 승인된 지도 연동 | [naver-maps.md](./naver-maps.md) |

원천별 상세(호출 주소, 수집 레인, 실행 허용 여부)는 파운데이션 이 소유한다:
[공공데이터 수집 카탈로그](../../../../platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md),
[V-World 수집 기능 계약](../../../../platforms/foundation-platform/docs/catalog/vworld/README.md).

## 카탈로그 원천 경계

V-World 와 data.go.kr 의 카탈로그 연동은 파운데이션 이 소유한다
([GZ-ADR-0034](../adr/0034-catalog-ownership-handover-to-foundation-platform.md),
[GZ-ADR-0048](../adr/0048-horizontal-platform-redefinition.md)). 공짱 은 클라이언트·예약 수집·원자료 보관·변경
감시를 추가하지 않고 카탈로그 사실을 파운데이션 공개 계약으로만 소비한다.

허용:

- 카탈로그 HTTP API 고정 계약 `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- 이벤트 고정 계약 `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`
- 공짱 읽기 모델로 가져오는 불변 PNU 기준점 산출물, 파운데이션 건물 응답을 공짱 응답 모양으로 바꾸는 라우트 번역

금지:

- 카탈로그 데이터를 위한 V-World·data.go.kr 직접 HTTP 호출
- 카탈로그 클라이언트나 대체 부패 방지 계층 크레이트, 건축물대장 동기화 작업
- `parcel_external_data` 쓰기, 원문 저장 바이너리, R2 원자료 보관 기록기
- 원천 전용 어긋남 점검 워크플로

파운데이션 이 자격 증명·할당량, 요청·응답 해석, 원문 응답 계보, 스키마 변경 감시, 정본 필지·건물
사실과 공개·기준 공간 레이어를 소유한다. 공짱 은 매물 의미, 매물 표식 제공, 그에 필요한 PNU 기준점 읽기
모델 사본을 소유한다.

원천 동작이 바뀌면 파운데이션 을 먼저 갱신하고, 공짱 은 바뀐 공개 계약에 맞춰 고정 계약만 갱신한다.

지키는 장치:

- `tools/semgrep/rules/gongzzang-catalog-boundary.yml`(루트 Semgrep 규칙) — 카탈로그 도메인 크레이트 재도입, 카탈로그 표 직접 쓰기,
  카탈로그 변경 라우트를 거부한다(CI `gongzzang-ci.yml`).
- `docs/architecture/foundation-platform-boundary.v1.json` — 경로 소유권 목록. `cargo run -p repo-guard -- capability-layout`
  이 도메인 크레이트와 대조한다.
- `crates/foundation-platform-client/tests/catalog_contract_pin.rs` — 고정 계약을 크레이트의 파운데이션 OpenAPI 스냅숏(`openapi/catalog.v1.json`)과 대조한다.

## 기본 시스템 규칙

공짱 이 소유한 외부 호출은 제한 시간·재시도·회로 차단기·관측성·감사/기록 규칙을 사용한다.

## 에이전트 전용 규칙

MCP 도구는 개발 중 탐색에 쓸 수 있지만 `apps/`, `services/`, `crates/`, `packages/`가 MCP/LLM SDK 를
import하면 안 된다([GZ-ADR-0010](../adr/0010-scope-information-platform-option-a.md)). 탐색용 MCP:
[korean-land-mcp](./korean-land-mcp.md), 법령 MCP 는 [korean-law.md](./korean-law.md)의 에이전트 경로 절.
