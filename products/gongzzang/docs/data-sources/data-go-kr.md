---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# data.go.kr 원천 경계

Catalog 관련 data.go.kr 연동은 Foundation Platform Catalog 입력 원천이다.

Gongzzang은 data.go.kr Catalog client·parser·예약 수집 작업·원자료 보관·변경 감시를 추가하지 않는다.
건물·필지 사실은 Foundation Platform 공개 계약으로만 소비한다.

## Gongzzang 계약

허용 사용:

- Foundation Platform Catalog HTTP API 고정 계약:
  `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- Foundation Platform event 고정 계약:
  `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`
- Foundation 건물 응답을 Gongzzang API 응답 형태로 바꾸는 route 번역

금지 사용:

- Catalog 데이터를 위한 data.go.kr 직접 HTTP 호출
- `data-go-kr-client` 또는 대체 Catalog ACL crate
- building-register sync job
- `parcel_external_data` 쓰기
- raw capture binary 또는 R2 raw archive writer
- data.go.kr 전용 drift smoke workflow

## 소유권

Foundation Platform 소유:

- data.go.kr credential과 quota 처리
- request/response parsing
- raw response lineage
- schema drift monitoring
- 정본 건물·필지 Catalog 사실

Gongzzang 소유:

- `/api/buildings` route 형태
- 매물 의미
- 매물 marker 제공

## 가드

- Foundation Platform catalog boundary — `scripts/lefthook/foundation-ownership-boundary.sh`
- Foundation Platform boundary / dependency-boundary contract — `docs/architecture/foundation-platform-boundary.v1.json`
- Foundation Platform Catalog API consumer contract — `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

data.go.kr 원천 동작이 바뀌면 먼저 Foundation Platform을 갱신한다. Gongzzang은 Foundation Platform
API/이벤트 계약이 바뀐 뒤 고정된 계약만 갱신한다.
