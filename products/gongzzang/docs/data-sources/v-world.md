---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# V-World 원천 경계

V-World는 Foundation Platform Catalog 입력 원천이다.

Gongzzang은 V-World client·예약 작업·원자료 보관·변경 감시를 추가하지 않는다. Catalog 사실은
Foundation Platform 공개 계약으로만 소비한다.

## Gongzzang 계약

허용 사용:

- Foundation Platform Catalog HTTP API 고정 계약:
  `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- Foundation Platform event 고정 계약:
  `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`
- Gongzzang read model로 가져오는 불변 PNU anchor artifact

금지 사용:

- V-World 직접 HTTP 호출
- `vworld-client` 또는 대체 Catalog ACL crate
- `parcel_external_data` 쓰기
- raw capture binary 또는 R2 raw archive writer
- V-World 전용 drift smoke workflow

## 소유권

Foundation Platform 소유:

- V-World credential과 quota 처리
- request/response parsing
- raw response lineage
- schema drift monitoring
- 정본 필지 geometry와 public/reference spatial layer

Gongzzang 소유:

- 매물 의미
- 매물 marker 제공
- 매물 marker 제공에 필요한 PNU anchor read-model 복사본

## 가드

- Foundation Platform catalog boundary — `scripts/lefthook/foundation-ownership-boundary.sh`
- Foundation Platform boundary / dependency-boundary contract — `docs/architecture/foundation-platform-boundary.v1.json`
- Foundation Platform Catalog API consumer contract — `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

V-World 원천 동작이 바뀌면 먼저 Foundation Platform을 갱신한다. Gongzzang은 Foundation Platform
API/이벤트 계약이 바뀐 뒤 고정된 계약만 갱신한다.
