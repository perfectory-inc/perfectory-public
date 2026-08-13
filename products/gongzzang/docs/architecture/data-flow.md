---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# 데이터 흐름

이 문서는 현재 Gongzzang 요청·데이터 경로를 정리한다.

## 1. 제품 요청 경로

```text
Browser
  -> Next.js app / proxy
  -> Gongzzang Rust API
  -> Gongzzang domain port
  -> Gongzzang repository or approved external adapter
  -> response
```

주요 런타임 파일:

- `apps/web/proxy.ts`
- `apps/web/app/api/proxy/[...path]/route.ts`
- `services/gongzzang-api/src/app.rs`
- `services/gongzzang-api/src/routes`
- `crates/*-domain`
- `crates/gongzzang-persistence`

브라우저가 임의의 경로 지식을 갖고 Rust API를 직접 호출하지 않는다. 공개 proxy와 경로 노출 정책은
다음 파일이 통제한다.

- `docs/architecture/traffic-auth-policy-registry.v1.json`
- `docs/architecture/platform-integration/route-exposure-policy.v1.json`
- `apps/web/lib/policies/traffic-auth-policy.generated.ts`
- `services/gongzzang-api/src/traffic_auth_policy.rs`

## 2. 매물 변경 경로

```text
Browser form/action
  -> Next.js proxy
  -> Rust API listing route
  -> Listing domain aggregate
  -> PgListingRepository
  -> Postgres transaction
       -> listing table
       -> audit_log
       -> outbox_event
```

변경 context와 추적 정보는 `MutationContext`로 전달한다.

Important files:

- `services/gongzzang-api/src/routes/listings`
- `crates/listing-domain`
- `crates/gongzzang-persistence/src/listing`
- `crates/audit-log-domain`
- `crates/outbox-event-domain`

## 3. Foundation Platform Catalog 조회 경로

```text
Gongzzang route
  -> Gongzzang Foundation Platform adapter
  -> Foundation Platform published API
  -> Gongzzang-owned DTO/read model
```

Gongzzang은 V-World나 data.go.kr Catalog API를 직접 호출하지 않는다.

Current approved adapters:

- `services/gongzzang-api/src/foundation_parcel_lookup.rs`
- `services/gongzzang-api/src/building_reader.rs`

Current supporting policies:

- `docs/architecture/foundation-platform-boundary.v1.json`
- `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- `docs/backend/circuit-breaker.md`

## 4. Foundation Platform 이벤트 경로

```text
Foundation Platform event
  -> Next.js public receiver
  -> Rust internal API
  -> foundation_platform_event_inbox
  -> anchor projection import / cache invalidation
```

Important files:

- `apps/web/app/foundation-platform/events/route.ts`
- `apps/web/lib/foundation-platform/event-inbox.ts`
- `services/gongzzang-api/src/routes/foundation_events.rs`
- `services/gongzzang-api/src/foundation_anchor_import.rs`
- `migrations/20260719000118_foundation_platform_event_inbox_anchor_import.sql` (current fresh-schema creation)

이벤트 수신자는 멱등적이고 서명으로 보호돼야 한다.

## 5. 매물 마커 데이터 경로

```text
Foundation Platform PNU anchor projection
  + Gongzzang listing semantics
  -> listing marker projection/index
  -> listing marker tile/count/mask/delta/tombstone API
  -> map client vector source
```

Important files:

- `crates/gongzzang-persistence/src/foundation_anchor.rs`
- `crates/gongzzang-persistence/src/listing/marker_*`
- `services/gongzzang-api/src/listing_marker_serving`
- `services/gongzzang-api/src/routes/listing_marker_*`
- `apps/web/lib/map/marker-tile-contract.ts`
- `apps/web/lib/map/marker-tile-style.ts`

공개 마커 경로는 출시 요청 형식으로 `bbox`나 `bounds`를 사용하지 않는다.

## 6. 미디어·레이크하우스 경로

```text
Listing photo lifecycle
  -> R2 object operation
  -> Gongzzang lakehouse/media namespace
  -> Foundation Platform lakehouse registry integration
```

Important files:

- `services/gongzzang-api/src/photo_upload.rs`
- `services/gongzzang-outbox-publisher/src/listing_photo_lakehouse.rs`
- `services/gongzzang-outbox-publisher/src/foundation_platform_lakehouse_registry.rs`
- `docs/architecture/platform-integration/lakehouse-registry-policy.v1.json`

## 7. 가드

데이터 흐름 소유권이 바뀌어도 Foundation Platform 경계·의존성 경계·플랫폼 연동 정책·PNU 앵커 PBF
마커 계약·traffic/auth 정책 레지스트리는 유지해야 한다. Catalog 경계는
`scripts/lefthook/foundation-ownership-boundary.sh`와
`docs/architecture/foundation-platform-boundary.v1.json` 계약으로 강제한다.
