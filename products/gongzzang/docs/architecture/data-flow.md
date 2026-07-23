# Data Flow

This document maps the current Gongzzang request and data paths.

## 1. Product Request Path

```text
Browser
  -> Next.js app / proxy
  -> Gongzzang Rust API
  -> Gongzzang domain port
  -> Gongzzang repository or approved external adapter
  -> response
```

Core runtime files:

- `apps/web/proxy.ts`
- `apps/web/app/api/proxy/[...path]/route.ts`
- `services/gongzzang-api/src/app.rs`
- `services/gongzzang-api/src/routes`
- `crates/*-domain`
- `crates/gongzzang-persistence`

The browser should not talk to the Rust API with ad-hoc route knowledge. Public proxy and route exposure policy are controlled by:

- `docs/architecture/traffic-auth-policy-registry.v1.json`
- `docs/architecture/platform-integration/route-exposure-policy.v1.json`
- `apps/web/lib/policies/traffic-auth-policy.generated.ts`
- `services/gongzzang-api/src/traffic_auth_policy.rs`

## 2. Listing Mutation Path

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

Mutation context and traceability are carried through `MutationContext`.

Important files:

- `services/gongzzang-api/src/routes/listings`
- `crates/listing-domain`
- `crates/gongzzang-persistence/src/listing`
- `crates/audit-log-domain`
- `crates/outbox-event-domain`

## 3. Foundation Platform Catalog Read Path

```text
Gongzzang route
  -> Gongzzang Foundation Platform adapter
  -> Foundation Platform published API
  -> Gongzzang-owned DTO/read model
```

Gongzzang must not call V-World or data.go.kr Catalog APIs directly.

Current approved adapters:

- `services/gongzzang-api/src/foundation_parcel_lookup.rs`
- `services/gongzzang-api/src/building_reader.rs`

Current supporting policies:

- `docs/architecture/foundation-platform-boundary.v1.json`
- `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- `docs/backend/circuit-breaker.md`

## 4. Foundation Platform Event Path

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

The event receiver must be idempotent and signature-protected.

## 5. Listing Marker Data Path

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

Public marker routes must not use `bbox` or `bounds` launch request shapes.

## 6. Media/Lakehouse Path

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

## 7. Guardrails

When data-flow ownership changes, the Foundation Platform boundary, dependency
boundary, platform-integration policy, PNU-anchor PBF marker contract, and
traffic/auth policy registry must stay intact. The Foundation Platform catalog
boundary is enforced by `scripts/lefthook/foundation-ownership-boundary.sh` and the
boundary contract `docs/architecture/foundation-platform-boundary.v1.json`.
