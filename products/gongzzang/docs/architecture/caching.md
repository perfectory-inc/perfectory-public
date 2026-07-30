---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# 캐시

이 문서는 현재 캐시와 최신성 모델을 설명한다.

## 1. 규칙

캐시는 가속기이며 정본이 아니다.

정본 데이터는 다음에 남는다.

- Gongzzang 제품 기록은 Gongzzang Postgres;
- Catalog 사실과 PNU 앵커는 Foundation Platform;
- 변경 불가 미디어·데이터 산출물은 R2/레이크하우스 객체;
- Valkey 8(Redis protocol)은 캐시·세션·rate limit·lock·inbox 중복 제거만 사용한다.

## 2. 런타임 캐시 사용처

현재 Valkey 8 사용처:

- Next.js session storage: `apps/web/lib/session/store.ts`
- session refresh single-flight: `apps/web/lib/session/single-flight.ts`
- frontend/API proxy rate limiting: `apps/web/lib/ratelimit.ts`
- Foundation Platform event inbox dedupe: `apps/web/lib/foundation-platform/event-inbox.ts`
- Rust API JTI denylist: `crates/product-identity-infrastructure/src/jti_denylist.rs`
- Rust API backend rate limit: `services/gongzzang-api/src/backend_rate_limit.rs`
- listing marker serving cache/single-flight: `services/gongzzang-api/src/listing_marker_serving`

## 3. 운영 Valkey 요구사항

Rust API 시작 시 환경별 Valkey 누락 처리는 다르다.

- development: Valkey 의존 검사는 완화되거나 건너뛸 수 있다.
- production: `REDIS_URL`이 없고 보안이 fail-open될 수 있는 곳에서는 즉시 실패한다.

Important file:

- `services/gongzzang-api/src/startup.rs`

Next.js env validation은 production-safe Valkey URL도 요구한다(`REDIS_URL`은 Redis protocol
환경변수 이름으로 유지한다).

Important file:

- `apps/web/lib/env.ts`

## 4. 마커 캐시 모델

매물 마커 제공은 보조 보호 수단으로 캐시와 single-flight를 사용한다.

주 확장 전략은 다음이다.

- precomputed/derived serving indexes;
- stable filter hashes;
- tile-shaped marker requests;
- delta/tombstone overlays for freshness.

숫자 필터가 제한 없는 cache-only 정합성을 만들면 안 된다. 캐시는 반복 요청을 빠르게 할 수 있지만
정합성은 index와 정규화된 필터 계약에서 나와야 한다.

## 5. Foundation Platform 캐시 무효화

Foundation Platform 이벤트는 Catalog 관련 캐시를 무효화하고 앵커 projection 가져오기를 시작할 수 있다.

Important files:

- `apps/web/app/foundation-platform/events/route.ts`
- `apps/web/lib/foundation-platform/event-inbox.ts`
- `services/gongzzang-api/src/routes/foundation_events.rs`
- `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`

## 6. 정적 타일 캐시

정적 기준 타일 수명 주기는 Foundation Platform이 소유한다.

Gongzzang 지도 client는 manifest와 불변 tile URL을 소비할 수 있지만 Foundation Platform 기준 타일의
build/promote 수명주기는 소유하지 않는다.

## 7. 가드

traffic/auth, Foundation event-receiver, PNU-anchor PBF marker 계약은 CI와 pre-commit hook에서 강제한다.
Foundation 경계는 `scripts/lefthook/foundation-ownership-boundary.sh`가 지키며 traffic/auth 정책
artifact는 registry에서 `cargo run -p gongzzang-api --bin generate-traffic-auth-policy`로 다시 만든다.
