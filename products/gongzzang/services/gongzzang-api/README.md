# services/gongzzang-api

공짱 HTTP API 서버 (Axum). `app.rs`가 도메인 라우트와 헬스 라우트
(`/healthz` · `/readyz` · `/readyz/db`)를 조립하는 SSOT다.

## 구조 (도메인 그룹 수준)

- `app.rs` — 라우터 조립 SSOT (전 route 등록 지점)
- `routes/` — 도메인별 핸들러: health/metrics(`/internal/metrics`), users, listings(+admin),
  listing-marker 7종(tiles·counts·deltas·masks·filters·tombstones·common), bookmarks,
  notifications, parcels, buildings, floors, foundation_events, auth_event
- `http/` — Problem Details 등 HTTP 공통
- 인프라 모듈 — startup, observability, photo_upload, foundation_anchor_import,
  foundation_parcel_lookup, backend_authorization, backend_rate_limit,
  traffic_auth_policy, listing_marker_policy, listing_marker_serving, building_reader
- `bin/` — `generate-traffic-auth-policy` (정책 산출물 6종 재생성; CI가 drift 검사)

## 로컬 실행

```bash
# products/gongzzang 루트에서 — .env의 DATABASE_URL 사용
bash scripts/sqlx-migrate.sh      # DB 생성 + 마이그레이션
cargo run -p gongzzang-api        # API_LISTEN_ADDR, 기본 0.0.0.0:8080
curl http://localhost:8080/healthz
```

## 테스트 — CI 계약과 동일한 2단계

```bash
cargo test --workspace --all-features --exclude gongzzang-persistence  # DB 미접속 스위트
cargo test -p gongzzang-persistence                                    # persistence 단위 스위트
```

Docker 하네스(모노레포 루트): `bash scripts/verify/cargo-verify.sh products/gongzzang`

## 생성 산출물 경로

- **트래픽/인증 정책**: `cargo run -p gongzzang-api --bin generate-traffic-auth-policy` —
  SSOT `docs/architecture/traffic-auth-policy-registry.v1.json`에서 산출물 6종을 재생성한다.
- **OpenAPI → TS 타입**: `packages/api-types`의 `pnpm generate`는
  `services/gongzzang-api/openapi.json`을 입력으로 요구하며 파일이 없으면 **의도적으로 실패**한다
  (자리표시 생성 금지). Rust 측 OpenAPI export(utoipa)는 아직 미구현.
