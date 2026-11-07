---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# services/gongzzang-api

공짱 HTTP API 서버 (Axum). `app.rs`가 도메인 라우트와 헬스 라우트
(`/healthz` · `/readyz` · 비운영 전용 `/readyz/db`)를 조립하는 SSOT다.

## 구조 (도메인 그룹 수준)

- `app.rs` — 라우터 조립 SSOT (전 route 등록 지점)
- `routes/` — 도메인별 핸들러: health, metrics(`/internal/metrics`), users, listings, admin_listings,
  listing-marker 7종(tiles·counts·deltas·masks·filters·tombstones·common), bookmarks,
  notifications, complexes(`/api/complexes`), foundation_events, auth_event. 모듈 목록 정본은 `main.rs`의 `mod routes`.
- `http/` — Problem Details, request id, mutation context 등 HTTP 공통
- 인프라 모듈 — startup, observability, photo_upload, foundation_anchor_import,
  foundation_parcel_lookup, complex_reader, backend_authorization, backend_rate_limit,
  traffic_auth_policy, listing_marker_policy, listing_marker_serving
- `bin/` — `generate-traffic-auth-policy`(정책 산출물 6종 재생성; CI가 drift 검사),
  `export-gongzzang-openapi`(`openapi.json` 생성), `foundation_anchor_import`,
  `listing_photo_content_address_backfill`(옛 id 키 매물 사진을 내용주소 저장소로 복사하고 대응을 쌓음,
  지우지 않음; 공짱 운영자가 손으로 돌린다 — 루트 ADR-0427 배포 순서 ③)

## 로컬 실행

```bash
# products/gongzzang 루트에서 — .env의 DATABASE_URL 사용
bash scripts/sqlx-migrate.sh      # DB 생성 + 마이그레이션
cargo run -p gongzzang-api        # API_LISTEN_ADDR, 기본 0.0.0.0:8080
curl http://localhost:8080/healthz
```

## 테스트

CI와 같은 명령은 모노레포 루트의 `cargo xtask verify gongzzang`이다(DB 없는 스위트; 단계 정의 정본은
`tools/xtask`). DB가 필요한 persistence 통합 시험은 `cargo xtask integration gongzzang postgres`.

Docker 하네스(모노레포 루트): `bash scripts/verify/cargo-verify.sh products/gongzzang`

## 생성 산출물 경로

- **트래픽/인증 정책**: `cargo run -p gongzzang-api --bin generate-traffic-auth-policy` —
  SSOT `docs/architecture/traffic-auth-policy-registry.v1.json`에서 산출물 6종을 재생성한다.
- **OpenAPI**: `cargo run -p gongzzang-api --bin export-gongzzang-openapi -- services/gongzzang-api/openapi.json`
  이 utoipa 문서를 커밋된 `openapi.json`으로 쓴다. 같은 bin의 시험이 커밋본과 생성본이 같은지 검사한다.
- **OpenAPI → TS 타입**: `packages/api-types`의 `pnpm generate`는 `openapi.json`을 입력으로 요구하며
  파일이 없으면 **의도적으로 실패**한다 (자리표시 생성 금지).
