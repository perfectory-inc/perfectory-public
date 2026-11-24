---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# services/gongzzang-api

공짱 HTTP API 서버 (Axum). `app.rs`가 도메인 라우트와 상태 점검 라우트
(`/healthz` · `/readyz` · 운영 밖 전용 `/readyz/db`)를 조립하는 정본이다.

## 구조 (도메인 그룹 수준)

- `app.rs` — 라우터 조립의 정본 (모든 라우트를 등록하는 곳)
- `routes/` — 도메인별 처리기: `health`, `metrics`(`/internal/metrics`), `users`, `listings`, `admin_listings`,
  매물 표식 7종(`tiles`·`counts`·`deltas`·`masks`·`filters`·`tombstones`·`common`), `bookmarks`,
  `notifications`, `complexes`(`/api/complexes`), `foundation_events`, `auth_event`. 모듈 목록 정본은 `main.rs`의 `mod routes`.
- `http/` — 문제 상세 형식, 요청 식별자, 변경 문맥 등 HTTP 공통
- 인프라 모듈 — `startup`, `observability`, `photo_upload`, `foundation_anchor_import`,
  `foundation_parcel_lookup`, `complex_reader`, `backend_authorization`, `backend_rate_limit`,
  `traffic_auth_policy`, `listing_marker_policy`, `listing_marker_serving`
- `bin/` — `generate-traffic-auth-policy`(정책 산출물 6종 재생성; CI가 어긋남 검사),
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

## 운영 배포에 꼭 있어야 하는 값

- `CLIENT_ADDRESS_HEADER`: 앞문이 클라이언트 주소를 싣는 헤더 이름(Cloudflare 뒤 `cf-connecting-ip`). 속도 제한 열쇠와
  감사 `ip_address` 가 이것으로 정해진다(`src/http/client_address.rs`; `X-Forwarded-For` 는 읽지 않는다). 운영
  (`APP_ENV=production`)에서는 없으면 **시작하지 않는다**: 부하분산기 뒤의 TCP 상대 주소는 부하분산기라, 모든
  사용자가 한 열쇠를 나누게 된다. 원본이 앞문에서 오는 요청만 받을 때만 그 헤더를 믿을 수 있다.

## 시험

CI와 같은 명령은 모노레포 루트의 `cargo xtask verify gongzzang`이다(DB 없는 시험 묶음; 단계 정의 정본은
`tools/xtask`). DB가 필요한 저장소 통합 시험은 `cargo xtask integration gongzzang postgres`.

Docker 실행 틀(모노레포 루트): `bash scripts/verify/cargo-verify.sh products/gongzzang`

## 생성 산출물 경로

- **트래픽/인증 정책**: `cargo run -p gongzzang-api --bin generate-traffic-auth-policy` —
  정본 `docs/architecture/traffic-auth-policy-registry.v1.json`에서 산출물 6종을 다시 만든다.
- **OpenAPI**: `cargo run -p gongzzang-api --bin export-gongzzang-openapi -- services/gongzzang-api/openapi.json`
  이 utoipa 문서를 커밋된 `openapi.json`으로 쓴다. 같은 실행 파일의 시험이 커밋본과 생성본이 같은지 검사한다.
- **OpenAPI → TS 타입**: `packages/api-types`의 `pnpm generate`는 `openapi.json`을 입력으로 요구하며
  파일이 없으면 **일부러 실패**한다 (자리표시로 만들지 않는다).
