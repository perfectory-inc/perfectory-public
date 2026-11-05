---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-11-05
---

# 관측성

이 문서는 현재 Gongzzang 관측성 표면을 정리한다.

## 1. 목표

관측성은 다음을 지원해야 한다.

- 요청 추적
- route 수준 장애 진단
- mutation/audit 재구성
- cache·의존성 readiness 검사
- 운영 승격 증거

## 2. 요청 식별자

Rust API 요청은 request-id 계층을 통과한다. 들어온 `X-Request-Id` 헤더를 쓰고, 없으면
`req_<ULID>` 를 만들어 tracing span에 붙이고 응답 헤더로 되돌려 준다.

주요 파일:

- `services/gongzzang-api/src/app.rs`
- `services/gongzzang-api/src/http/request_id.rs`
- `services/gongzzang-api/src/http/mutation_ctx.rs`

`MutationContext`는 그 요청 식별자를 DB `correlation_id` 컬럼 값으로 repository에 전달해, 같은
트랜잭션 안에서 쓰기와 audit record, outbox event를 함께 만들 수 있게 한다.

## 3. 로깅·tracing·에러 수집

Rust 서비스는 `tracing`과 `tracing-subscriber`를 사용한다.

예시:

- `services/gongzzang-api/src/startup.rs`
- `services/gongzzang-api/src/app.rs`
- `services/gongzzang-outbox-publisher/src/main.rs`
- `crates/circuit-breaker/src/execute.rs`
- `crates/circuit-breaker/src/breaker.rs`

API의 에러 수집은 Sentry다 (`services/gongzzang-api/src/observability.rs` `init_sentry`, `main.rs`에서
호출). `SENTRY_DSN`이 없으면 꺼진다.

프론트엔드는 OpenTelemetry Node SDK를 서버 런타임에서 띄우고(`apps/web/instrumentation.ts`, 읽기 전용
미리보기 빌드에서는 끔), panel 상호작용용 가벼운 helper를 갖는다(`apps/web/lib/panel/telemetry.ts`).

## 4. Health와 metric

Rust API health route:

- `/healthz`
- `/readyz`
- `/readyz/db` (디버그용, `expose_db_debug_route` 로 켤 때만 — 운영은 뺄 수 있다)
- `/internal/metrics`

주요 파일:

- `services/gongzzang-api/src/routes/health.rs`
- `services/gongzzang-api/src/routes/metrics.rs`
- `docs/architecture/traffic-auth-policy-registry.v1.json`

Readiness는 DB를 검사하고, Valkey(`REDIS_URL`)가 설정된 경우에만 Valkey도 검사한다. liveness는
가벼워야 한다.

## 5. Audit과 outbox

audit에 중요한 쓰기는 다음을 기록해야 한다.

- actor
- action
- resource kind/id
- 가능하면 before/after 상태
- correlation id
- 생성 시각

많은 DB repository가 이미 트랜잭션 기반 `audit_log` + `outbox_event` 패턴을 사용한다.

주요 파일:

- `crates/gongzzang-persistence/src/audit_log.rs`
- `crates/gongzzang-persistence/src/audit_state.rs`
- `crates/gongzzang-persistence/src/admin_action.rs`
- `crates/gongzzang-persistence/src/bookmark.rs`
- `crates/gongzzang-persistence/src/business_verification.rs`
- `crates/gongzzang-persistence/src/featured_content.rs`
- `crates/gongzzang-persistence/src/system_alert.rs`
- `crates/gongzzang-persistence/src/listing`

## 6. Catalog 관측성 경계

Catalog 공개 API 변경 관측성은 Gongzzang이 아니라 Foundation Platform 소유다.

Gongzzang은 다음을 다시 만들지 않는다.

- Gongzzang 소유 Catalog API-drift workflow
- Foundation Platform 소유 `api-health` capability
- `crates/api-health-recorder`
- `crates/gongzzang-persistence/src/api_health.rs`
- `docs/observability/api-drift-smoke-test.md`

이 경로들의 소유자는 `docs/architecture/foundation-platform-boundary.v1.json` `path_ownership`에
`extracted_catalog_observability_asset` 으로 기록되어 있다. 이 목록을 읽어 재도입을 막는 검사는
지금 없다 — `tools/semgrep/rules/gongzzang-catalog-boundary.yml`(루트 Semgrep 규칙)는 Catalog crate·Catalog 표 쓰기·
Catalog 변경 route만 본다.

## 7. 남은 공백

저장소에는 tracing·Sentry·health·audit·policy registry와 부하 테스트 골격이 있다.

남은 hardening 영역:

- Rust 서비스의 OTel exporter는 없다. 웹의 OTel collector 연결은 운영 배포로 증명된 기록이 없다.
- 운영 SLO dashboard와 alert route는 증명되지 않았다.

## 8. 가드

traffic/auth policy registry 산출물은 CI(`gongzzang-ci.yml` `Traffic/auth policy drift`)에서
`cargo run -p gongzzang-api --bin generate-traffic-auth-policy` 후 `git diff --exit-code`로 강제한다.
Foundation Platform Catalog 경계는 `tools/semgrep/rules/gongzzang-catalog-boundary.yml`(루트 Semgrep 규칙, CI
`monorepo-guard` 의 `semgrep-rules`)가 강제한다. 둘 다 커밋 전 훅에서는 돌지 않는다(루트 `lefthook.yml`).
