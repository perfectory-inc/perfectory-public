---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# 관측성

이 문서는 현재 Gongzzang 관측성 표면을 정리한다.

## 1. 목표

관측성은 다음을 지원해야 한다.

- request tracing
- route 수준 장애 진단
- mutation/audit 재구성
- cache·의존성 readiness 검사
- 운영 승격 증거

## 2. 요청 식별자

Rust API 요청은 request-id 계층을 통과한다.

주요 파일:

- `services/gongzzang-api/src/app.rs`
- `services/gongzzang-api/src/http/request_id.rs`
- `services/gongzzang-api/src/http/mutation_ctx.rs`

`MutationContext`는 correlation 데이터를 repository로 전달해 같은 트랜잭션 안에서 쓰기와 audit record,
outbox event를 함께 만들 수 있게 한다.

## 3. Logging과 tracing

Rust 서비스는 `tracing`과 `tracing-subscriber`를 사용한다.

예시:

- `services/gongzzang-api/src/startup.rs`
- `services/gongzzang-api/src/app.rs`
- `services/gongzzang-outbox-publisher/src/main.rs`
- `crates/circuit-breaker/src/execute.rs`
- `crates/circuit-breaker/src/breaker.rs`

프론트엔드는 panel 상호작용을 위한 가벼운 OpenTelemetry helper를 갖는다.

- `apps/web/lib/observability/tracer.ts`
- `apps/web/lib/panel/telemetry.ts`
- `apps/web/instrumentation.ts`

## 4. Health와 metric

Rust API health route:

- `/healthz`
- `/readyz`
- `/readyz/db`
- `/internal/metrics`

주요 파일:

- `services/gongzzang-api/src/routes/health.rs`
- `services/gongzzang-api/src/routes/metrics.rs`
- `docs/architecture/traffic-auth-policy-registry.v1.json`

Readiness는 설정된 DB와 Redis를 검사하고 liveness는 가벼워야 한다.

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

## 6. Catalog Observability Boundary

Catalog 공개 API 변경 관측성은 Gongzzang이 아니라 Foundation Platform 소유다.

Gongzzang은 다음을 다시 만들지 않는다.

- Gongzzang 소유 Catalog API-drift workflow
- Foundation Platform-owned `api-health` capability
- `crates/api-health-recorder`
- `crates/gongzzang-persistence/src/api_health.rs`
- `docs/observability/api-drift-smoke-test.md`

이 경계는 `scripts/lefthook/foundation-ownership-boundary.sh`와
`docs/architecture/foundation-platform-boundary.v1.json` 계약이 강제한다.

## 7. Current Gaps

저장소에는 tracing·health·audit·policy registry와 부하 테스트 증거 골격이 있다.

남은 hardening 영역:

- full OTel collector/exporter wiring은 완료된 runtime 배포로 증명되지 않았다.
- production SLO dashboard와 alert route는 이 감사에서 아직 증명되지 않았다.

## 8. Guardrails

traffic/auth policy registry와 Foundation Platform 경계는 CI와 pre-commit에서 강제한다. Foundation
Platform Catalog 경계는
`scripts/lefthook/foundation-ownership-boundary.sh`; the traffic/auth policy artifacts are
regenerated with `cargo run -p gongzzang-api --bin generate-traffic-auth-policy`.
