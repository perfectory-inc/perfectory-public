---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# crates/circuit-breaker

공짱 외부 API 호출 표준 미들웨어 — `Policy` + `Breaker` + `execute`.

## 책임

- `execute(&breaker, &policy, op_name, f)` — 호출마다 timeout, 재시도(지수 백오프
  `retry_base_ms * 2^attempt`, jitter 없음), circuit breaking 적용.
- `Breaker` — `Closed` / `Open` / `HalfOpen` 상태 (`CircuitState`).
- `Policy` — timeout·재시도·open 임계값·window·cooldown을 이름 있는 상수로 표현.
  Foundation Platform published API 기본값은 `Policy::foundation_platform_default()`가 정본이에요.
- 오류는 `BreakerError` (`Timeout`, `MaxRetriesExceeded` 등).

Idempotency-Key 검증, 감사 로그, cached-response fallback은 이 crate에 없어요. 필요하면
호출하는 adapter가 소유해요.

## 의존

- `tokio` (`time`), `thiserror`, `tracing`. 다른 미들웨어 라이브러리는 쓰지 않아요.

## 정책

- Gongzzang 소유 외부 API 호출은 이 crate의 `execute`를 통과해요. 직접 `reqwest::Client` 사용은
  `clippy.toml`의 `disallowed-types`가 막고, 정당한 wrapper crate만 crate 단위로 허용해요.
- 각 API 정책은 owning adapter 또는 service boundary의 이름 있는 상수로 둬요.
- `Open` 진입은 `tracing::warn!`으로 기록해요. 알림 연동은 관측성 쪽 몫이에요.

사용처: `foundation-platform-client`, `services/gongzzang-api`, `services/gongzzang-outbox-publisher`.

→ [Circuit Breaker 컨벤션](../../docs/backend/circuit-breaker.md)
