# services/gongzzang-outbox-publisher

공짱 outbox 이벤트 발행 daemon. SP4-i.

## 환경변수

| 변수 | 기본 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | `Postgres` 접속 문자열 |
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | tick 주기 (ms) |
| `OUTBOX_BATCH_SIZE` | `100` | tick 당 fetch limit |
| `RUST_LOG` | `info` | `tracing-subscriber` env filter |

## 기동

```bash
cargo run -p gongzzang-outbox-publisher
```

## 종료

`SIGTERM` (Unix) / `Ctrl+C` 로 graceful shutdown — 진행 중 tick 완료 후 종료.

## 발행 대상

v1 의 default sink 는 `LoggingSink` — `tracing::info!` 로 구조화 event 발행해요
(target = `outbox.publish`). 운영 시 `Loki` / `Grafana` 가 해당 target 필터로 발행
흐름 모니터링.

진짜 외부 시스템 (`Kafka` / `Webhook` / `SQS` 등) 통합은 후속 sub-project 에서
같은 `Sink` trait 구현체로 추가해요.

## 후속

- 분산 lease claim (`SELECT FOR UPDATE SKIP LOCKED`)은 구현되어 있다. migration
  `migrations/20260719000120_outbox_delivery_leases.sql`을 운영 DB에 적용하고 멀티 인스턴스·lease 만료
  재획득 smoke를 실행하는 단계가 남아 있다.
- 외부 sink 구현체 (Kafka / Webhook / SQS / NATS)
- 재시도 정책 (`attempt_count` 컬럼 + DLQ)
- Circuit breaker 통합
- Prometheus metrics

Current delivery semantics: [ADR-0032](../../docs/adr/0032-eventual-consistency-strategy.md).
Executable behavior lives in this service and `crates/gongzzang-outbox` tests.
