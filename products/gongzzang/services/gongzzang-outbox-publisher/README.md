---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# services/gongzzang-outbox-publisher

공짱 outbox 이벤트 발행 daemon. `crates/gongzzang-outbox`의 `tick`을 interval loop에서 호출해요.

## 환경변수

| 변수 | 기본 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | `Postgres` 접속 문자열 |
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | tick 주기 (ms) |
| `OUTBOX_BATCH_SIZE` | `100` | tick 당 claim limit |
| `OUTBOX_LEASE_SECONDS` | `60` | claim lease 길이 (초) |
| `OUTBOX_LAKEHOUSE_REGISTRY_ENABLED` | 운영이면 `true`, 아니면 `false` | listing photo lakehouse sink 사용 여부 |
| `APP_ENV` / `NODE_ENV` | — | `production`이면 운영으로 판단 |
| `FOUNDATION_PLATFORM_API_BASE_URL` | (sink 사용 시 필수) | Foundation Platform lakehouse registry API |
| `FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE` | (sink 사용 시 필수) | workload identity 토큰 파일 |
| `LISTING_PHOTO_R2_ACCOUNT_ID` / `_ACCESS_KEY` / `_SECRET_KEY` / `_BUCKET` | (sink 사용 시 필수) | 매물 사진 R2 읽기 설정 |
| `RUST_LOG` | `info` | `tracing-subscriber` env filter |

변수 정본은 `src/main.rs`와 `src/listing_photo_lakehouse.rs`예요.

## 기동

```bash
cargo run -p gongzzang-outbox-publisher
```

기동 시 `migrations/20260719000120_outbox_delivery_leases.sql`이 적용됐는지 확인하고, 없으면 시작하지 않아요.

## 종료

`SIGTERM` (Unix) / `Ctrl+C` 로 graceful shutdown — 진행 중 tick 완료 후 종료.

## 발행 대상

- 기본 sink는 `LoggingSink` — `tracing::info!` 로 구조화 event 발행해요 (target = `outbox.publish`).
- lakehouse registry가 켜지면 `LoggingSink`와 `ListingPhotoLakehouseSink`에 함께 발행해요(fan-out).
  후자는 `listing_photo.upload_confirmed` 이벤트의 R2 객체를 읽어 Foundation Platform lakehouse registry에 등록해요.

## 전달 의미

- 여러 인스턴스가 `SELECT … FOR UPDATE SKIP LOCKED` + lease(`lease_owner`, `lease_until`)로 같은 row를
  중복 claim하지 않아요. 전달은 at-least-once — 죽은 worker의 row는 lease 만료 뒤 다시 claim돼요.
- 시도 횟수 컬럼·DLQ, Prometheus 지표, 외부 메시지 브로커 sink는 없어요.

Current delivery semantics: [ADR-0032](../../docs/adr/0032-eventual-consistency-strategy.md).
실행 동작은 이 서비스와 `crates/gongzzang-outbox` 테스트가 소유한다.
