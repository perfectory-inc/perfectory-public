---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# services/gongzzang-outbox-publisher

공짱 아웃박스 이벤트를 발행하는 상주 프로세스. `crates/gongzzang-outbox`의 `tick`을 일정 주기로 호출해요.

## 환경 변수

| 변수 | 기본 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | `Postgres` 접속 문자열 |
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | `tick` 주기 (밀리초) |
| `OUTBOX_BATCH_SIZE` | `100` | `tick` 한 번에 가져가는 행 수 상한 |
| `OUTBOX_LEASE_SECONDS` | `60` | 가져간 행의 임차 길이 (초) |
| `OUTBOX_LAKEHOUSE_REGISTRY_ENABLED` | 운영이면 `true`, 아니면 `false` | 매물 사진을 레이크하우스로 보내는 출구를 쓸지 |
| `APP_ENV` / `NODE_ENV` | — | `production`이면 운영으로 본다 |
| `FOUNDATION_PLATFORM_API_BASE_URL` | (출구를 쓸 때 필수) | 파운데이션 레이크하우스 등록 API |
| `FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE` | (출구를 쓸 때 필수) | 워크로드 신원 토큰 파일 |
| `LISTING_PHOTO_R2_ACCOUNT_ID` / `_ACCESS_KEY` / `_SECRET_KEY` / `_BUCKET` | (출구를 쓸 때 필수) | 매물 사진 R2 읽기 설정 |
| `RUST_LOG` | `info` | `tracing-subscriber` 의 환경 변수 필터 |

변수 정본은 `src/main.rs`와 `src/listing_photo_lakehouse.rs`예요.

## 기동

```bash
cargo run -p gongzzang-outbox-publisher
```

기동 시 `migrations/20260719000120_outbox_delivery_leases.sql`이 적용됐는지 확인하고, 없으면 시작하지 않아요.

## 종료

`SIGTERM` (Unix) / `Ctrl+C` 로 정상 종료 — 진행 중인 `tick` 을 마친 뒤 끝나요.

## 발행 대상

- 기본 출구는 `LoggingSink` — `tracing::info!` 로 구조화된 이벤트를 남겨요 (대상 = `outbox.publish`).
- 레이크하우스 등록이 켜지면 `LoggingSink`와 `ListingPhotoLakehouseSink`에 함께 발행해요(갈래 발행).
  후자는 `listing_photo.upload_confirmed` 이벤트의 R2 객체를 읽어 파운데이션 레이크하우스 등록부에 올려요.

## 전달 의미

- 여러 인스턴스가 `SELECT … FOR UPDATE SKIP LOCKED` + 임차(`lease_owner`, `lease_until`)로 같은 행을
  두 번 가져가지 않아요. 전달은 적어도 한 번 — 죽은 작업자의 행은 임차가 끝난 뒤 다시 가져가져요.
- 시도 횟수 칼럼·실패 대기열, Prometheus 지표, 외부 메시지 중개자로 가는 출구는 없어요.

지금의 전달 의미: [ADR-0032](../../docs/adr/0032-eventual-consistency-strategy.md).
실행 동작은 이 서비스와 `crates/gongzzang-outbox` 시험이 소유해요.
