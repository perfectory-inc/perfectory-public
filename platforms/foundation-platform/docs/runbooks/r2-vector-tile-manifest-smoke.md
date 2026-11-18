---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-05
---

# R2 벡터 타일 매니페스트 점검

## 목적

`foundation-platform`가 Cloudflare R2 에 붙어 객체를 실제로 쓸 수 있는지 확인한다.
이 점검은 실행 포인터인 `gold/manifest.json`을 건드리지 않는다.

## 필요한 환경 변수

```text
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID=
FOUNDATION_PLATFORM_RUNTIME_ENV=local|ci|staging|production
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev  # local process uses the remote dev bucket
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION=auto
FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_SMOKE_OBJECT_KEY=gold/_smoke/foundation-platform-r2-smoke.json
```

`FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT`를 주지 않으면 `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID`로
`https://<account_id>.r2.cloudflarestorage.com` 호출 주소를 만든다.

## 실행

```bash
cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- smoke-r2
```

시험 실행기에서 실제 R2 왕복 `#[ignore]` 시험까지 돌리려면 동의 값을 더 넣는다.
이 값 없이 `--ignored` 로 돌리면 시험은 건너뛰지 않고 실패한다. 레인으로 돌릴 때는
`cargo xtask integration foundation r2`(또는 `cargo xtask live r2-lakehouse`)가 동의 값을 넣는다.

```bash
export FOUNDATION_PLATFORM_R2_LIVE_SMOKE="1"
cargo test -p foundation-outbox --test r2_smoke_contract \
  r2_smoke_round_trip_writes_reads_and_deletes_a_dedicated_object -- --ignored
```

성공 조건:

- 전용 점검 객체 키에 쓰기 성공(시험은 `gold/_smoke/foundation-platform-r2-smoke-<uuid>.json`
  고유 키를 쓴다)
- 같은 키에서 읽기 성공
- 읽은 본문이 쓴 본문과 바이트 단위로 같다
- 점검 객체 삭제 성공

## 안전장치

기본 키는 `gold/_smoke/foundation-platform-r2-smoke.json` 이다.
`FOUNDATION_PLATFORM_R2_SMOKE_OBJECT_KEY`를 바꿀 수 있지만 다음 값은 거부한다.

- 빈 문자열
- `/`로 시작하는 키
- `..` 또는 `\` 를 포함하는 키
- `gold/manifest.json`

`gold/manifest.json`은 카탈로그 아웃박스가 승격·되돌리기 이벤트를 처리할 때만 쓴다.

## R2 없이 하는 내부 발행 점검

R2 자격 증명이 없을 때도 DB 아웃박스에서 활성 매니페스트를 다시 읽고 정본 포인터
쓰기 요청을 만드는 경로는 검증할 수 있다.

```bash
export DATABASE_URL="<platforms/foundation-platform/.env.local.example 형식의 로컬 DATABASE_URL>"
cargo test -p foundation-outbox --test publish_roundtrip tick_publishes_active_vector_tile_manifest_pointer_from_catalog_outbox -- --ignored
```

이 시험은 실제 R2 에 쓰지 않는다. 기록용 객체 저장소 어댑터로
`gold/manifest.json`, `application/json`, `no-cache, max-age=0`, 매니페스트 본문을 검증한다.
