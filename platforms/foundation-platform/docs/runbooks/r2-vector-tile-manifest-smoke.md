# R2 Vector Tile Manifest Smoke

## 목적

`foundation-platform`가 Cloudflare R2에 접근해 object를 실제로 쓸 수 있는지 확인한다.
이 smoke는 runtime pointer인 `gold/manifest.json`을 건드리지 않는다.

## 필요한 환경변수

```text
R2_ACCOUNT_ID=
R2_BUCKET_NAME=
R2_ENDPOINT=
R2_REGION=auto
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_SMOKE_OBJECT_KEY=gold/_smoke/foundation-platform-r2-smoke.json
```

`R2_ENDPOINT`를 지정하지 않으면 `R2_ACCOUNT_ID`로
`https://<account_id>.r2.cloudflarestorage.com` endpoint를 만든다.

## 실행

```bash
cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- smoke-r2
```

테스트 러너에서 live R2 round-trip ignored test까지 실행하려면 추가로 opt-in 한다.
이 값이 없으면 `cargo test -- --ignored` 를 돌려도 실제 R2 write/read/delete 는 skip 된다.

```bash
export FOUNDATION_PLATFORM_R2_LIVE_SMOKE="1"
cargo test -p foundation-outbox --test r2_smoke_contract \
  r2_smoke_round_trip_writes_reads_and_deletes_a_dedicated_object -- --ignored
```

성공 조건:

- dedicated smoke object key에 write 성공
- 같은 key에서 read 성공
- read body가 write body와 byte-for-byte 동일
- smoke object delete 성공

## 안전장치

기본 key는 `gold/_smoke/foundation-platform-r2-smoke.json` 이다.
`FOUNDATION_PLATFORM_R2_SMOKE_OBJECT_KEY`를 바꿀 수 있지만 다음 값은 거부한다.

- 빈 문자열
- `/`로 시작하는 key
- `..` 또는 `\` 를 포함하는 key
- `gold/manifest.json`

`gold/manifest.json`은 Catalog outbox가 promote/rollback 이벤트를 처리할 때만 쓴다.

## R2 전 내부 publish smoke

R2 자격증명이 없을 때도 DB outbox에서 active manifest를 다시 읽고 canonical pointer
write 요청을 만드는 경로는 검증할 수 있다.

```bash
export DATABASE_URL="postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
cargo test -p foundation-outbox --test publish_roundtrip tick_publishes_active_vector_tile_manifest_pointer_from_catalog_outbox -- --ignored
```

이 테스트는 실제 R2에 쓰지 않는다. recording object storage adapter로
`gold/manifest.json`, `application/json`, `no-cache, max-age=0`, manifest body를 검증한다.
