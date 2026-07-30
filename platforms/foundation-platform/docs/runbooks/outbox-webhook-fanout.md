---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Outbox 웹훅 전달 운영 런북

`foundation-outbox-publisher`는 매니페스트가 아닌 Catalog 이벤트와 직원 Identity 이벤트를 HTTP 웹훅
엔드포인트로 전달한다. Gongzzang·Dawneer의 소비자 캐시 무효화와 Gongzzang의 필지 마커 앵커 가져오기
작업 등록에 사용한다. 벡터 타일 매니페스트 승격·롤백 이벤트는 먼저 정본 R2 포인터를 발행한다.

## 설정

Set semicolon-separated `name=url` pairs:

```bash
export FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_ENDPOINTS="gongzzang=https://gongzzang.example.invalid/foundation-platform/events;dawneer=https://dawneer.example.invalid/foundation-platform/events"
export FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET="<shared-webhook-hmac-secret>"
```

원격 엔드포인트는 `https`여야 한다. 일반 `http`는
`http://127.0.0.1:3000/foundation-platform/events` 같은 로컬 루프백 개발 주소에서만 허용한다.

## 실행

```bash
export DATABASE_URL="postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
cargo run -p foundation-outbox-publisher -- run
```

퍼블리셔는 outbox 이벤트 하나마다 JSON 봉투 하나를 전송하며 다음 헤더를 포함한다.

- `x-foundation-platform-event-id`
- `x-foundation-platform-event-type`
- `x-foundation-platform-outbox-scope`
- `x-foundation-platform-signature`
- `x-foundation-platform-timestamp`

`x-foundation-platform-signature` 값은 `v1=<hmac_sha256_hex>` 형식이다. 서명 대상은
`<x-foundation-platform-timestamp>.<raw JSON request body>`이며
`FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET`으로 HMAC-SHA256을 계산한다. 시크릿은 배포 시크릿
저장소에 두고 소비자의 `FOUNDATION_PLATFORM_WEBHOOK_SECRET`과 함께 교체한다.

소비자용 봉투 fixture는 `docs/events/webhook/outbox-webhook-envelope.v1.example.json`이며 CI에서 검증한다.

수신자 계약 fixture는 `docs/events/webhook/receiver-contract.v1.example.json`이다. Gongzzang·Dawneer
수신자 slug, 엔드포인트 경로, 필수 멱등성 키, 허용할 2xx 확인 응답, 최대 확인 지연시간, 확인 본문,
캐시 무효화 효과와 앵커 가져오기 등록 효과를 기록하며 CI에서 검증한다.

2xx가 아닌 응답은 발행 시도를 실패로 처리하고 `retry_count`를 늘리며, 재시도할 수 있도록 이벤트를
미발행 상태로 둔다.

## 필지 마커 앵커 스냅샷 이벤트

`export-parcel-marker-anchor-artifacts`는 변경 불가 앵커 JSONL 객체와 `manifest.json`을 기록한 뒤
`catalog.parcel_marker_anchor.snapshot.published.v1` 이벤트를 `catalog.outbox_event`에 넣는다.
outbox worker가 이를 Gongzzang으로 전달하고, 수신자는 내구성 있는 앵커 가져오기 작업으로 저장한다.

내보내기 명령은 절대 artifact 기본 URL을 요구한다. 소비자가 제공기관별 객체 키를 알 필요가 없게 하기
위해서다.

```bash
export FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_PUBLIC_BASE_URL="https://static.foundation-platform.example.com"
```

발행 payload는 다음 필드를 사용한다.

- `anchor_snapshot_id`: `anchor-snapshot-<export_run_id>`
- `source_geometry_version`: the configured source snapshot id
- `artifact_manifest_url`: public base URL plus the versioned manifest object key
- `artifact_checksum_sha256`: the export manifest checksum
- `row_count`: accepted anchor row count

이 outbox 이벤트를 우회해 Gongzzang을 직접 호출하지 않는다. outbox 행이 재시도·재생·감사의 경계다.

## 검증

```bash
cargo test -p foundation-outbox --test webhook_broadcaster
cargo test -p foundation-outbox-publisher webhook_endpoint_specs
cargo test -p foundation-outbox-publisher outbox_record_is_derived_from_export_summary
```

이벤트 스키마 호환성과 웹훅 봉투·수신자 계약 fixture는 CI에서 검증한다.

이 검사는 발신자 봉투 구조, 추적 헤더, HTTPS/루프백 URL 정책, 2xx가 아닌 응답의 재시도를 검증한다.
Gongzzang이나 Dawneer가 실제 수신 엔드포인트를 배포했다는 뜻은 아니다.

로컬 DB를 사용하는 smoke는 `catalog.outbox_event` 행을 넣고 `OutboxWorker.tick()`을 실행한 뒤 로컬
HTTP 수신자에 전송하고 행을 발행 완료로 표시한다.

```bash
export DATABASE_URL="postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
cargo test -p foundation-outbox --test publish_roundtrip tick_delivers_catalog_event_to_webhook_and_marks_published_at -- --ignored --exact
```

이 검사는 실제 outbox worker를 통한 foundation-platform 발신 전달을 증명한다. Gongzzang·Dawneer의
수신 엔드포인트 구현·배포까지 증명하지는 않는다. M3.2 전환 전에는 지원하는 모든 수신자 계약 이벤트를
배포된 소비자에 보내고, 멱등성 있는 캐시 무효화와 앵커 가져오기 등록을 확인하는 교차 저장소 E2E를
실행해야 한다.

### 배포 수신자 E2E (교차 저장소)

> 2026-06-21 기록: PowerShell 외부 사전조건 검사기, 원격 웹훅 수신자 E2E smoke 실행기와 GitHub
> `consumer_receiver_e2e` 전환 증거 workflow는 형식적인 절차라서 모두 제거했다.

전환 완료를 주장하기 전 배포 수신자 E2E는 여전히 필요하지만, 이제 소비자 저장소가 실행 주체다.
각 소비자(`gongzzang`, `dawneer`)는 다음을 충족해야 한다.

- `/foundation-platform/events` 수신 엔드포인트를 노출하고
  `FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET`을 사용해야 한다.
- `docs/events/webhook/receiver-contract.v1.example.json`의 모든 이벤트(골드 포인터 캐시 무효화와
  필지 마커 앵커 스냅샷 작업 등록)를 받아 계약 지연시간 안에 요구된 확인 본문을 반환해야 한다.
- 자체 테스트에서 캐시 무효화가 멱등적이며 실제 캐시 계층에 연결됐고, 앵커 스냅샷이 내구성 있는
  가져오기 작업을 등록한다는 것을 증명해야 한다.

루프백·문서·자리표시자 호스트는 배포 수신자 증거로 인정하지 않는다.
