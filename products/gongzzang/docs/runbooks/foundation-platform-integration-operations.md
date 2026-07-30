---
status: current
owner: gongzzang-제품
doc_type: runbook
last_reviewed: 2026-07-29
---

# Foundation Platform 연동 운영 런북

## 범위

이 런북은 Gongzzang 런타임의 Foundation Platform 호출과 Foundation Platform 웹훅의 Gongzzang 전달을
다룬다.

Policy SSOT:
`docs/architecture/platform-integration/operations-policy.v1.json`

## 필수 텔레메트리

모든 연동 span 또는 log event에는 secret이 아닌 routing context를 포함한다.

- `service.name`
- `peer.service`
- `http.request.method`
- `url.path`
- `platform_integration.call_id`
- `platform_integration.policy_id`
- `platform_integration.direction`
- `platform_integration.decision`
- `correlation_id`

Webhook event에는 다음도 포함한다.

- `foundation_platform.event_id`
- `foundation_platform.event_type`

서비스 token·웹훅 secret·cookie·authorization header를 절대 로그에 남기지 않는다.

## SLO

Foundation Platform Catalog 읽기:

- Availability: 99.9%
- p95 latency: 300 ms
- p99 latency: 1000 ms
- Timeout: 5000 ms

Foundation Platform webhook receiver:

- Availability: 99.9%
- p95 latency: 250 ms
- p99 latency: 1000 ms
- Duplicate event acknowledgement: 100%
- Dead-letter alert threshold: 1 event

## 알림

`foundation_platform_catalog_read_slo_burn`

- error budget burn 또는 latency가 SLO window를 넘으면 owner에게 page한다.
- 먼저 Foundation Platform health, network egress, 최근 deploy를 확인한다.
- circuit이 열렸으면 직접 DB 접근을 추가하지 말고 degraded 응답을 계속 제공한다.

`foundation_platform_catalog_circuit_open`

- circuit breaker가 열리면 owner에게 page한다.
- 실패가 timeout·429·5xx 중 무엇인지 확인한다.
- ad hoc client로 breaker를 우회하지 않는다.

`foundation_platform_webhook_dead_letter_or_latency`

- poison event가 dead-letter 경로에 도달하거나 receiver latency가 SLO를 넘으면 owner에게 page한다.
- event id·event type·correlation id를 보존한다.
- replay 전에 schema 호환성을 고친다.

`foundation_platform_webhook_replay_surge`

- 중복 event 비율이 급증하면 operations ticket을 만든다.
- side effect를 다시 적용할 이유가 아니라 publisher retry/replay 조사로 취급한다.

## 앵커 산출물 가져오기 복구

Anchor snapshot event는 `foundation_platform_event_inbox`에 영속 보관한다. importer는
`FOUNDATION_PLATFORM_EVENT_ID`로 event를 `processing`으로 표시하고 불변 artifact를
`parcel_marker_anchor`로 가져온 뒤 영향을 받은 `listing_marker_projection` row를 갱신하고 event를
`processed` 또는 `failed`로 표시한다.

event가 `processing`인 상태에서 importer process가 종료되면 같은 `FOUNDATION_PLATFORM_EVENT_ID`로
재실행하거나 local artifact path 변수를 빼고 pending inbox batch를 처리한다. Batch mode는
`pending_import` and `processing` anchor snapshot events from
`foundation_platform_event_inbox`, with
`FOUNDATION_PLATFORM_ANCHOR_IMPORT_BATCH_LIMIT` defaulting to 10 and capped at 100.
local artifact path 환경변수가 없으면 importer는 저장된 event payload를 읽고
the stored event payload, fetches `artifact_manifest_url`, verifies
`artifact_checksum_sha256`, manifest URL 기준 object key를 해석하고 가져온 JSONL object를 import한다.
processing event는 의도적으로 다시 claim할 수 있다. importer는 event id에서 만든 PostgreSQL advisory
lock을 잡아 두 worker가 같은 event를 동시에 가져오지 못하게 한다. process connection이 끝나면 lock은
자동 해제된다. Batch mode는 이미 잠긴 event를 건너뛰지만 가져올 수 있는 event가 하나라도 실패하면
실패로 종료한다.

Inspect pending or interrupted anchor imports:

```sql
select event_id, status, anchor_snapshot_id, source_geometry_version, received_at,
       processed_at, failed_at, failure_reason
from foundation_platform_event_inbox
where event_type = 'catalog.parcel_marker_anchor.snapshot.published.v1'
  and status in ('pending_import', 'processing', 'failed')
order by received_at asc;
```

event payload의
`artifact_manifest_url`, `artifact_checksum_sha256`, and `published_at` match
the Foundation Platform release record. Do not edit `parcel_marker_anchor` or
`listing_marker_projection` directly.

## Listing Marker Freshness Operations

Gongzzang listing markers compose runtime visibility as:

```text
visible markers = base tile + delta overlay - tombstone overlay - unauthorized records
```

Foundation Platform은 PNU anchor 원천 데이터를 소유한다. Gongzzang은 listing 의미,
projection, delta log, tombstone log와 dirty tile 재생성 결정을 소유한다.

Watch these metrics from `/internal/metrics`:

- `gongzzang_listing_marker_dirty_tiles_pending`
- `gongzzang_listing_marker_dirty_tile_oldest_age_seconds`
- `gongzzang_listing_marker_tombstones_active`
- `gongzzang_listing_marker_deltas_active`

tomstone이나 delta가 예상보다 늘면 `listing_marker_dirty_tile_queue`를 점검하고,
`listing_marker_tombstone_log`, and `listing_marker_delta_log`. Do not bypass this by adding
listing-owned latitude/longitude or public `bbox` marker APIs.

## Load And Fault Verification

필수 테스트:

- `webhook_duplicate_burst_ack`는 duplicate burst가 반복 side effect 없이 acknowledge됨을 증명한다.
- `webhook_dead_letter_poison_event`는 잘못된 event가 반복 실패 대신 dead-letter path로 들어감을 증명한다.
- `anchor_import_processing_reclaim` proves a processing anchor import can be
  safely reclaimed for retry after a worker exit.
- `catalog_circuit_breaker_timeout_fault`는 timeout이 failure를 기록함을 증명한다.
- `catalog_circuit_breaker_open_fault`는 열린 circuit이 호출을 막음을 증명한다.

운영 준비 조건은 이 테스트와 `docs/architecture/platform-integration/index.v1.json`의
platform-integration policy 계약이 계속 유지되는 것이다.
