---
status: current
owner: gongzzang-제품
doc_type: runbook
last_reviewed: 2026-11-05
---

# Foundation Platform 연동 운영 런북

## 범위

이 런북은 Gongzzang 런타임의 Foundation Platform 호출과 Foundation Platform 웹훅의 Gongzzang 전달을
다룬다.

Policy SSOT: [`docs/architecture/platform-integration/operations-policy.v1.json`](../architecture/platform-integration/operations-policy.v1.json).
필수 span 속성(`telemetry.required_span_attributes`), 금지 속성(`telemetry.forbidden_attributes`),
SLO 수치(`slos`), 알림 정의(`alerts`), 필수 부하·장애 시험(`load_fault_tests`)은 모두 그 파일이 정본이다.
이 런북은 수치를 다시 적지 않고, 알림이 울렸을 때 할 일만 적는다.

서비스 token·웹훅 secret·cookie·authorization header를 절대 로그에 남기지 않는다.

## 알림별 대응

`foundation_platform_catalog_read_slo_burn`

- 먼저 Foundation Platform health, network egress, 최근 deploy를 확인한다.
- circuit이 열렸으면 직접 DB 접근을 추가하지 말고 degraded 응답을 계속 제공한다.

`foundation_platform_catalog_circuit_open`

- 실패가 timeout·429·5xx 중 무엇인지 확인한다.
- ad hoc client로 breaker를 우회하지 않는다.

`foundation_platform_webhook_dead_letter_or_latency`

- event id·event type·correlation id를 보존한다.
- replay 전에 schema 호환성을 고친다.

`foundation_platform_webhook_replay_surge`

- side effect를 다시 적용할 이유가 아니라 publisher retry/replay 조사로 취급한다.

## 앵커 산출물 가져오기 복구

> 현재 상태: 이 이벤트(`catalog.parcel_marker_anchor.snapshot.published.v1`)의 Foundation 쪽 생산자는
> [루트 ADR-0413](../../../../docs/adr/0413-the-parcel-anchor-marker-tile-pipeline-retires-and-returns-as-a-cluster-unit-of-the-tile-bake.md)
> 으로 은퇴했다. 새 이벤트는 오지 않는다. 가져오기 경로(`services/gongzzang-api/src/bin/foundation_anchor_import.rs`)와
> 아래 절차는 이미 받은 이벤트를 다룰 때만 쓰며, 소비자 쪽 정리는 Gongzzang 몫으로 남아 있다.

Anchor snapshot event는 `foundation_platform_event_inbox`에 영속 보관한다. importer는
`FOUNDATION_PLATFORM_EVENT_ID`로 event를 `processing`으로 표시하고 불변 artifact를
`parcel_marker_anchor`로 가져온 뒤 영향을 받은 `listing_marker_projection` row를 갱신하고 event를
`processed` 또는 `failed`로 표시한다.

event가 `processing`인 상태에서 importer process가 종료되면 같은 `FOUNDATION_PLATFORM_EVENT_ID`로
재실행하거나, local artifact path 변수를 빼고 pending inbox batch를 처리한다. Batch mode는
`foundation_platform_event_inbox`의 `pending_import`·`processing` anchor snapshot event를 처리하며
한 번에 처리할 수는 `FOUNDATION_PLATFORM_ANCHOR_IMPORT_BATCH_LIMIT`가 정한다(기본값·상한은 importer 코드가 정본).
local artifact path 환경변수가 없으면 importer는 저장된 event payload를 읽고 `artifact_manifest_url`을 가져와
`artifact_checksum_sha256`을 검증한 뒤, manifest URL 기준 object key를 해석해 가져온 JSONL object를 import한다.
processing event는 의도적으로 다시 claim할 수 있다. importer는 event id에서 만든 PostgreSQL advisory
lock을 잡아 두 worker가 같은 event를 동시에 가져오지 못하게 한다. process connection이 끝나면 lock은
자동 해제된다. Batch mode는 이미 잠긴 event를 건너뛰지만 가져올 수 있는 event가 하나라도 실패하면
실패로 종료한다.

중단되거나 대기 중인 앵커 가져오기 확인:

```sql
select event_id, status, anchor_snapshot_id, source_geometry_version, received_at,
       processed_at, failed_at, failure_reason
from foundation_platform_event_inbox
where event_type = 'catalog.parcel_marker_anchor.snapshot.published.v1'
  and status in ('pending_import', 'processing', 'failed')
order by received_at asc;
```

event payload의 `artifact_manifest_url`, `artifact_checksum_sha256`, `published_at`이 Foundation Platform
릴리스 기록과 맞는지 확인한다. `parcel_marker_anchor`나 `listing_marker_projection`을 직접 고치지 않는다.

## 매물 마커 최신성 운영

Gongzzang listing marker는 런타임 노출을 다음처럼 합성한다:

```text
visible markers = base tile + delta overlay - tombstone overlay - unauthorized records
```

Foundation Platform은 PNU anchor 원천 데이터를 소유한다. Gongzzang은 listing 의미,
projection, delta log, tombstone log와 dirty tile 재생성 결정을 소유한다.

`/internal/metrics`에서 볼 지표 (`services/gongzzang-api/src/routes/metrics.rs`):

- `gongzzang_listing_marker_dirty_tiles_pending`
- `gongzzang_listing_marker_dirty_tile_oldest_age_seconds`
- `gongzzang_listing_marker_tombstones_active`
- `gongzzang_listing_marker_deltas_active`

tombstone이나 delta가 예상보다 늘면 `listing_marker_dirty_tile_queue`, `listing_marker_tombstone_log`,
`listing_marker_delta_log`를 점검한다. listing 소유 위도·경도나 공개 `bbox` marker API를 추가해서 우회하지 않는다.

## 부하·장애 검증

필수 시험 목록과 각 시험 파일은 `operations-policy.v1.json`의 `load_fault_tests`가 정본이다.
운영 준비 조건은 그 시험과 [`index.v1.json`](../architecture/platform-integration/index.v1.json)의
platform-integration policy 계약이 계속 유지되는 것이다.
