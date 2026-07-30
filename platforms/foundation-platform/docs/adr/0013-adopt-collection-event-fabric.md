# ADR 0013 - 수집 이벤트 패브릭 채택(Gongzzang ADR-0047)

| Field | Value |
|---|---|
| Date | 2026-06-22 |
| Status | Accepted |
| Scope | `foundation-platform` Bronze ingestion / national data collection pipeline |
| Governs | This ADR is a consumer pointer; the governing design is **gongzzang ADR-0047** |
| Related ADRs | [ADR 0001](./0001-inherit-gongzzang-adrs.md), [ADR 0002](./0002-r2-primary-object-storage.md), [ADR 0005](./0005-object-lake-layout-and-indexing.md), gongzzang ADR-0026/0032/0044/0046/0047 |

## 배경

`foundation-platform`은 Bronze 수집 파이프라인(Catalog ETL: V-World/data.go.kr를 R2 객체
레이크로 수집)의 **구현 소유자**다. 파이프라인의 형태인 "Collection Event Fabric" 설계는
저장소 간 데이터 플랫폼 결정과 Kafka/Kubernetes 연기(ADR-0046)가 `gongzzang`에 기록되므로
**gongzzang `docs/adr/0047-collection-event-fabric.md`**에 있다. ADR-0001에 따라
`foundation-platform`은 gongzzang ADR을 상속한다.

이 문서의 연결이 없으면 `foundation-platform` worker가 합의된 Kafka 형태 계약을 보지 못한
채 다른 방식으로 수집 파이프라인을 만들 수 있다. 이 ADR이 그 연결이다.

## 결정

`foundation-platform`은 **gongzzang ADR-0047** 계약에 맞춰 Bronze 수집 파이프라인을 만든다.
구현자가 반드시 지켜야 할 핵심은 다음과 같다.

1. **Kafka 형태, broker 연기.** 파이프라인은 Kafka식 이벤트 패브릭(job dispatch +
   `raw_written` fan-out)을 기준으로 설계하지만 **지금 broker는 만들지 않는다**. 출시 전에는
   기존 Postgres outbox와 ledger로 실행한다. Kafka/MSK는 gongzzang ADR-0046의 조건이
   충족될 때 도입하며, 나중 도입은 **재작성 아닌 어댑터 교체**다.
2. **trait는 하나가 아니라 두 개다.**
    - **`JobBus`** — collection-job *dispatch*(publish/poll/ack/nack)다. **새 trait**이며
     Foundation Platform-private. The compatibility path remains the existing **JSONL ledger**
     (option A), while option B is now implemented as the migrated `catalog.collection_job` table
     plus `PostgresJobBus`. The adapter is contract-tested against a real disposable PostgreSQL
     instance. The legacy data.go.kr national async executor is now fail-closed because national
     Bronze collection is bulk-only. The active `hub.go.kr` bulk collector claims and acks through
     `PostgresJobBus`; Kafka remains a later transport choice after its trigger is met.
    - **`RawWrittenSink`** — **producer** 경계(새 typed trait)다. worker가
     `CollectionRawWrittenV1` to the sink on `ack`. Distinct from `EventBroadcaster` because
     `EventBroadcaster::publish` needs a persisted outbox `event_id`/`OutboxScope`, while the
     producer emits *before* persisting. The production sink inserts the `catalog.outbox_event` row;
     the existing `OutboxWorker` + `EventBroadcaster` fan it out. (Refines gongzzang ADR-0047's
     "RawWrittenSink = EventBroadcaster" — see that ADR's 2026-06-22 refinement note.)
    - **`EventBroadcaster`**(기존)는 `collection.raw_written` *fan-out*만 담당한다(outbox row →
     consumers). Publish-only; must **not** be overloaded to pull jobs or be the producer seam.
3. **Claim-Check.** 원본 바이트는 **R2 Bronze**에 둔다. 메시지에는 **pointer + content hash +
   status + lineage**만 넣고 raw payload는 넣지 않는다(gongzzang ADR-0026: Bronze는 R2, Postgres
   JSONB가 아님).
4. **무결성 해시는 producer가 계산한다.** worker는 업로드 stream을 **tee-hash**(`sha256`)하고
   R2/S3 `ETag`를 content digest로 신뢰하지 않는다(ADR-0047 OQ-5).
   - **Canonical source for `collection.raw_written.bronze_checksum_sha256`** is the producer-computed
     `PublicDataBronzePagePlan.checksum_sha256`, persisted as `bronze_object.checksum_sha256`.
     `raw_written` MUST carry this real, **non-empty** digest. The JSONL ledger event's
     `bronze_checksum_sha256` is a *coverage/audit projection* of the same value, **not** the source
     of truth for `raw_written`. Where a legacy path still leaves the JSONL field empty
     (child-process `ledger-execute`, pending Slice 2d), the canonical `bronze_object` value remains
     authoritative and `raw_written` is unaffected — so empty hashes can never leak into the
     claim-check contract.
5. **Ledger가 SSOT다.** JSONL/Postgres ledger가 수집 상태의 정본이며 패브릭은 여기에
    패브릭은 여기에 맞춰 조정한다. Kafka offset이 생겨도 전송 세부사항일 뿐이며 broker가
    없어도 상태는 ledger에서 복구한다. 기존 `*_coverage_ledger_check` 감사를 재사용한다.
6. **늘리지 말고 재사용한다.** 실패는 기존 `catalog.outbox_quarantine` DLQ 테이블을
    새 DLQ 테이블은 만들지 않는다. 기존 reuse-manifest gate와 provider rate policy를 재사용한다.
7. **경계(ADR-0047 OQ-6).** 패브릭은 **Foundation Platform 내부 전용**이다. 외부에 공개하는 것은
    `collection.raw_written` event-type 이름과 schema만 `shared-kernel`을 통한 gongzzang/dawneer
    소비자 계약으로 공개한다. 내부 topic(`collection.jobs`, `.job_status`, `.retry`, `.dlq`)과
    `JobBus`는 `shared-kernel`이나 소비자 계약으로 **유출하지 않는다**.
8. **Quota gate(ADR-0047 OQ-2).** `request_cap`/일일 예산 게이트는 Kafka 이전에는
    pre-Kafka 단계에서 적용한다. Kafka로 전환할 때는 partition 수가 아니라 **소비자 측 rate
    limiter**로 옮기는 것을 전환 전 필수 작업으로 둔다.

## 영향

- `foundation-platform` 구현자는 하나의 권위 사양을 따르므로 설계가 갈라지지 않는다.
- 출시 전 호환 경로는 이미
  `services/foundation-outbox-publisher`에서 실행된다. 영속 Postgres dispatch 어댑터는
  마이그레이션 테이블 하나를 추가하지만 Kafka를 Bronze 의존성으로 만들지 않는다.
- dispatch 방식(Postgres → Kafka)은 저장소 간 계약을 바꾸지 않고 교체할 수 있다.
  공개되는 것은 `raw_written`뿐이기 때문이다.
- 비용은 설계 SSOT가 다른 저장소에 있다는 점이다. ADR-0047이 바뀌면 이 연결을 함께 갱신한다.
  정본 사본은 ADR-0047이며 이 파일은 schema를 다시 적지 않는다.

## 참고 문서

- **설계 정본:** gongzzang `docs/adr/0047-collection-event-fabric.md`.
- gongzzang ADR-0026 (Bronze API archive in R2), ADR-0032 (eventual consistency / outbox),
  ADR-0044 (product-first / no premature infra), ADR-0046 (Kafka/Kubernetes deferred).
- `foundation-platform` ADR-0001 (inherit gongzzang ADRs), ADR-0002 (R2 primary object storage),
  ADR-0005 (object-lake layout).
- Implementation surface: `services/foundation-outbox-publisher/src/national_data_collection_async/*`,
  `national_bronze_object_manifest.rs`, `national_data_collection_coverage_ledger_check.rs`,
  `crates/outbox-publisher/src/{broadcaster,worker,lineage}.rs`,
  `crates/foundation-shared-kernel/src/events/catalog_v1.rs`, `catalog.outbox_quarantine`.
