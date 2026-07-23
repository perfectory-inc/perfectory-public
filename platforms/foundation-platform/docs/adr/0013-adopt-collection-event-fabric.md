# ADR 0013 - Adopt Collection Event Fabric (gongzzang ADR-0047)

| Field | Value |
|---|---|
| Date | 2026-06-22 |
| Status | Accepted |
| Scope | `foundation-platform` Bronze ingestion / national data collection pipeline |
| Governs | This ADR is a consumer pointer; the governing design is **gongzzang ADR-0047** |
| Related ADRs | [ADR 0001](./0001-inherit-gongzzang-adrs.md), [ADR 0002](./0002-r2-primary-object-storage.md), [ADR 0005](./0005-object-lake-layout-and-indexing.md), gongzzang ADR-0026/0032/0044/0046/0047 |

## Context

`foundation-platform` is the **implementation owner** of the Bronze collection pipeline (Catalog ETL:
V-World / data.go.kr ingestion into the R2 object lake). The *design* for how that pipeline is
shaped — the "Collection Event Fabric" — lives in **gongzzang `docs/adr/0047-collection-event-fabric.md`**,
because cross-repo data-platform decisions and the Kafka/Kubernetes deferral (ADR-0046) are recorded
in `gongzzang` (per ADR-0001, `foundation-platform` inherits gongzzang ADRs).

Without a pointer here, a `foundation-platform` worker could build the collection pipeline a different way
and never see the agreed Kafka-shaped contract. This ADR is that pointer.

## Decision

`foundation-platform` builds the Bronze collection pipeline to the **gongzzang ADR-0047** contract. The
load-bearing points a `foundation-platform` implementer must honor:

1. **Kafka-shaped, broker-deferred.** The pipeline is designed around a Kafka-style event fabric
   (job dispatch + `raw_written` fan-out), but **no broker is built now** — pre-launch it runs on the
   existing Postgres outbox + ledger. Kafka/MSK is deferred to its trigger (gongzzang ADR-0046).
   Adopting Kafka later is an **adapter swap, not a rewrite**.
2. **Two traits, not one.**
   - **`JobBus`** — collection-job *dispatch* (publish / poll / ack / nack). This is **new** and
     Foundation Platform-private. The **first** implementation is backed by the existing **JSONL ledger**
     (option A, no broker, no DB change); a Postgres `collection_job` table + `PostgresJobBus`
     (option B) is a later step that **requires owner DB approval** before any migration.
   - **`RawWrittenSink`** — the **producer** seam (new, typed): a worker emits its
     `CollectionRawWrittenV1` to the sink on `ack`. Distinct from `EventBroadcaster` because
     `EventBroadcaster::publish` needs a persisted outbox `event_id`/`OutboxScope`, while the
     producer emits *before* persisting. The production sink inserts the `catalog.outbox_event` row;
     the existing `OutboxWorker` + `EventBroadcaster` fan it out. (Refines gongzzang ADR-0047's
     "RawWrittenSink = EventBroadcaster" — see that ADR's 2026-06-22 refinement note.)
   - **`EventBroadcaster`** (existing) — `collection.raw_written` *fan-out* only (outbox row →
     consumers). Publish-only; must **not** be overloaded to pull jobs or be the producer seam.
3. **Claim-Check.** Raw bytes stay in **R2 Bronze**. Messages carry only a **pointer + content hash +
   status + lineage** — never the raw payload. (gongzzang ADR-0026: Bronze in R2, not Postgres JSONB.)
4. **Integrity hash is producer-computed.** The worker **tee-hashes the upload stream** (`sha256`);
   do not trust the R2/S3 `ETag` as a content digest (ADR-0047 OQ-5).
   - **Canonical source for `collection.raw_written.bronze_checksum_sha256`** is the producer-computed
     `PublicDataBronzePagePlan.checksum_sha256`, persisted as `bronze_object.checksum_sha256`.
     `raw_written` MUST carry this real, **non-empty** digest. The JSONL ledger event's
     `bronze_checksum_sha256` is a *coverage/audit projection* of the same value, **not** the source
     of truth for `raw_written`. Where a legacy path still leaves the JSONL field empty
     (child-process `ledger-execute`, pending Slice 2d), the canonical `bronze_object` value remains
     authoritative and `raw_written` is unaffected — so empty hashes can never leak into the
     claim-check contract.
5. **Ledger is SSOT.** The JSONL/Postgres ledger remains the source of truth for collection state; the
   fabric reconciles to it. Kafka offsets, if/when present, are a transport detail — state recovers
   from the ledger even with no broker. Reuse the existing `*_coverage_ledger_check` audit.
6. **Reuse, don't multiply.** Failures reuse the existing `catalog.outbox_quarantine` DLQ table (no
   new DLQ table); reuse the existing reuse-manifest gate and provider rate policy.
7. **Boundary (ADR-0047 OQ-6).** The fabric is **Foundation Platform-private**. Only the
   `collection.raw_written` event-type name(s) + schema are published as the gongzzang/dawneer
   consumer contract (via `shared-kernel`). Internal topics (`collection.jobs`, `.job_status`,
   `.retry`, `.dlq`) and `JobBus` must **not** leak into `shared-kernel` or any consumer contract.
8. **Quota gate (ADR-0047 OQ-2).** The `request_cap`/daily-budget gate stays in `select_pending_jobs`
   pre-Kafka; on any Kafka cutover it must be re-homed into a **consumer-side rate limiter** (never
   partition count) as a required pre-cutover task.

## Consequences

- A `foundation-platform` implementer has one authoritative spec to follow; no risk of divergent designs.
- Zero new infrastructure pre-launch — the fabric runs on the outbox/ledger already in
  `services/foundation-outbox-publisher`.
- The dispatch mechanism (Postgres → Kafka) stays swappable without a cross-repo contract change,
  because only `raw_written` is public.
- Cost: the design SSOT is in another repo — keep this pointer in sync if ADR-0047 is revised (the
  governing copy is ADR-0047; this file does not re-state the schemas).

## References

- **Governing design:** gongzzang `docs/adr/0047-collection-event-fabric.md`.
- gongzzang ADR-0026 (Bronze API archive in R2), ADR-0032 (eventual consistency / outbox),
  ADR-0044 (product-first / no premature infra), ADR-0046 (Kafka/Kubernetes deferred).
- `foundation-platform` ADR-0001 (inherit gongzzang ADRs), ADR-0002 (R2 primary object storage),
  ADR-0005 (object-lake layout).
- Implementation surface: `services/foundation-outbox-publisher/src/national_data_collection_async/*`,
  `national_bronze_object_manifest.rs`, `national_data_collection_coverage_ledger_check.rs`,
  `crates/outbox-publisher/src/{broadcaster,worker,lineage}.rs`,
  `crates/foundation-shared-kernel/src/events/catalog_v1.rs`, `catalog.outbox_quarantine`.
