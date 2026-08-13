# ADR-0047: 수집 이벤트 패브릭 — Kafka 형태 Bronze 수집 제어면(broker 보류)

| | |
|---|---|
| Date | 2026-06-22 |
| Status | Accepted — **지금은 설계, broker는 보류**; 구축 순서가 아니라 계약과 이전 단계를 기록 |
| Scope | foundation-platform Bronze-ingestion collection pipeline (Catalog public-API → R2 Bronze). Cross-repo because the `collection.raw_written` contract and event-type names are a published consumer contract. |
| Owner | perfectoryinc (platform owner) |
| Governs under | [✱ Product-first](../../AGENTS.md) · [ADR-0044](./0044-bazel-transition-reconciliation.md) (no premature infra) · refines [ADR-0046](./0046-kafka-kubernetes-preliminary-design.md) (Kafka transport ladder) · [ADR-0032](./0032-eventual-consistency-strategy.md) (outbox/eventual consistency) · [ADR-0026](./0026-bronze-api-archive-r2-not-postgres-jsonb.md) (Bronze in R2) |

> 이것은 전국 Bronze 수집 *제어면*의 **선행 설계**다. topic 분류, 이벤트 schema,
> claim-check 규칙, 멱등성/retry/DLQ 모델, ledger↔offset 대조를 **전송 중립 계약**으로
> 정하고, 출시 전 Kafka broker 없이 **기존 Postgres/outbox/ledger 기반에서 실행**한다.
> 조건 전에 MSK를 만드는 것은 ADR-0044가 되돌린 "사용자보다 인프라 우선" 함정이다.
> 지금 Kafka 형태로 설계하는 목적은 나중 broker 교체를 재작성 아닌 wiring 변경으로 만드는 것이다.

---

## 배경

### 현재 사실(Foundation 코드로 확인한 것)

현재 전국 수집은 **ledger 기반**이며 이 패브릭의 약 90%가 이미 구현되어 있다. 아직 streaming
제어면이라는 이름으로 정리하지 않았을 뿐이다.

- **Job plan/command** — the Planner (`national_data_collection_plan_compile.rs` / `national_data_collection_async/plan.rs`) writes a JSONL **execution ledger** of `LedgerEntry` rows with `status:"planned"`, each carrying `job_id`, `idempotency_key`, `collection_snapshot_id`, `compiler_input_hash_sha256`, `shard_id`, `scope_unit_id`, `request_fingerprint_sha256` (`foundation-platform.bronze_request_fingerprint.v1`), provider/endpoint, page window, and `request_count_estimate`.
- **Job dispatch/consume** — `select_pending_jobs` (`ledger.rs`) windows pending rows under a `request_cap` and skips already-`succeeded` job ids; `ledger_execute/.../runner.rs` executes them.
- **Raw write / claim ticket** — workers PUT raw bytes to **R2 Bronze** under the deterministic key `bronze/source=.../page=<N>/part-<M>.json` (`expand_bronze_object_keys`, `national_bronze_object_manifest.rs`) and append a `succeeded`/`job_reused` event (`events.rs`) carrying `bronze_object_key`, `storage_driver:"r2"`, `request_count`, `source_record_count`, `request_fingerprint_*`, `collection_snapshot_id`.
- **Audit** — `national_data_collection_coverage_ledger_check.rs` already computes Chaperone-class reconciliation (collected / duplicate / missing / extra / empty / late) and **double-entry checks** each evidence file's self-reported counts against the recomputed event log; it requires `missing == extra == duplicate == failed == 0` for rollout.
- **Pluggable transport** — `crates/outbox-publisher` has the `EventBroadcaster` trait (`LoggingBroadcaster`, `WebhookBroadcaster`, `CatalogEventBroadcaster`) + `OutboxWorker` (`FOR UPDATE SKIP LOCKED`, `retry_count`, quarantine) and the `catalog.outbox_quarantine` DLQ table (`failure_stage`, `failure_code`, `attempt_count`, `resolution_kind IN ('replayed','discarded','superseded')`, idempotent `ON CONFLICT … version = version+1`).
- **Per-source rate policy** — `provider-rate-policy.v1.json` + `public_provider_rate_policy.rs` (AIMD token-bucket lanes, `daily_request_budget_env`, throttle signals, `defer_without_drop`/`pause_lane`) + `provider_request_spacing.rs`.

### Kafka 중심 목표

하나의 **Collection Event Fabric**을 둔다. 이름이 정해진 topic 소수, 두 핵심 이벤트 schema
(`job` 명령 + `raw_written` claim-check), 명시적 멱등성/retry/DLQ/audit 규칙을 Kafka topic·
partition key·consumer offset 형태로 고정한다. downstream(Silver/Gold/AI 보강/검색 indexer/
알림)이 하나의 순서 로그에서 fan-out할 수 있다. ADR-0046이 조건부 Kafka 단계를 정했으므로
이 ADR은 수집 제어면에 패브릭을 배치하고 broker와 무관한 wire 계약을 고정한다.

### Claim-Check(핵심)

**원본 바이트는 어떤 stream에도 절대 보내지 않는다.** 모든 메시지는 R2 Bronze 객체 pointer,
`sha256`, `byte_size`, `record_count`, lineage만 담은 *claim ticket*이다. ticket을 잃어도
멱등 재수집 비용만 생기고 데이터는 잃지 않는다. 이는 Enterprise Integration Patterns의
**Claim-Check** 패턴이며 transactional Outbox, Dead-Letter Channel, Competing Consumers,
Idempotent Receiver와 Uber Chaperone식 완전성·중복 감사도 함께 사용한다.

---

## 결정

**Collection Event Fabric을 전송 교체 가능한 계약(topic 이름, partition key, payload schema,
호환성 규칙, 멱등성/retry/DLQ 의미)으로 정의하고 지금은 기존 Postgres + outbox + JSONL-ledger
기반에서 실행한다. 출시 전에 Kafka broker(MSK)를 세우지 않는다.** Worker와 Planner는 전송
interface만 의존하며 Kafka 교체는 schema·consumer·audit을 바꾸지 않는 wiring 변경이다.

dispatch에는 발행보다 많은 동작이 필요하므로 좁은 trait 두 개를 둔다.

- **`JobBus`** — the dispatch side (Planner produces, Worker consumes): `publish_jobs`, `poll_jobs(group, max, lease)`, `ack(lease_token)`, `nack(lease_token, retryable)`. `ack` ≡ commit offset; `nack(retryable=false)` ≡ DLQ.
- **`RawWrittenSink`** — the **producer** seam: a collection worker hands its typed
  `collection.raw_written` payload to the sink when it `ack`s a job. The production sink **inserts a
  `catalog.outbox_event` row**; the **existing `OutboxWorker` + `EventBroadcaster`** then fan that row
  out to consumers unchanged. So `EventBroadcaster` is still reused — for the *fan-out* half — but it
  is **not** the producer seam itself.

> **Resolved contradiction (facets 1/2 vs 4):** facets 1–3 spoke of "the `EventBroadcaster` trait" for both directions; facet 4 correctly observed `EventBroadcaster` is publish-only and cannot express pull/lease/ack. **We adopt the two-trait split.** `JobBus` owns dispatch; `RawWrittenSink` owns producer-side emission; `EventBroadcaster` owns fan-out. Both new traits are `Arc<dyn …>`-injected, identical to how `OutboxWorker` holds `Arc<dyn EventBroadcaster>` today.
>
> **Refinement (2026-06-22, from the Slice 3-A implementation):** the producer seam is a **distinct typed trait `RawWrittenSink`**, *not* `EventBroadcaster` directly. Reason: `EventBroadcaster::publish` takes an `EventEnvelope` carrying an outbox `event_id` + `OutboxScope` that exist **only after** the outbox row is persisted, whereas the producer must emit *before* persisting (its input is the typed `CollectionRawWrittenV1`). `RawWrittenSink::emit(&CollectionRawWrittenV1)` is therefore the pre-persist producer port; its production impl persists the row and the existing `OutboxWorker`/`EventBroadcaster` path fans it out. This keeps producer-shaping out of the fan-out trait and avoids the "two `EventBroadcaster`s" the split rejected.

**ledger가 "무엇을 수집해야 하는가"와 "무엇을 수집했는가"의 SSOT다.** bus는 dispatch·lease·
fan-out만 담당한다. bus가 삭제되어도 ledger와 event log로 상태를 완전히 복원하며 coverage
check가 이미 이 작업을 한다. Kafka는 audit 정본이 되지 않는다(SSS 3 traceability, 6 SSOT).

---

## Topic 분류와 이벤트 스키마

### Topic(5개, 의도적으로 최소화)

| Topic | Direction | Partition key | Notes |
|---|---|---|---|
| `collection.jobs` | Planner → Workers | **`scope_unit_id`** | Command stream; one record per planned job. = ledger `status:"planned"` rows. |
| `collection.raw_written` | Workers → downstream | **`scope_unit_id`** | Claim-check event; one record per Bronze object (page/part). Fan-out point for Silver/Gold/AI/search/notify. |
| `collection.job_status` | Workers → audit | **`scope_unit_id`** | Lifecycle: `running`/`succeeded`/`failed`/`retryable`/`reused`/`empty`. Keep full history (the Chaperone late/dup trail is the product value — no compaction). |
| `collection.jobs.retry` | retry scheduler → Workers | `scope_unit_id` | Delayed re-delivery; backoff in `not_before_utc` so it doesn't head-of-line-block fresh jobs. |
| `collection.jobs.dlq` | Workers/scheduler → operator | `scope_unit_id` | Terminal; logical view over the existing `catalog.outbox_quarantine` (see Reliability). |

Naming rule: lowercase dotted `collection.<noun>[.<modifier>]`. **No env, no version in the topic name.**

### 파티션 키 — `scope_unit_id`(확정)

기존 canonical `scope_unit_id`(`scope:legal-dong:<sigungu><bjdong>`,
`scope:sigungu-month:<lawd_cd>:<deal_ymd>`)로 partition한다. source(~3 key라 병렬성이 없음)나
`job_id`(병렬성은 최대지만 scope별 순서가 없음)로 partition하지 않는다. `scope_unit_id`는
수만 개 key를 제공해 충분한 병렬성과 scope별 전체 순서를 보장한다. 여러 페이지 job 안에서는
`page_number`가 보조 순서다. `collection.jobs`, `collection.job_status`,
`collection.raw_written`가 같은 key를 사용하므로 세 stream을 별도 비용 없이 함께 partition할 수 있다.

> **Resolved contradiction (facet 1 vs facets 2/4):** facet 1 chose `scope_unit_id` as the partition key; facets 2/4 floated `idempotency_key`/`request_fingerprint_sha256` as the Kafka *message key*. These are different roles. **Decision: partition key = `scope_unit_id` (ordering/locality); message dedup key = `idempotency_key` (carried in the record so redeliveries of one job land in order and dedup).** `request_fingerprint_sha256` is the *content identity* used for reuse/dedup of bytes (below), not the partition key. See OQ-1 for the one remaining nuance.

### 이벤트 타입·버전 규칙

- Event types follow the shared-kernel pattern `collection.<aggregate>.<action>.v<N>` (e.g. `collection.raw_written.v1`), aligned with `catalog_v1.rs`. **Topic name drops the `.vN`** (topics are version-spanning); the payload's `schema_version` discriminates.
- Backward-compatible evolution = **add optional fields only.** Any removal/rename/semantic change = new `.v2` type + new struct variant coexisting on the same topic (mirrors `…CreatedV1`/`V2`).
- Unknown `type`/`schema_version` → route to `collection.jobs.dlq` (or `*.unknown` sink) with telemetry, never silently drop (matches the §10 Migration BLOCKER and the existing fail-closed posture).
- A **compatibility-corpus test** (frozen example JSON per version) gates schema changes — codec/struct roundtrip only, a unit test, **not a running broker**.

### `collection.job.planned.v1` (= `collection.jobs` value)

기존 `LedgerEntry`의 projection에 command-control field 네 개를 더한 형태다. `attempt`,
`deadline_utc`(claim lease, 현재 선택·동시 worker가 생기면 강제), `rate_budget`/`lane_id`,
`request_cap_share`를 제외한 모든 field는 ledger에 이미 있다. provider별 범위 field는
`plan_compile.rs`에서 그대로 재사용하는 `provider_request` 하위 객체에 담는다.
`serviceKey`/`raw_payload`는 넣지 않는다(기존 금지 토큰 검사가 bus payload에도 적용된다).

### `collection.raw_written.v1` (= claim ticket)

Bronze 객체(페이지/part)마다 이벤트 하나를 발행하며 `national_bronze_object_manifest` 항목과
1:1로 대응한다. 따라서 manifest는 이 stream을 materialize한 replay이며 두 표현은 byte
호환이어야 한다. 이벤트에는 `claim_check { storage_driver, object_key | (last_object_key,
object_count), sha256, byte_size, record_count }`, `request_fingerprint_*`,
`collection_snapshot_id`, `page`, `source`, 필수 `lineage` 블록을 담고, 발행 시각인
`occurred_at_utc`와 원천 fetch 시각인 `fetched_at_utc`를 구분한다.

> **Resolved contradiction (facet 1 per-page vs facet 4 per-job):** **per-Bronze-object (per page/part)** is the canonical granularity — it gives exact manifest parity and finer downstream parallelism. To keep messages tiny, a single `raw_written` for a multi-page job MAY carry `(last_object_key, object_count)` and let consumers reconstruct page keys via the existing deterministic `expand_bronze_object_keys` rule, rather than inlining N keys. (Per-job-with-`object_keys[]` is rejected; it loses per-page fan-out. Volume sanity-check against national page counts is OQ-4.)

---

## Claim-Check·R2 포인터·원천별 rate limit

### R2 Bronze 키(Claim-Check 포인터)

기존 grammar를 유지하되 segment 순서를 formalize해 파싱 가능하게 만들고
`expand_bronze_object_keys`가 그대로 동작하게 한다.

```
bronze/source=<source_slug>/endpoint=<endpoint_slug>/snapshot=<collection_snapshot_id>/
       scope=<scope_unit_id>/job=<job_id>/page=<NNNN>/part=<MMMM>.json
```

- `collection_snapshot_id`는 **불변성 경계**다. 재수집은 *새* snapshot을 만들고 기존 값을
  덮어쓰지 않는다. 기존 `create_new(true)` semantics와 manifest duplicate-key blocker가
  write-once를 강제한다.
- Pointer is provider-relative (manifest blocks anything not starting `bronze/source=`); `storage_driver` must be `r2` in any published manifest (`local` is dev-only).

### 무결성 — 유일한 필수 조건

**R2 path에서 `bronze_checksum_sha256`가 현재 `Some("")`로 나간다**(`events.rs`;
`bronze_result.rs`는 local path에서는 hash하지만 R2 path에서는 빈 hash를 반환한다). content hash
없는 claim-check는 신뢰할 수 없다. **해결 방법은 worker가 각 R2 object의 실제 lower-hex
`sha256`을 계산·발행하는 것이다(업로드 stream tee-hash 또는 신뢰할 수 있을 때 R2 ETag/sha 사용,
OQ-5).** 이 ADR에서 반드시 build해야 할 유일한 producer 변경이며, 이후 manifest의 기존
`is_lower_sha256` gate가 fingerprint와 같은 방식으로 content hash에도 적용된다.

### 계보(필수)

`raw_lineage { source, endpoint_slug, fetched_at_utc, license, srid, request_count, source_record_count }`가
claim과 함께 이동하므로 bytes를 inline하지 않고도 AGENTS.md §8/§10.3-14 traceability를 충족한다.
공간 source(V-World cadastral/NED = `EPSG:4326`)에서는 `srid`가 필수(non-null)이고, attribute-only
register(building register, real-transaction)와 OQ에서 확인한 V-World land-register에서는 `null`이다.
`license`는 `endpoint_catalog`에서 가져온다.

### Per-source rate-limit / quota (three-layer enforcement, reusing the existing AIMD lanes)

rate policy를 **재작성하지 않는다**. 각 job을 lane에 묶고 세 계층에서 강제한다.
1. **Planner가 budget에 맞춘다** — pull의 `request_cap`은 lane의 남은 일일 budget
   (`daily_request_budget_env`)이므로 fabric이 quota보다 많은 upstream call을 구조적으로 dispatch하지
   못한다. 기존 `select_pending_jobs` greedy packing과 첫 job이 cap을 넘으면 fail-closed하는 동작을 재사용한다.
2. **Worker가 실시간 token bucket으로 throttle한다** — lane의 in-flight/rps window에서 acquire하고
   `ProviderRequestSpacing`으로 간격을 두며 `is_throttle_signal`로 response를 분류하고
   `update_lane_state`(AIMD)에 반영한다.
3. **Lane pauses + defers on exhaustion** — `on_quota_exhausted → pause_lane`; in-flight jobs flip to `defer_without_drop` (re-queued, status stays `planned`, **not** failed) so coverage can still reach `missing == 0` later. Deferral ≠ loss.

---

## 신뢰성(멱등성·재시도·DLQ·원장↔offset 조정)

### 하나의 규칙

**A job is collected iff a `job_succeeded`/`job_reused` event exists in the event log that the coverage ledger reconciled against the plan — never because a topic/offset said so.** The offset is *liveness*, not *truth*.

### Idempotency — exactly-once *effect* under at-least-once delivery (three gates)

1. **Gate 1 — reuse (skip the external call):** before fetching, consult the reuse manifest (`reuse_manifest.rs`, keyed by `request_fingerprint_sha256`). If the bytes already exist in Bronze, emit `job_reused` (0 provider quota) instead of re-fetching. This must be the **mandatory first step** of consuming a `collection.jobs` message.
2. **Gate 2 — deterministic R2 key:** keys are a pure function of `(fingerprint, page)`, so a redelivered fetch PUTs to the same key (recommend `If-None-Match: *` for a cheap no-op). At-least-once fetch → at-most-once distinct object.
3. **Gate 3 — ledger dedup accounting:** even if both gates are bypassed (racing workers), `inspect_succeeded_event` raises `duplicate_succeeded_event` and the coverage check fails closed. Duplicate *bytes* are harmless; duplicate *accounting* is a hard blocker.

### Retry + DLQ

- **Retry:** transient errors (`http_429`, `http_5xx`, `timeout`, `circuit_open`, `r2_5xx`) → `collection.jobs.retry` with `backoff = provider_spacing(provider) × 2^attempt` + jitter, capped, on the *per-source* clock. Poison (`http_400/401/403`, `schema_reject`, `auth_key_invalid`) → straight to DLQ, no wasted quota. `retry_on`/`fail_fast_on` live in `endpoint_catalog` (one SSOT); the job carries only the resolved `max_attempts`.
- **DLQ:** **reuse `catalog.outbox_quarantine` — do not build a new table.** Add one `consumer_key='collection-worker'`, one `source_outbox_table='catalog.collection_jobs'`, and one `failure_stage='fetch'` to the CHECK enums. The idempotent `ON CONFLICT … version = version+1` means a poison job redelivered N times = **one** DLQ row with rising `attempt_count`. Every `failure_message` passes the existing `safe_runner_error_message` scrubber + `FORBIDDEN_TOKENS` scan (no keys, no `raw_payload` — DLQ holds the ticket, not bytes).

> **Resolved contradiction (facet 4 OQ "separate DLQ?"):** **unify on `catalog.outbox_quarantine`.** The `(source_outbox_table, event_id, consumer_key)` key already distinguishes collection failures from broadcast failures, so they do not actually conflate; a second table is unjustified ceremony.

- **Operator actions** reuse existing indexes/states: `replayed` (re-publish the stored claim → idempotent by the three gates), `discarded` (job stays `missing` forever → correctly blocks rollout until an operator records why), `superseded` (a re-plan with a new `compiler_input_hash` obsoleted it).

### Ledger ↔ offset reconciliation (Chaperone-style audit)

`national_data_collection_coverage_ledger_check.rs`가 **Chaperone** 역할을 하며 수정 없이 재사용된다.
이미 `(provider, endpoint)`별 collected/duplicate/late(`started − succeeded`)/missing/extra/empty
count를 내고 evidence와 재계산 event log를 double-entry check한다. offset은 liveness/lag만 담당한다.
새 alarm은 **`planned − (succeeded ∪ in_DLQ ∪ in_flight)`인데 offset이 claim을 *지난* 경우를
"lost-without-trace"로 보고** `collection.reconcile.gap`을 내는 것이다. downstream은
`collection.raw_written`를 at-least-once로 처리하고 `request_fingerprint_sha256` +
`bronze_object_key`로 dedupe한다. 유실되면 event log에서 다시 파생한다.

> **Resolved open decision (facet 2 OQ-1, `job_reused` dup):** make Gate-3 **ignore a `job_reused` that follows an existing `job_succeeded` for the same `job_id + fingerprint`** (idempotent redelivery, tolerated), while still blocking **two fresh `job_succeeded`** for one `job_id`. This is a small, named rule in `inspect_succeeded_event` and is part of this ADR's accepted scope.

---

## 전환 단계(Postgres/outbox 현재 → 조건 발생 시 Kafka/MSK)

이는 수집 제어면에 적용한 ADR-0046 Kafka 단계다. 모든 단계에서 같은 계약을 사용한다.

| Fabric concept | Rung 1 — pre-launch backing (ships now, 0 brokers) | Rung 2 — Kafka/MSK (swap on trigger) |
|---|---|---|
| `collection.jobs` | ledger rows `status='planned'` (the plan *is* the queue) | partitioned topic, key=`scope_unit_id`, dedup=`idempotency_key` |
| worker pull + lease | `select_pending_jobs` wrapped as `JobBus::poll_jobs`; `FOR UPDATE SKIP LOCKED` = lease | consumer group |
| `ack` / offset | append `job_succeeded`; `read_succeeded_job_ids(compiler_input_hash)` excludes it | offset commit |
| backoff / retry | `poll_interval` + `provider_request_spacing`; `collection.jobs.retry` via `not_before_utc` | retry/delay topics |
| `collection.jobs.dlq` | `catalog.outbox_quarantine` (exists) | DLQ topic + mirror to quarantine |
| `collection.raw_written` | outbox row + `OutboxWorker` + `EventBroadcaster` (exists) | topic via a `KafkaRawWrittenBroadcaster` (`EventBroadcaster` impl) |
| Chaperone audit | `coverage_ledger_check` (exists) — reads ledger, **not** topic | **unchanged** — still reads ledger, not topic |

**The swap trigger (turn on `KafkaJobBus` + `KafkaRawWrittenBroadcaster`) — adopt when ANY fires** (concrete, refining ADR-0046's rung-3 triggers for this pipeline):

1. **Throughput:** a national plan epoch's pending-job backlog or sustained dispatch rate exceeds what one Postgres-polled worker drains within the plan's freshness SLO **and** within the V-World/data.go.kr daily quota window.
2. **Multi-consumer fan-out:** ≥2 *independent* real-time consumers of `collection.raw_written` need their own offsets/replay (e.g. AI enrichment + search indexer running concurrently and falling behind), beyond what outbox-table fan-out + per-consumer cursors can serve.
3. **Replay/retention:** need to replay `raw_written` history to a *new* consumer without re-running collection (Kafka log retention beats reconstructing from JSONL).

**What changes at the trigger: only the adapter wiring** — add `KafkaJobBus` (impl `JobBus`) and `KafkaRawWrittenBroadcaster` (impl `EventBroadcaster`), bind topics (`collection.jobs` key=`scope_unit_id`; `collection.raw_written` key=`scope_unit_id`; `collection.jobs.dlq`). Planner / Worker / Bronze-manifest / coverage-check code: **unchanged**. Run it **managed (MSK / Redpanda Cloud) before ever self-hosting brokers** (per ADR-0046). The request-cap *quota gate* must be re-homed into the consumer as a rate-limiter post-cutover — it must not be lost in translation (OQ-2).

---

## ADR-0046을 구체화하는 방식

ADR-0046 deferred Kafka generally and put it on a transport ladder behind triggers. ADR-0047 **does not reverse that defer** — it sharpens it for the Bronze-ingestion pipeline:

- ADR-0046 framed Kafka purely as a `WebhookBroadcaster → SqsBroadcaster → KafkaBroadcaster` *publish-side* ladder for Catalog/Workforce domain events. ADR-0047 adds the missing **dispatch side** (`JobBus` with pull/lease/ack) — collection needs a job queue, not just fan-out — and names **Kafka as the eventual collection control-plane**, designed now so the contract is fixed.
- ADR-0046's rung-3 Kafka triggers (replay, many consumers, throughput) are **instantiated with concrete, measurable conditions for collection** (the three triggers above).
- The **broker stays deferred.** ADR-0047's rung 1 (Postgres/outbox/ledger) is not a stopgap — at 0 users it is the correct YAGNI choice, and it is the same backing ADR-0046 already endorsed (`EventBroadcaster` over outbox). Nothing here advances Kafka adoption; it only fixes the seam so adoption stays a config flip.

이 ADR은 ADR-0046의 Kubernetes 단계를 **변경하지 않는다**.

---

## 영향

- **Positive:** one fixed, broker-independent contract for national collection → agents/humans stop re-deriving topic/partition/idempotency decisions per session. The eventual MSK swap is a wiring change, not a rewrite. The ledger stays the single audit SSOT regardless of transport (pillars 3 + 6). ~90% is already built; net new work is small and product-visible.
- **Net new work (build-now):** (1) the **real R2 content hash** (the one true prerequisite); (2) the two narrow traits (`JobBus`, reusing `EventBroadcaster` as `RawWrittenSink`) wrapping existing pull/ack/DLQ; (3) widen two `outbox_quarantine` CHECK enums + add `consumer_key='collection-worker'`; (4) wire the reuse-manifest gate as the mandatory first consume step; (5) the Gate-3 `job_reused`-after-`job_succeeded` tolerance rule; (6) add `raw_lineage` + `lane_id` fields; (7) `request_cap = remaining lane budget`.
- **Negative / honest limitations:** the trait seam carries a small upfront cost even if Kafka never lands (≈ one trait + one adapter; acceptable). Pre-launch leasing is tx-scoped (`SKIP LOCKED`) — concurrent multi-worker collection needs an explicit `lease_owner`/`lease_until` and a shared lane-budget counter *before* it is enabled (OQ-3). Per-page `raw_written` is N× message volume for multi-page jobs (mitigated by the `(last_object_key, object_count)` compaction; sanity-check pending, OQ-4).
- **Affected:** Foundation Platform collection workers, Bronze manifest/ledger code, event broadcasting, and quarantine storage. Gongzzang consumes only the published `collection.raw_written` event names and schemas; the internal job fabric remains Foundation-private.

---

## 지금 만들지 않는 것(제품 우선 가드)

- **No Kafka broker, no MSK, no Redpanda** — `KafkaJobBus`/`KafkaRawWrittenBroadcaster` are deferred behind the named trigger; until then they don't exist (no feature-flagged dead broker scaffolding).
- **No new DLQ table, no new audit machine, no new registry/ratchet/evidence-bundle** — reuse `outbox_quarantine`, `coverage_ledger_check`, and the AIMD rate lanes. The only schema delta is widening two CHECK enums.
- **No new PowerShell** (AGENTS.md rule 5) — all logic is Rust; the compatibility-corpus test is a unit test, not infra.
- **No per-provider topics, no `deadline_utc`/lease enforcement, no shared lane-budget counter** until concurrent workers actually exist (define optional fields now, enforce on demand).
- **Every guard answers "what bug does it stop?":** dup-accounting blocker → silent double-count of quota/rows; missing-job blocker → claiming national coverage we don't have; lost-without-trace alarm → a consumer ate a command and produced no data; secret scrubber → API-key leakage onto the wire; content-hash gate → serving a corrupt/wrong Bronze object downstream.

---

## 결정 규칙과 미해결 질문

Items marked resolved below are normative parts of this ADR. Unresolved items use their recommended
default until a named reassessment trigger fires; operational progress is not recorded here.

1. **OQ-1 — Kafka message key on cutover.** Confirmed: partition key = `scope_unit_id`. Remaining nuance: should the *record key* be `idempotency_key` (per-job dedup ordering) given partitioning is by `scope_unit_id`? (Kafka couples key→partition; we'd set partition explicitly or accept `scope_unit_id` as both.) Recommend `scope_unit_id` as the Kafka key and `idempotency_key` as a header-level dedup token.
2. **OQ-2 — quota gate post-cutover. RESOLVED.** The `request_cap`/daily-budget gate lives at the serialized dispatcher boundary before cutover. **Principle adopted:** on Kafka cutover the quota gate moves into a *consumer-side rate limiter* — it must never be approximated by partition count or broker config. Re-homing the gate into the consumer is a **required pre-cutover task**, not a free decision at cutover time.
3. **OQ-3 — pre-launch lease model.** Keep tx-scoped `SKIP LOCKED` (single-worker, simplest) vs. add `lease_owner`/`lease_until` + a shared Postgres lane-budget counter now to enable multi-worker without Kafka? Recommendation: stay tx-scoped until trigger #1/#2.
4. **OQ-4 — `raw_written` volume.** Per-page granularity is chosen; confirm acceptable after a sanity-check against national page counts, or default multi-page jobs to the `(last_object_key, object_count)` compact form.
5. **OQ-5 — R2 content hash source. RESOLVED.** The worker **tee-hashes the upload stream** (compute `sha256` over the bytes as they are streamed to R2) and that producer-computed digest is the integrity hash in `raw_written`. We **do not** trust the R2/S3 `ETag`: for multipart uploads the ETag is not a stable, verifiable whole-object `sha256`. Hashing is provider-independent and survives a storage-adapter change.
6. **OQ-6 — boundary publication. RESOLVED.** The fabric stays **Foundation-private**. The **only** published consumer contract is the `collection.raw_written` event name and schema. Internal job, retry, DLQ topics and the `JobBus` port are not public contracts. This keeps the dispatch adapter swappable without a consumer contract change.
7. **OQ-7 — SRID for V-World land-register (`ladfrlList`).** Confirm `srid = null` (attribute-only) rather than inheriting cadastral's `EPSG:4326`.
8. **OQ-8 — DLQ replay authorization.** Replay re-spends provider quota — should operator-initiated replay require the same rollout-approval gate (`national_data_collection_rollout_approval_check.rs`) as a fresh run, or be exempt?

---

## 참고 문서

- Refines [ADR-0046](./0046-kafka-kubernetes-preliminary-design.md) (Kafka transport ladder + triggers). Builds on [ADR-0026](./0026-bronze-api-archive-r2-not-postgres-jsonb.md) (Bronze in R2), [ADR-0032](./0032-eventual-consistency-strategy.md) (outbox/eventual consistency), [ADR-0039](./0039-service-owned-lakehouse-registry-integration.md) (service-owned lakehouse). Governed by [ADR-0044](./0044-bazel-transition-reconciliation.md) + [AGENTS.md](../../AGENTS.md) ✱ product-first.
- Patterns: EIP Claim-Check / Transactional Outbox / Dead-Letter Channel / Competing Consumers / Idempotent Receiver; Uber Chaperone (Kafka end-to-end audit).
- foundation-platform code: `services/foundation-outbox-publisher/src/national_data_collection_async/{ledger,events,plan}.rs`, `national_bronze_object_manifest.rs`, `national_data_collection_ledger_execute/support/{runner,bronze_result,reuse_manifest,job_outcome}.rs`, `national_data_collection_coverage_ledger_check.rs`, `public_provider_rate_policy.rs` + `provider_request_spacing.rs` + `docs/catalog/provider-rate-policy.v1.json`, `crates/foundation-outbox/src/{broadcaster,worker,lineage}.rs`, `crates/foundation-shared-kernel/src/events/catalog_v1.rs`, `migrations/20260519000001_postgis_mirror_dlq.sql`.

---

> Package and service paths use the monorepo-unique names required by root ADR-0001. Historical
> pre-consolidation paths are not part of this contract.
