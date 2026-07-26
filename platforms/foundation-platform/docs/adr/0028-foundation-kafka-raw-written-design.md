# Foundation `collection.raw_written` Kafka Relay

<!-- public-repository-safety: reviewed-public-contract -->

## Status

Implemented as an opt-in post-commit adapter. Production cutover remains gated on broker,
schema-registry, topic/ACL, and consumer readiness evidence.

## Goal

Publish the Foundation collection claim-check event `catalog.collection.raw_written.v1` to a real Kafka-compatible broker without making collection code or the JobBus depend on Kafka directly. Postgres remains the publishable-event source of truth, R2 remains the raw-byte store, and the existing outbox retry/quarantine contract remains authoritative.

## Context and constraints

Foundation already has the relevant seams:

- `OutboxRawWrittenSink` records the typed collection event in `catalog.outbox_event`.
- `OutboxWorker` claims unpublished rows, calls an `EventBroadcaster`, and marks a row published only after the broadcaster returns success.
- `CollectionRawWrittenV1` is a claim-check contract: it contains the R2 object pointer, checksum, counts, and lineage, never raw provider bytes.
- `JobBus` is deliberately transport-neutral and remains JSONL/in-memory in this slice.
- The current fallback broadcaster is webhook or logging. `foundation-outbox` now owns the opt-in
  Kafka producer adapter; collection and JobBus crates do not depend on Kafka.

The design follows the stable part of large-scale deployments rather than copying their entire infrastructure:

- Zapier documents a durable local outbox, asynchronous poller, Avro/Schema Registry, and at-least-once Kafka delivery; it later moved its durability fallback toward S3/SQS after encountering local-outbox scaling and operational costs.
- Shopify, Uber, and Yelp document CDC-to-Kafka systems with partition keys, schema governance, replay, and materialized downstream views. Those are a later scale path, not a reason to bypass the current transactional outbox.
- Debezium's Outbox Event Router is the future CDC adapter target. The event contract and topic names must therefore remain compatible with an eventual Kafka Connect migration.

## Alternatives considered

### A. Foundation polling publisher with a Kafka adapter (selected)

Add a `KafkaEventBroadcaster` to `foundation-outbox`. The existing worker remains the only database poller and performs claim, publish, retry, and quarantine. The adapter publishes the supported event to Kafka and returns only after broker acknowledgement. This is the smallest runnable path for the current repository and preserves a clean migration seam to Debezium later.

### B. Debezium/Kafka Connect now

Capture `catalog.outbox_event` with a Debezium connector and route it using the Outbox Event Router SMT. This is the preferred high-volume operating model, but it requires Kafka Connect, connector configuration, offset storage, deployment ownership, and CI orchestration that do not exist in Foundation today. It is a follow-up adapter, not this slice.

### C. Direct Kafka writes from collection workers

Rejected. It creates a database/R2/Kafka dual-write window, couples Bronze ingestion to broker availability, and violates the existing `RawWrittenSink` and outbox boundary.

## Architecture

```text
Bronze/R2 write
      |
      v
collection worker --(same success path)--> catalog.outbox_event (Postgres SSOT)
                                                |
                                                v
                                      OutboxWorker claim/lease
                                                |
                                                v
                              KafkaEventBroadcaster (raw_written)
                               |                 |
                               |                 +--> Karapace schema registration
                               v
                        Kafka topic (Avro)
```

The event row is acknowledged only after Kafka's producer delivery future succeeds. A process crash after Kafka acknowledgement and before `published_at` is written can produce a duplicate; this is intentional at-least-once behavior. The stable outbox `event_id` is carried in the Kafka value and consumers must deduplicate it.

### Event routing

- Supported Kafka event type: `catalog.collection.raw_written.v1`.
- Canonical topic: `foundation-platform.catalog.collection-raw-written.v1`.
- Schema subject: `<topic>-value` using Karapace TopicNameStrategy.
- Key: `scope_unit_id`, preserving per-scope ordering and matching the existing collection event contract.
- Value: Confluent Avro wire format (`0x00` magic byte + 4-byte schema id + Avro payload).
- Schema artifact: `platforms/foundation-platform/schemas/foundation-platform.catalog.collection-raw-written.v1.avsc`, record name `CollectionRawWrittenEnvelopeV1`, namespace `kr.perfectory.foundation.catalog`.
- Avro field order and mapping are fixed for v1: `event_id` (string, canonical hyphenated UUID from the outbox envelope), `event_type` (string, default `catalog.collection.raw_written.v1`), `specversion` (string, default `1.0`), `source` (string, default `/foundation-platform/collection`), `schema_version` (int from the typed payload), `collection_snapshot_id` (string), `job_id` (string), `scope_unit_id` (string), `provider` (string), `endpoint` (string), `endpoint_slug` (string), `bronze_object_key` (string), `bronze_object_count` (long), `bronze_checksum_sha256` (string), `bronze_size_bytes` (long), `source_record_count` (long), `request_count` (long), `request_fingerprint_sha256` (string), `request_fingerprint_schema_version` (string), `license` (`["null","string"]`, default null), `srid` (`["null","string"]`, default null), `reused_bronze_object` (boolean, default false), `fetched_at_utc` (timestamp-millis long from the typed payload), `event_occurred_at` (timestamp-millis long from the typed payload), and `outbox_occurred_at` (timestamp-millis long from `EventEnvelope.occurred_at`).
- UUIDs are encoded as strings. Every Rust `u64` count must fit `i64::MAX` before Avro conversion; otherwise publication fails and follows the existing quarantine path. `DateTime<Utc>` values are converted with `timestamp_millis()` and preserve the typed payload timestamp versus the outbox-row timestamp as separate fields.
- Raw provider bytes and full Bronze rows never enter Kafka.

Other catalog event types continue through the configured legacy fallback broadcaster. Composition is explicit: `CatalogEventBroadcaster` still intercepts vector-manifest events; its fallback is a `KafkaEventBroadcaster`, whose own fallback is the existing webhook/logging broadcaster. For `raw_written`, Kafka is attempted first. If Kafka fails, the legacy fallback is not called and the worker retries the row. If Kafka succeeds and legacy dual-publish is enabled but legacy fails, the worker reports failure and the next attempt may publish a duplicate Kafka record; this is accepted at-least-once behavior and the stable `event_id` is the deduplication key. With dual-publish disabled, a successful Kafka publish is sufficient for the row. Non-`raw_written` events always go only to the legacy fallback.

### Configuration and rollout gate

Kafka is opt-in and fail-closed when enabled:

- `FOUNDATION_PLATFORM_KAFKA_ENABLED` (default `false`).
- `FOUNDATION_PLATFORM_RUNTIME_ENV` (`local`, `ci`, `staging`, or `production`) is required
  whenever Kafka is enabled. The Kafka adapter enforces this itself so a direct library caller
  cannot bypass the publisher's runtime boundary.
- `FOUNDATION_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` (required when enabled).
- `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_URL` (required when enabled).
- `FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC` (default canonical topic).
- `FOUNDATION_PLATFORM_KAFKA_CLIENT_ID` (default stable service id).
- `FOUNDATION_PLATFORM_KAFKA_MESSAGE_TIMEOUT_MS` (positive) and `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_TIMEOUT_SECONDS` (positive).
- `FOUNDATION_PLATFORM_KAFKA_SECURITY_PROTOCOL` (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`; default `PLAINTEXT`).
- `FOUNDATION_PLATFORM_KAFKA_SASL_MECHANISM`, `FOUNDATION_PLATFORM_KAFKA_SASL_USERNAME`, and `FOUNDATION_PLATFORM_KAFKA_SASL_PASSWORD` when a SASL protocol is selected.
- `FOUNDATION_PLATFORM_KAFKA_SSL_CA_LOCATION`, `FOUNDATION_PLATFORM_KAFKA_SSL_CERTIFICATE_LOCATION`, and `FOUNDATION_PLATFORM_KAFKA_SSL_KEY_LOCATION` for file-based broker TLS when required.
- `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_USERNAME` and `FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_PASSWORD` for optional Karapace HTTP Basic Auth. HTTPS uses the host trust store; a future CA-bundle option is a separate change.
- `FOUNDATION_PLATFORM_KAFKA_DUAL_PUBLISH_LEGACY` (default `true` during rollout; set `false` after consumer cutover evidence).

Kafka topic creation is an external provisioning step; the adapter sets `allow.auto.create.topics=false` and fails startup/first publish if the topic is absent. Production provisioning must use at least three partitions, replication factor three where the broker topology permits, `cleanup.policy=delete`, and an explicit retention/ACL policy. The single-node CI broker uses one partition and one replica. No Kafka credentials are committed. TLS/SASL properties are passed through environment configuration and are not logged.

## Components and responsibilities

### `foundation-outbox` Kafka adapter

- Own `KafkaEventBroadcaster`, configuration validation, Karapace schema registration, Avro value conversion, and producer delivery.
- Depend on the Kafka client only inside this infrastructure crate; collection domain/application crates remain unaware of Kafka.
- Reject malformed `raw_written` payloads before attempting publication and return `PublishError::Broadcaster` so the existing retry/quarantine path records the failure.
- Use bounded producer timeouts and no unbounded buffering in the worker path.

### Publisher service composition

- Parse the opt-in Kafka configuration at startup.
- Build the exact wrapper chain described above only when Kafka is enabled; otherwise preserve the existing fallback unchanged.
- Keep the existing vector-tile manifest pointer behavior unchanged.
- Keep `run-publisher` and `publish-once` behavior identical when Kafka is disabled.

### Schema artifact

Commit one append-only Avro schema for `catalog.collection.raw_written.v1` at the path and record identity above. The initial fields have the exact types/defaults listed above; later fields are appended with defaults only. Compatibility is `BACKWARD_TRANSITIVE`. Any incompatible change requires a new schema version and topic; published fields are never deleted or retyped.

## Governing ADR status

Foundation ADR-0013 and ADR-0016 remain authoritative for the Bronze commit protocol: Kafka is not a Bronze write dependency, raw bytes stay in R2, and the ledger remains SSOT. This design is an explicitly opt-in **post-commit outbox adapter**, not a reversal of those ADRs or a national rollout gate. This ADR records the limited adapter, the topic/schema contract, and the eventual Debezium migration trigger.

## Error handling and delivery semantics

1. Configuration/schema registration errors fail startup when Kafka is enabled.
2. Serialization, schema, transport, and delivery errors are returned as broadcaster errors.
3. `OutboxWorker` increments retry count, releases the lease, and retries on the next tick.
4. Exhausted retries are written to `catalog.outbox_quarantine`; no event is silently dropped.
5. A successful Kafka delivery followed by a database failure may duplicate on retry. The event id is the idempotency key; exactly-once is not claimed.
6. Kafka broker outages do not lose committed Postgres outbox rows and do not block Bronze writes.

## Testing and operational evidence

### Unit and contract tests (normal Cargo verification)

- Configuration validation: required endpoints, positive timeouts, topic/client id, and invalid combinations.
- Avro conversion: every `CollectionRawWrittenV1` field, timestamp-millis encoding, event id, key selection, and claim-check rule.
- Schema subject/topic constants and backward-compatible fixture validation.
- Routing: Kafka handles only `raw_written`; fallback receives other event types and optional legacy dual-publish calls.
- Existing outbox worker tests continue to verify retry/quarantine behavior with a recording broadcaster.

### Live integration test

Three explicitly named live tests are implemented. `live_kafka_karapace.rs` exercises schema
registration, Avro wire encoding, unique-key publish, consume, schema-id verification, and offset
commit directly against Redpanda/Karapace. `live_kafka_outbox_roundtrip.rs` requires
`DATABASE_URL`, inserts a real `catalog.outbox_event` row, constructs the same publisher
composition used by `run-publisher`, invokes `OutboxWorker::tick()`, consumes the resulting record,
and asserts `published_at` is set. `live_kafka_outage.rs` constructs the real librdkafka producer
against an unreachable bootstrap and asserts retry, unpublished state, and quarantine at the retry
limit. All three tests are `#[ignore]` for ordinary credential-free Cargo verification. They fail
fast (not print-and-return) when `FOUNDATION_TEST_KAFKA_REQUIRED=1` or `DATABASE_URL` is required
but absent.

Reuse the existing pinned Redpanda+Karapace compose definition at `platforms/intelligence-platform/docker/c2-event-backbone.compose.yml` rather than duplicating image digests. Document the Foundation command and environment names next to the test.

### CI and local run

- Normal `cargo xtask verify foundation` remains credential-free and does not require Kafka.
- A separate `kafka-integration` Foundation job starts the pinned compose stack, creates the
  one-partition CI topic explicitly, waits for broker/registry readiness, sets
  `FOUNDATION_TEST_KAFKA_REQUIRED=1`, runs all three ignored live tests through
  `scripts/verify/foundation-kafka-live.sh`, and tears the stack down in an `always` cleanup step.
  Missing services or test failures fail the required workflow gate.
- Local developers run the same script with a disposable Postgres database and no cloud
  credentials. Managed Kafka deployment later supplies bootstrap/TLS/SASL values only.

## Non-goals

- This Kafka relay slice does not select or make the `PostgresJobBus` the national executor default;
  the adapter and its real-Postgres contract tests are tracked by ADR-0013 and migration
  `20260725000001_collection_job_bus.sql`.
- No change to national collection executor scheduling or direct-loop behavior.
- No raw R2 object replication into Kafka.
- No Silver/Gold, Trino, Spark, Iceberg, or LLM normalization changes.
- No Debezium/Kafka Connect deployment yet.
- No claim of exactly-once delivery or automatic consumer cutover.

## Acceptance criteria

1. With Kafka disabled, existing Foundation tests and publisher behavior are unchanged.
2. With Redpanda+Karapace running and Kafka enabled, a committed `catalog.collection.raw_written.v1` row is published as a schema-registered Avro record with the correct key and claim-check fields.
3. A broker outage leaves the outbox row unpublished and exercises retry/quarantine rather than losing it.
4. CI runs the three live tests against the real/emulated broker and fails when the broker or schema registry is unavailable.
5. No collection crate imports `rdkafka`; only the outbox infrastructure adapter does.
6. The event contract and topic remain usable by a future Debezium Outbox Event Router migration.
