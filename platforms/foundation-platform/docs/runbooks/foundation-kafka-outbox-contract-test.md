# Foundation Kafka outbox live contract

This is the pre-production proof for the existing Postgres outbox to Kafka relay. Normal
`cargo test` and `cargo xtask verify foundation` intentionally remain credential-free. The live
gate is explicit, ignored by default, and fails when required services are absent.

## What the gate proves

The gate runs the same path used by the publisher service:

1. a real Redpanda broker and Karapace register the checked-in Avro schema;
2. the canonical topic `foundation-platform.catalog.collection-raw-written.v1` is provisioned
   explicitly with one partition and one replica for the disposable stack;
3. a real Postgres `catalog.outbox_event` row is leased by `OutboxWorker`, delivered as a
   Confluent-Avro claim-check, and marked with `published_at` only after delivery;
4. an unreachable Kafka bootstrap causes retry and quarantine, with `published_at` remaining
   null; and
5. the Kafka value contains `event_id`, `scope_unit_id`, and the R2 object key/checksum/counts,
   never raw R2 bytes.

Delivery is at-least-once. A consumer must deduplicate by `event_id`; exactly-once is not claimed.
The Postgres outbox remains the durable source of truth and is the replay/rollback mechanism.

## Run the complete live gate

Prerequisites: Docker, `curl`, and a disposable Postgres/PostGIS database with `DATABASE_URL` set.
When Cargo/Rust 1.96 is on `PATH`, the script runs tests on the host; otherwise it automatically
uses the pinned Rust verification container. The script owns the pinned Redpanda/Karapace stack
and always removes it. If Postgres is also container-network-only, set
`FOUNDATION_KAFKA_TEST_CONTAINER_DATABASE_URL` to its network URL.

```bash
export DATABASE_URL='postgres://foundation_platform:foundation_platform_dev_2026@127.0.0.1:5432/foundation_platform'
bash scripts/verify/foundation-kafka-live.sh
```

The script starts `platforms/intelligence-platform/docker/c2-event-backbone.compose.yml`, waits for
both services, creates/describes the canonical topic, exports the required Kafka variables, and
runs these ignored tests with `--nocapture`:

- `live_kafka_karapace`
- `live_kafka_outbox_roundtrip`
- `live_kafka_outage`

Use `INTELLIGENCE_TEST_KAFKA_HOST_PORT`, `INTELLIGENCE_TEST_KARAPACE_HOST_PORT`, and
`FOUNDATION_KAFKA_COMPOSE_PROJECT` when another local stack uses the default ports/project name.
The script exits non-zero for missing Docker, Postgres, broker, registry, topic, schema, or test
failures. It never turns a required-mode failure into a green skip.

## CI

The `kafka-integration` job in `.github/workflows/foundation-ci.yml` provisions the pinned
PostGIS service, runs Foundation migrations, invokes the same script, and has an `if: always()`
Compose cleanup step. Its result is included in `required/foundation`; a skipped or failed live
Kafka gate blocks the workflow.

## Production cutover checklist

Do not enable Kafka in staging/production until all of these have evidence in the deployment
system:

- managed Kafka bootstrap endpoints and TLS/SASL configuration;
- HTTPS Schema Registry endpoint and authentication/CA trust;
- canonical topic owner, partition count, replication factor, retention, and ACLs;
- consumer contract test showing `event_id` deduplication and R2 claim-check reads;
- publish latency/error, retry, quarantine, duplicate, and schema/topic/ACL alerts;
- an observation period with `FOUNDATION_PLATFORM_KAFKA_DUAL_PUBLISH_LEGACY=1`.

Local Redpanda and Karapace are test infrastructure, not a production substitute. No Kafka or
Schema Registry credentials belong in the repository.

## Rollback

1. Set `FOUNDATION_PLATFORM_KAFKA_ENABLED=0` and restart the publisher. Existing Bronze/R2 and
   Postgres outbox commits continue without Kafka.
2. Confirm pending rows remain `published_at IS NULL`; replay them after broker/schema/topic repair
   using the normal outbox worker.
3. Inspect `catalog.outbox_quarantine` before re-enabling delivery. Consumers must tolerate
   duplicates by `event_id` after a partial Kafka acknowledgement.
