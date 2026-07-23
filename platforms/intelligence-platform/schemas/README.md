# schemas/

Avro schema files (.avsc) for events published by the intelligence-platform.

## What lives here

One file per versioned event topic:

| File | Topic | Kafka key |
|------|-------|-----------|
| `intelligence.normalization-proposal.submission-requested.v1.avsc` | `intelligence.normalization-proposal.submission-requested.v1` | `aggregate_id` (= `idempotency_key`) |

## Evolution discipline — BACKWARD_TRANSITIVE

All schemas in this directory are governed by **BACKWARD_TRANSITIVE** compatibility.

Rules:
- **Additive-only within a version:** new fields MUST carry an Avro `default` and MUST be appended after all existing fields. Consumers on the previous schema version silently ignore the new field; producers on the new schema version fill it.
- **Never mutate a published field:** do not rename, remove, change the type of, or reorder any field once the schema file has been committed to `main`.
- **Breaking change = new file + new topic:** if a structural break is unavoidable (rename, type change, field removal), create a new file (e.g. `.v2.avsc`), a new topic, and run the old and new topics in parallel for the agreed migration window before decommissioning the v1 topic.

## Schema registry (C2 plan)

At C2 the schemas will be registered in **Karapace** (Confluent-compatible schema registry) using **TopicNameStrategy**: the registered subject name is `<topic>-value` (e.g. `intelligence.normalization-proposal.submission-requested.v1-value`).

Producers will serialize with the 5-byte Confluent wire-format prefix (`\x00 + schema_id_int32_big_endian`) before the Avro payload. Consumers use the same registry to resolve the schema ID on read.

## Contract test

`crates/normalization/intelligence-normalization-application/tests/event_schema_contract.rs` pins schema-to-code compatibility by:

1. Parsing the .avsc file at test time via `apache_avro::Schema::parse_str`.
2. Performing a full serialize → deserialize round-trip of a `NormalizationOutboxRecord` fixture.
3. Asserting that every field in the parsed schema either is in the required-fields set or carries an Avro `default` — this is the additive-evolution tripwire: adding a field without a default fails the test immediately.

Run: `cargo test -p intelligence-normalization-application --test event_schema_contract`.

## C2 live event backbone verification

Run these commands from the Intelligence Platform workspace root.

The compose harness defaults to:

- Kafka on `127.0.0.1:19092`
- Karapace on `http://127.0.0.1:18081`

On Windows, this live test path needs a working `cmake-build` toolchain for
`rdkafka`: use VS BuildTools/MSVC plus BuildTools CMake and BuildTools Ninja,
or an equivalent setup. The current module and platform boundary is documented
in `../docs/architecture.md`.

Start local dependencies:

```bash
docker compose -f docker/c2-event-backbone.compose.yml up -d
```

Run live test:

```bash
INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
INTELLIGENCE_TEST_KARAPACE_URL=http://127.0.0.1:18081 \
cargo test -p messaging-infrastructure --test live_kafka_karapace -- --nocapture
```

If those host ports conflict with existing services, override them before `docker compose up`.
For this host, `18081` is already in use, so the conflict-safe Karapace override is:

```bash
INTELLIGENCE_TEST_KARAPACE_HOST_PORT=18082 \
docker compose -f docker/c2-event-backbone.compose.yml up -d
INTELLIGENCE_TEST_KARAPACE_URL=http://127.0.0.1:18082 \
cargo test -p messaging-infrastructure --test live_kafka_karapace -- --nocapture
```

Kafka can also be moved if needed:

```dotenv
INTELLIGENCE_TEST_KAFKA_HOST_PORT=19093
INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093
```

Stop dependencies:

```bash
docker compose -f docker/c2-event-backbone.compose.yml down -v
```
