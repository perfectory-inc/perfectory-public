# Outbox Webhook Fan-Out Runbook

`foundation-outbox-publisher` can deliver non-manifest Catalog events and Staff Identity events to HTTP webhook endpoints. This is for consumer cache invalidation in Gongzzang and Dawneer, and for Gongzzang's parcel marker anchor import enqueue path. Vector tile manifest promote/rollback events still publish the canonical R2 pointer first.

## Configuration

Set semicolon-separated `name=url` pairs:

```bash
export FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_ENDPOINTS="gongzzang=https://gongzzang.example.invalid/foundation-platform/events;dawneer=https://dawneer.example.invalid/foundation-platform/events"
export FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET="<shared-webhook-hmac-secret>"
```

Remote endpoints must use `https`. Plain `http` is accepted only for loopback development URLs such as `http://127.0.0.1:3000/foundation-platform/events`.

## Run

```bash
export DATABASE_URL="postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
cargo run -p foundation-outbox-publisher -- run
```

The publisher posts one JSON envelope per outbox event and includes:

- `x-foundation-platform-event-id`
- `x-foundation-platform-event-type`
- `x-foundation-platform-outbox-scope`
- `x-foundation-platform-signature`
- `x-foundation-platform-timestamp`

`x-foundation-platform-signature` is `v1=<hmac_sha256_hex>`, calculated over
`<x-foundation-platform-timestamp>.<raw JSON request body>` with
`FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET`. Keep this secret in the deployment secret
store and rotate it together with the consumer-side `FOUNDATION_PLATFORM_WEBHOOK_SECRET`.

The consumer-facing envelope fixture is
`docs/events/webhook/outbox-webhook-envelope.v1.example.json`, verified in CI.

The receiver contract fixture is `docs/events/webhook/receiver-contract.v1.example.json`.
It records the Gongzzang and Dawneer receiver slugs, endpoint path, required idempotency key,
accepted 2xx acknowledgements, maximum acknowledgement latency, required acknowledgement body,
cache invalidation effect, and anchor import enqueue effect, verified in CI.

Any non-2xx response fails the publish attempt, increments `retry_count`, and leaves the event unpublished for retry.

## Parcel Marker Anchor Snapshot Event

`export-parcel-marker-anchor-artifacts` writes immutable anchor JSONL objects and
`manifest.json`, then inserts `catalog.parcel_marker_anchor.snapshot.published.v1`
into `catalog.outbox_event`. The outbox worker delivers that event to Gongzzang,
where the receiver stores it as a durable anchor import job.

The export command requires an absolute artifact base URL so consumers never need
provider-specific object keys:

```bash
export FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_PUBLIC_BASE_URL="https://static.foundation-platform.example.com"
```

The emitted payload uses:

- `anchor_snapshot_id`: `anchor-snapshot-<export_run_id>`
- `source_geometry_version`: the configured source snapshot id
- `artifact_manifest_url`: public base URL plus the versioned manifest object key
- `artifact_checksum_sha256`: the export manifest checksum
- `row_count`: accepted anchor row count

Do not bypass this outbox event with an ad-hoc direct call to Gongzzang. The
outbox row is the retry, replay, and audit boundary.

## Verification

```bash
cargo test -p foundation-outbox --test webhook_broadcaster
cargo test -p foundation-outbox-publisher webhook_endpoint_specs
cargo test -p foundation-outbox-publisher outbox_record_is_derived_from_export_summary
```

The event-schema-compatibility and webhook envelope/receiver contract fixtures are verified in CI.

This verifies sender-side envelope shape, trace headers, HTTPS/loopback URL policy, and retry behavior on non-2xx responses. It does not prove that Gongzzang or Dawneer have deployed receiver endpoints.

For a local DB-backed smoke that inserts a `catalog.outbox_event` row, runs `OutboxWorker.tick()`,
posts to a local HTTP receiver, and marks the row published:

```bash
export DATABASE_URL="postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
cargo test -p foundation-outbox --test publish_roundtrip tick_delivers_catalog_event_to_webhook_and_marks_published_at -- --ignored --exact
```

This proves foundation-platform sender fan-out through the real outbox worker. It still does not prove
that Gongzzang or Dawneer receiver endpoints have been implemented or deployed.
Before M3.2 cutover, run a cross-repo E2E that posts every supported receiver-contract event
to the deployed consumers and verifies idempotent cache invalidation plus anchor import enqueue
behavior.

### Deployed receiver E2E (cross-repo)

> 2026-06-21 note: the PowerShell external-prerequisite checker, the remote
> webhook-receiver-e2e smoke runner, and the GitHub `consumer_receiver_e2e` cutover-evidence
> workflow were all removed as ceremony.

A deployed-receiver E2E still has to happen before any cutover claim, but it is now driven from the
consumer repositories. Each consumer (`gongzzang`, `dawneer`) must:

- expose its `/foundation-platform/events` receiver endpoint with the shared
  `FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET`;
- accept every event in `docs/events/webhook/receiver-contract.v1.example.json` (the gold-pointer
  cache-invalidation event and the parcel-marker-anchor snapshot enqueue event), returning the
  required acknowledgement body within the contract latency budget;
- prove, from its own test suite, that the cache invalidation is idempotent and wired to the real
  cache layer and that the anchor snapshot enqueues a durable import job.

Loopback, documentation, and placeholder hosts do not count as deployed-receiver evidence.
