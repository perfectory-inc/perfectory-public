# ADR 0029 - Runtime Environment Backend Separation

## Status

Accepted. This ADR defines the environment boundary for Foundation runtime backends. It does not
select the production Kafka provider; that remains a separate Kafka implementation decision.

## Decision

Foundation has four explicit runtime environments:

| Runtime | Object storage | Broker | Durable DB | Model/provider policy |
|---|---|---|---|---|
| `local` | Dedicated Cloudflare R2 development bucket | Redpanda + Karapace local C2 | Local Docker Postgres | Provider keys and model gateway are developer-owned |
| `ci` | Dedicated CI R2 bucket for protected live smoke; logging is allowed only for credential-free compose smoke | Redpanda + Karapace test fixture | Ephemeral Postgres service | Mock providers/models unless a protected live job says otherwise |
| `staging` | Dedicated Cloudflare R2 staging bucket | A managed production-compatible broker selected by the deployment | Staging Postgres | Staging provider/model credentials |
| `production` | `foundation-platform-lakehouse-prod` | The selected production broker | Production Postgres | Production credentials only |

The developer environment deliberately uses R2. MinIO is not a Foundation development dependency.
Unit tests may use fakes, but operational commands must use the runtime environment's declared
backend and must not silently fall back to a fake.

## Pre-launch sharing exception

Until the product launches, a developer process may intentionally run with
`FOUNDATION_PLATFORM_RUNTIME_ENV=production` and use the existing production R2/Data Catalog.
This is a temporary operational choice, not a new `local` environment: the execution location is
still a developer machine, while the selected backend environment is explicitly `production`.
The process must also set `FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer` and the narrow
acknowledgement `FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1`; the publisher rejects a developer→production
run without that acknowledgement.
The local metadata database, Valkey, Kafka, identity provider, and compute remain local unless a
real production endpoint is supplied through private operations configuration. They must not be
invented from local compose hostnames. Before external launch, switch the process to `local` and
the dedicated development bucket, then provision and validate separate non-production endpoints.

## R2 bucket identities

The non-production bucket names are:

- `local` (developer process): `foundation-platform-lakehouse-dev` (remote Cloudflare R2 development bucket)
- `ci`: `foundation-platform-lakehouse-ci`
- `staging`: `foundation-platform-lakehouse-staging`
- `production`: `foundation-platform-lakehouse-prod`

The production bucket remains owned by the lakehouse domain SSOT:
`LakehouseOwnerService::FoundationPlatform::production_r2_bucket_name()`.
The runtime policy must call that function instead of duplicating the production string.

Every environment receives separate R2 credentials. Tokens are scoped to the environment bucket;
developers and CI must not hold production R2 credentials. A wrong bucket is a startup/preflight
error, not a warning.

## Fallback rules

- `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=local` is a local/CI bounded-test option only.
- `FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=log` is a local/CI compose-smoke option only.
- `InMemoryJobBus`, process-local Intelligence state, fixture topics, and local Parquet are test or
  development aids, not staging/production backends.
- Staging and production require `r2` for Bronze and Catalog object storage.
- Staging and production must fail closed when `FOUNDATION_PLATFORM_RUNTIME_ENV` is missing or
  unknown.
- Production must never inherit a default local, logging, memory, or fixture configuration.

## Redpanda/Kafka boundary

Redpanda/Karapace is the local and CI C2 broker/registry used to test Kafka and Avro contracts.
It is not evidence that Foundation production publishes Kafka events. A production broker must be
selected and wired through the separate Kafka design before any staging/production broker setting is
accepted.

## Enforcement

`foundation-outbox-publisher` validates the runtime environment at operational Catalog and Bronze
live-write boundaries. Every Bronze write path must build its adapter through the shared
preflighted `live_write_bronze_*_object_storage_from_env` boundary; an additional source-level
guard rejects direct use of the unvalidated configuration builders outside the policy module.
Callers may still run the same preflight before provider downloads so a bad target fails before
large response bodies are streamed. Unit tests remain credential-free; protected R2/Kafka live
smoke tests are separate from ordinary Cargo verification and must fail when explicitly required
services are unavailable.
