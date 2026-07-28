# Runtime environment separation

This runbook is the operator-facing companion to ADR 0029.

## Environment variable

Every operational Foundation publisher process must set:

```dotenv
FOUNDATION_PLATFORM_RUNTIME_ENV=local|ci|staging|production
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer|ci|service
FOUNDATION_PLATFORM_PRELAUNCH_SHARED=0|1
```

The publisher rejects missing or unknown values. Do not use `prod`, `dev`, or free-form names.
`PRELAUNCH_SHARED=1` is accepted only for a developer process explicitly targeting production.

## R2 configuration

Developer local uses Cloudflare R2, not MinIO:

```dotenv
FOUNDATION_PLATFORM_RUNTIME_ENV=local
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
FOUNDATION_PLATFORM_PRELAUNCH_SHARED=0
FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev
```

Use a bucket-scoped R2 token. Production credentials are allowed in `.env.local` only under the
explicit, temporary pre-launch exception below.

### Current pre-launch exception

The product is not launched yet, so the current private `.env.local` intentionally selects the
existing production R2/Data Catalog with `FOUNDATION_PLATFORM_RUNTIME_ENV=production`. This is
explicit production mode from a developer machine, not an alias of `local`. Do not apply this
exception after external launch; switch to `runtime=local` and the dedicated development bucket
first. Postgres, Valkey, Kafka, Identity, and compute remain local until real production endpoints
are provisioned in private operations configuration.

The current private pre-launch profile therefore contains:

```dotenv
FOUNDATION_PLATFORM_RUNTIME_ENV=production
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1
FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-prod
```

The expected bucket names are:

```text
local       foundation-platform-lakehouse-dev (remote R2 development bucket)
ci          foundation-platform-lakehouse-ci
staging     foundation-platform-lakehouse-staging
production  foundation-platform-lakehouse-prod
```

## Redpanda/Karapace

Start the local C2 fixture only for Kafka/Avro contract work:

```bash
docker compose -f platforms/intelligence-platform/docker/c2-event-backbone.compose.yml up -d
```

Run its live tests explicitly with the broker and registry environment variables. This does not
publish a Foundation production topic. Production broker selection and producer wiring are tracked
separately.

## Credential-free verification

Ordinary Cargo verification and unit tests must remain credential-free. They may use mocks, files,
and logging adapters. Those tests do not prove R2 or Kafka connectivity.

Protected live smoke jobs must provide the dedicated CI/staging credentials and set the matching
runtime environment. A live smoke job must fail when an explicitly required backend is absent; it
must not turn a missing service into a passing soft skip.

## Safety checks

Before a live collection or publisher run, verify:

1. The runtime environment is explicit.
2. The execution context is explicit; developer→production requires the pre-launch flag.
3. Bronze and Catalog drivers are `r2` outside local/CI bounded tests.
4. `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET` matches the environment-specific bucket.
5. The R2 token is scoped to that bucket.
6. No fixture topic, local file root, logging adapter, or process-local state is used in staging or
   production.

The Bronze publisher enforces this at the write boundary as well as at command startup: every
live object-storage adapter is constructed through the shared preflighted
`live_write_bronze_object_storage_from_env` or
`live_write_bronze_streaming_object_storage_from_env` helper. The backend-profile guard rejects a
new ingest caller that bypasses those helpers. This is defense in depth for future collection
commands; it does not replace the earlier preflight that runs before a provider download.
