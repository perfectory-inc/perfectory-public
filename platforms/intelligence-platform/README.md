# Intelligence Platform

Rust-first implementation path for the enterprise intelligence-platform.

This Rust workspace is the **canonical, source-of-truth** implementation of the
platform boundary: APIs, validation, provenance, idempotency, outbox state,
adapters, and Foundation Platform submission. The former Python prototype was
retired on 2026-07-08 (see `docs/adr/0001-canonical-implementation-rust.md`);
it is no longer part of the deployable estate or the contract-reference set. The
Foundation Platform wire contract is defined solely here and under `schemas/`.

## Shape

- `crates/intelligence-contracts`: shared wire and identity contracts.
- `crates/knowledge/knowledge-domain`: knowledge validation and domain types.
- `crates/knowledge/knowledge-application`: knowledge use cases and ports.
- `crates/knowledge/knowledge-infrastructure`: knowledge persistence adapters.
- `crates/normalization/intelligence-normalization-domain`: normalization rules and domain types.
- `crates/normalization/intelligence-normalization-application`: normalization use cases and ports.
- `crates/normalization/intelligence-normalization-infrastructure`: model, Foundation, state, and rate-limit adapters.
- `crates/messaging/messaging-infrastructure`: Kafka, Avro, and schema-registry adapters.
- `services/intelligence-api`: Axum HTTP API boundary.
- `services/intelligence-worker`: background jobs, event consumption, and outbox delivery.

Apps should call intelligence-platform APIs. They should not bind directly to
Open WebUI or model servers. Open WebUI can stay useful as a development UI for
models, but it is not the production backend contract.

Production code should integrate with a model runtime or gateway endpoint, not
with the Open WebUI application endpoint. In the current local setup,
`<model-runtime-host>:8080` is Open WebUI and requires UI/API authentication, while
`<model-runtime-host>:11434` is the Ollama model runtime with an OpenAI-compatible API.
`<model-runtime-host>` stands for the operator's local model-runtime machine; keep
the real hostname/IP only in local env (`MODEL_RUNTIME_BASE_URL`), never in committed files.

## Local Ports

- Rust API scaffold: `127.0.0.1:8010`
- Current Open WebUI dev UI: `<model-runtime-host>:8080`
- Current Ollama model runtime: `<model-runtime-host>:11434`

Set `INTELLIGENCE_API_BIND=0.0.0.0:8010` only for temporary LAN access, such
as connecting Open WebUI to the policy gateway during local development. Keep
the default `127.0.0.1:8010` for single-machine development.

## Enterprise Runtime C0-C1

This section covers the production-grade configuration added in the C0-C1
foundation plan.  All configuration is via environment variables; defaults are
loopback-only and safe for single-machine development without any additional
variables set.

### Inbound authentication (fail-closed)

Binding to any non-loopback address requires inbound authentication.  The
process refuses to start if `INTELLIGENCE_API_BIND` is non-loopback and
`INTELLIGENCE_INBOUND_AUTH_MODE` is not `shared-token`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `INTELLIGENCE_API_BIND` | No | `127.0.0.1:8010` | Listen address. Non-loopback requires auth. |
| `INTELLIGENCE_INBOUND_AUTH_MODE` | Conditional | `disabled` | Set to `shared-token` for non-loopback. |
| `INTELLIGENCE_INBOUND_SERVICE_TOKEN` | When mode is `shared-token` | — | Bearer token callers must supply. It is bound to the configured workload identity below. |
| `INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID` | When mode is `shared-token` | — | Fixed service subject represented by the token; request headers cannot override it. |
| `INTELLIGENCE_INBOUND_SERVICE_TENANT_ID` | When mode is `shared-token` | — | Fixed tenant scope represented by the token. |
| `INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID` | When mode is `shared-token` | — | Fixed product scope represented by the token. |
| `INTELLIGENCE_INBOUND_SERVICE_ACTIONS` | When mode is `shared-token` | — | Comma-separated explicit actions, for example `submit_normalization_proposal`. |
| `INTELLIGENCE_CORS_ALLOWED_ORIGINS` | No | *(none — cross-origin refused)* | Comma-separated allowed origins. |

```dotenv
INTELLIGENCE_API_BIND=0.0.0.0:8010
INTELLIGENCE_INBOUND_AUTH_MODE=shared-token
INTELLIGENCE_INBOUND_SERVICE_TOKEN=replace-with-a-strong-random-secret
INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID=service:intelligence-client
INTELLIGENCE_INBOUND_SERVICE_TENANT_ID=tenant:production
INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID=foundation-platform
INTELLIGENCE_INBOUND_SERVICE_ACTIONS=submit_normalization_proposal
INTELLIGENCE_CORS_ALLOWED_ORIGINS=https://app.example.com
```

### Admission control

Three knobs control load-shedding, body size, and per-request deadlines.
Health endpoints (`/healthz`, `/readyz`, `/metrics`) are outside the admission
stack and are never shed or concurrency-blocked.

| Variable | Default | Description |
|----------|---------|-------------|
| `INTELLIGENCE_MAX_BODY_BYTES` | `1048576` (1 MiB) | Requests with larger bodies receive 413. |
| `INTELLIGENCE_REQUEST_TIMEOUT_SECONDS` | `30` | Requests exceeding this wall-clock duration receive 504. |
| `INTELLIGENCE_MAX_CONCURRENCY` | `128` | Requests arriving when the semaphore is exhausted receive 503. |

Overload response semantics:

| Status | Condition |
|--------|-----------|
| 401 | Missing or wrong `Authorization: Bearer` token |
| 413 | Body exceeds `INTELLIGENCE_MAX_BODY_BYTES` |
| 422 | Idempotency key reused with a different payload |
| 429 | Per-tenant/subject route rate limit exceeded; includes `Retry-After` |
| 503 | Global concurrency cap (`INTELLIGENCE_MAX_CONCURRENCY`) saturated |
| 504 | Request exceeded `INTELLIGENCE_REQUEST_TIMEOUT_SECONDS` |

### Durable state

When `DATABASE_URL` is set, both the normalization outbox and the audit log use
a Postgres-backed adapter.  Migrations run automatically at connect.  Without
`DATABASE_URL` the API falls back to a process-local in-memory store.

**The in-memory fallback is loopback dev only — it is NOT safe to run multiple
replicas against it; each process holds a separate store.**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(absent — in-memory fallback)* | Postgres connection string. |
| `DATABASE_TIMEOUT_SECONDS` | `10` | Connection and query timeout in seconds. |
| `DATABASE_MAX_CONNECTIONS` | `10` | Maximum pool connections (must be > 0). |

```dotenv
DATABASE_URL=postgres://user:pass@db.internal:5432/intelligence
DATABASE_TIMEOUT_SECONDS=10
DATABASE_MAX_CONNECTIONS=10
```

### Outbox drain worker

The drain worker is a separate binary that claims pending outbox records and
delivers them to Foundation Platform. Run it alongside the API process when
`DATABASE_URL` is set.  **`DATABASE_URL` is required** — the worker refuses to
start against an in-memory outbox.

```bash
cargo run -p intelligence-worker --bin normalization_outbox_drain_worker
```

| Variable | Default | Description |
|----------|---------|-------------|
| `NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE` | `4` | Records claimed per drain cycle. |
| `NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS` | `60` | Delivery lease duration per claimed record. |
| `NORMALIZATION_OUTBOX_MAX_ATTEMPTS` | `8` | Maximum delivery attempts before dead-lettering. |
| `NORMALIZATION_OUTBOX_DRAIN_IDLE_SECONDS` | `2` | Sleep between polls when the outbox is empty. |

**Lease-vs-batch invariant warning:** ensure
`NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE * FOUNDATION_PLATFORM_TIMEOUT_SECONDS` stays
well below `NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS`. If the tail of a batch
outlives its lease, another worker instance will reclaim the record and attempt
a duplicate delivery. Foundation Platform deduplication via `Idempotency-Key` is the
backstop, but keeping the batch small and the lease generous avoids the race
entirely.

### Other worker binaries

`intelligence-worker` ships three more binaries besides the drain worker:

```bash
cargo run -p intelligence-worker --bin building_register_floor_normalization
cargo run -p intelligence-worker --bin building_register_unit_normalization
cargo run -p intelligence-worker --bin foundation_knowledge_consumer
```

- `building_register_floor_normalization` — runs the building-register **floor**
  normalization proposal job (generate → validate → submit through the Foundation
  submitter; supports a dry-run mode via env).
- `building_register_unit_normalization` — same job shape for building-register
  **unit** normalization.
- `foundation_knowledge_consumer` — consumes Foundation knowledge-source events
  (Kafka + Karapace schema resolution, with DLQ) into the Postgres
  knowledge-source registry. Note: the Foundation-side producer for this topic
  does not exist yet; the default source topic is a fixture constant.

### Observability

| Endpoint | Auth required | Admission | Notes |
|----------|---------------|-----------|-------|
| `GET /healthz` | No | Exempt — 2 s timeout, 1 KiB body cap | Process liveness; always 200 while running. |
| `GET /readyz` | No | Exempt — 2 s timeout, 1 KiB body cap | Config-based readiness; 503 when model gateway or foundation submitter is unconfigured. |
| `GET /metrics` | Yes (bearer), when auth mode is `shared-token` | Exempt from shed/concurrency; 2 s timeout, 1 KiB body cap | Prometheus text format. Buckets include 30 s and 60 s for LLM request latency. |

`/metrics` is served on the main port outside the load-shed and concurrency
stack so that Prometheus scrapes work even during saturation.  Moving `/metrics`
to a separate loopback listener is deferred to C3.

## Commands

The workspace pins Rust `1.96.0` through the repository-root `rust-toolchain.toml`
(area-local toolchain files are forbidden by the root toolchain guard).

Run these after Rust is installed locally:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p intelligence-api
```

## Current Endpoints

- `GET /healthz`
- `GET /readyz`
- `GET /metrics` (bearer-authenticated in `shared-token` mode; see Observability)
- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /intelligence/v1/normalization/validate-proposal`
- `POST /intelligence/v1/normalization/generate-and-validate`
- `POST /intelligence/v1/normalization/generate-validate-submit`
- `POST /intelligence/v1/normalization/submit-proposal`

Platform-native routes mount under `/intelligence/v1/...` per root ADR-0001 §6.
The OpenAI-compatible surface (`/v1/models`, `/v1/chat/completions`) keeps its
ecosystem-mandated paths as a recorded exception to that convention.

`/v1/chat/completions` is the policy-enforced chat boundary. It accepts an
OpenAI-compatible non-streaming chat request, injects the `ko-KR` answer policy,
validates the model output, and makes one repair call when the first answer does
not pass the Korean output validator. Apps should use this endpoint instead of
calling Open WebUI directly.

Generation endpoints return `501` until a model proposal generator is
configured. Submission endpoints return `501` until a Foundation Platform submitter is
configured.

## Foundation Platform Submission

Configure these environment variables before starting the Rust API:

```dotenv
FOUNDATION_PLATFORM_BASE_URL=http://127.0.0.1:18080
FOUNDATION_PLATFORM_NORMALIZATION_PATH=/internal/normalization/proposals
FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE=/run/secrets/foundation-workload-token
```

The token file must contain a Zitadel workload bearer issued for the Intelligence
Platform runtime. The Rust API reads it during startup and sends only the bearer
authorization header. Static service tokens and non-workload credentials are
not accepted. When a Foundation Platform base URL is set, the token
file is required and startup fails fast if it is absent or unreadable.

The submit flow validates proposals first, skips invalid proposals, enqueues by
idempotency key, sends to Foundation Platform, and deduplicates already-sent records.

Every proposal POST carries an `Idempotency-Key` header whose value equals the
outbox idempotency key (`{tenant_id}:{target_kind}:{raw_record_id}:{schema_version}`).
Foundation Platform MAY use this header for server-side exactly-once intake dedup per
the IETF Idempotency-Key draft. Re-deliveries by the outbox drain worker reuse
the same key, so Foundation Platform can safely deduplicate retries without storing
proposal state on the intelligence-platform side.

## Model Runtime

Configure these environment variables to enable AI proposal generation:

```dotenv
INTELLIGENCE_API_BIND=0.0.0.0:8010
INTELLIGENCE_INBOUND_AUTH_MODE=shared-token
INTELLIGENCE_INBOUND_SERVICE_TOKEN=replace-with-a-strong-random-secret
INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID=service:intelligence-client
INTELLIGENCE_INBOUND_SERVICE_TENANT_ID=tenant:production
INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID=foundation-platform
INTELLIGENCE_INBOUND_SERVICE_ACTIONS=submit_normalization_proposal
MODEL_RUNTIME_BASE_URL=http://<model-runtime-host>:11434
MODEL_RUNTIME_CHAT_PATH=/v1/chat/completions
MODEL_RUNTIME_DEFAULT_MODEL=gemma2:9b
MODEL_RUNTIME_PROFILE_ID=normalization-ko
MODEL_RUNTIME_API_KEY=optional-token
# For reasoning-first models such as Qwen 3.6, set this so message.content is
# populated with the final answer instead of spending the response on reasoning.
MODEL_RUNTIME_REASONING_EFFORT=none
```

A local example profile is available at `config/local-ollama.env.example`.
Load it through the deployment environment or secret/config mechanism; it is
configuration data, not an executable production wrapper. Any
non-loopback bind requires the two `INTELLIGENCE_INBOUND_*` auth variables; see
the **Enterprise Runtime C0-C1** section for the full fail-closed guard rules.

The runtime uses an OpenAI-compatible chat completions shape. The base URL can
point to Ollama, vLLM, or another compatible model gateway, but apps still call
`intelligence-platform`, not the model runtime. `MODEL_GATEWAY_*` names are
still accepted as a deprecated compatibility alias, but new deployments should
use `MODEL_RUNTIME_*`.

`MODEL_RUNTIME_REASONING_EFFORT` is optional. Use `none` for reasoning-first
models when the application expects the final JSON or text in
`choices[].message.content`.

Example policy-enforced chat call after starting `cargo run -p intelligence-api`:

```bash
curl --fail-with-body \
  --request POST \
  --url http://127.0.0.1:8010/v1/chat/completions \
  --header "Authorization: Bearer ${INTELLIGENCE_INBOUND_SERVICE_TOKEN}" \
  --header 'Content-Type: application/json' \
  --data @- <<'JSON'
{
  "model": "gemma2:9b",
  "messages": [{"role": "user", "content": "짧게 자기소개해 주세요."}],
  "temperature": 0.2,
  "max_tokens": 256
}
JSON
```

Do not rely on hidden Korean aliases such as `gemma-ko` for production behavior.
Korean behavior belongs to the chat policy, validator, and repair flow exposed by
the intelligence platform.

### Temporary Open WebUI Connection

For the current local Open WebUI at `http://<model-runtime-host>:8080`, add an
OpenAI-compatible connection that points to the intelligence platform instead of
Ollama directly (`<intelligence-api-host>` is the LAN address of the machine
running `intelligence-api`):

```text
Base URL: http://<intelligence-api-host>:8010/v1
API Key: local-dev
Model: gemma2:9b
```

Use this only as a temporary bridge. The final product UI should call
`intelligence-platform` directly and Open WebUI should stay a development tool.

## LangChain And LangGraph

LangChain and LangGraph are not runtime dependencies of this Rust platform.
LangChain is useful for quickly assembling LLM apps and agents. LangGraph is
useful as a reference for durable, stateful, human-in-the-loop agent execution.
The Rust platform adopts those architecture ideas through explicit contracts,
outbox state, idempotency, and review boundaries.
