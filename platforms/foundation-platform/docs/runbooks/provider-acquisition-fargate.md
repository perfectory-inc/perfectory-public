# Provider Acquisition Headless Replay Runbook

Status: runtime-neutral reference; Fargate is not selected by this document
Owner: foundation-platform

This file keeps the historical path name, but the acquisition contract is runtime-neutral. It
defines the security and ownership boundary for browser-assisted provider acquisition; it is not
an execution log and does not record account provisioning or live-run evidence. Per
[ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md), execution IDs,
provider file identities, object keys, checksums, byte counts, and dated results belong in the
private operations evidence system.

## Purpose

Use this runbook when a provider file cannot be collected by normal Rust HTTP and must be acquired
through a provider-controlled browser page, such as V-World RAON/KUpload. The selected adapter and
runtime must preserve the same Foundation Platform commit boundary.

## Command Chain

```text
browser acquisition adapter
  -> provider-controlled download page
  -> provider-approved raw file acquisition
  -> private task-local artifact or replay request
  -> foundation-outbox-publisher import-provider-acquisition-landing
  -> Rust local staging
  -> Rust validation
  -> BronzeCommitter commit
  -> R2 Bronze CreateOnly + Postgres bronze_object
```

Python/browser code is an acquisition adapter only. Rust owns validation, checksum, storage,
lineage, and final commit. Diagnostic R2 landing is optional and must be used only when a bounded
investigation requires it. The default operational mode sets
`FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE=1` so the same large file is not written
twice.

## Required Runtime Capabilities

The chosen runtime must provide:

- Python 3.11+ and the provider-acquisition worker dependencies when using its browser adapter
- browser and provider-agent dependencies required by the selected adapter
- the Rust `foundation-outbox-publisher` binary for Bronze commits
- writable ephemeral storage for private replay files and staged response bytes
- runtime-only secret injection
- outbound network access to the provider and R2 endpoints

Do not depend on a Windows desktop agent in the production acquisition path. A provider agent that
is required for exact bulk files must first be proven in the checked-in Linux container contract.
Provider package binaries and credentials are supplied at build or task runtime and are never
committed to the repository.

## Required Environment

Provider acquisition:

| variable | purpose |
|---|---|
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_STAGING_DIR` | task-local staging directory for private bytes |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_IMPORT_OUTPUT_PATH` | redacted import report path |
| provider login/cookie variables | authenticated browser session, only when required |

Bronze commit:

| variable | purpose |
|---|---|
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE` | set to `1` to commit staged bytes to Bronze |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE` | set to `1` for the default no-landing path |
| `DATABASE_URL` | Bronze catalog database |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_SLUG` | canonical source slug |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_ID` | provider identity used by the Bronze compiler |
| `FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_NAME` | provider file label |

R2 Bronze write:

| variable | purpose |
|---|---|
| `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER` | set to `r2` |
| `R2_BUCKET_NAME` | environment-specific Bronze bucket binding |
| `R2_ACCOUNT_ID` | Cloudflare account ID |
| `R2_ENDPOINT` | R2 S3 endpoint; optional when derived from the account ID |
| `R2_ACCESS_KEY_ID` | runtime access key |
| `R2_SECRET_ACCESS_KEY` | runtime secret key |

The task must fail before provider download when the live-write preflight fails. Active account,
bucket, and secret bindings are private deployment state rather than public runbook facts.

## Step 1 - Capture a Bounded Replay Request

```bash
python -m foundation_platform_provider_acquisition.raon \
  --download-ds-id "$DOWNLOAD_DS_ID" \
  --file-no "$FILE_NO" \
  --output "$PUBLIC_PROOF_PATH" \
  --prove-raon-replay \
  --private-replay-request-output "$PRIVATE_REPLAY_REQUEST_PATH" \
  --landing-object-key "$LANDING_OBJECT_KEY"
```

Set `DOWNLOAD_DS_ID` and `FILE_NO` from an approved private selection. The public proof must redact
provider secrets. The private replay request is runtime-only.

## Step 2 - Validate and Commit Through Rust

```bash
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH="$PRIVATE_REPLAY_REQUEST_PATH" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_IMPORT_OUTPUT_PATH="$IMPORT_REPORT_PATH" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE=1 \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE=1 \
foundation-outbox-publisher import-provider-acquisition-landing
```

When Bronze commit is enabled, the importer must fail before replaying the provider request if
`DATABASE_URL` or any required Bronze identity is missing.

## Batch Mode

Use the committed batch runner for repeatable blocked-file collection:

```bash
python -m foundation_platform_provider_acquisition.raon_batch \
  --selection "$PROVIDER_ACQUISITION_SELECTION_JSON" \
  --batch-id "$BATCH_ID" \
  --output-root "$PROVIDER_ACQUISITION_OUTPUT_ROOT" \
  --source-slug "$SOURCE_SLUG" \
  --rust-binary foundation-outbox-publisher
```

The batch runner is an orchestrator only:

- it reads an operator-approved provider selection
- it asks the browser adapter for a private replay request
- it invokes `foundation-outbox-publisher import-provider-acquisition-landing`
- it enables the direct-to-Bronze path by default
- it deletes private replay and staging files after each job
- it emits a redacted per-file outcome while detailed evidence stays private

For parallel execution, split work with `--shard-index` and `--shard-count` or explicit
`--provider-file-id` filters. Every shard must use the same Rust importer and `BronzeCommitter`.
Browser code must never become an independent storage writer.

For runtimes that pass selections through environment variables, provide base64-encoded UTF-8 JSON
through `PROVIDER_ACQUISITION_SELECTION_JSON_BASE64`. A mounted file through
`PROVIDER_ACQUISITION_SELECTION_JSON` is also supported.
`PROVIDER_ACQUISITION_SELECTION_JSON_INLINE` is limited to controlled local debugging where the
caller owns shell quoting. The entrypoint materializes the selection only in task-local ephemeral
storage.

The runtime must start the required Linux provider agent and browser support before the batch. The
checked-in container definitions have distinct responsibilities:

- `services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof` captures one
  bounded replay request and intentionally excludes the Rust importer and R2/Postgres write path.
- `services/foundation-provider-acquisition-worker/Dockerfile.raon-batch` includes the Linux agent,
  provider-acquisition worker, and compiled `foundation-outbox-publisher`. Secrets are injected at
  task runtime, not copied into the image.

Build either image only with an explicitly supplied provider package and checksum. For example:

```bash
docker build \
  -f services/foundation-provider-acquisition-worker/Dockerfile.raon-batch \
  --build-arg RAON_DEB_URL="$RAON_DEB_URL" \
  --build-arg RAON_DEB_SHA256="$RAON_DEB_SHA256" \
  -t foundation-platform/raon-batch:local \
  .
```

## Step 3 - Cleanup

Remove private runtime files before the task exits:

```bash
rm -f "$PRIVATE_REPLAY_REQUEST_PATH"
rm -f "$PROVIDER_BROWSER_LOG_PATH"
```

If cleanup fails, emit a warning and treat task storage as sensitive until the task exits.

## Replay Identity Shape

```text
landing/provider=<provider>/acquisition=<adapter>/job_id=<job-id>/download_ds_id=<dataset-id>/file_no=<file-number>/download.zip
```

The adapter may carry this landing-shaped identity in a private replay request for compatibility
and traceability. Direct-to-Bronze mode does not write it to R2. Browser code never writes
`bronze/`.

## Safety Gates

Apply these gates in order for each adapter or material runtime change:

1. Capture one operator-approved file as a private replay request.
2. Validate the response body with the Rust importer; prefix-only inspection is insufficient.
3. If a diagnostic landing is necessary, write one bounded object and reconcile it before retry.
4. Bind provider identity to canonical Foundation Platform source metadata.
5. Commit one bounded file through `BronzeCommitter`.
6. Run a small approved batch and reconcile catalog and object-store outcomes.
7. Expand scope only with explicit owner approval and private, reviewable execution evidence.

These invariants always apply:

- Public proof must not contain cookies, agent tokens, signed URLs, or request bodies.
- Rust validation must reject empty bodies, HTML bodies, invalid archive bytes, and archives that
  contain provider HTML/error pages.
- R2 writes must use CreateOnly.
- Bronze commit must go through `BronzeCommitter`.
- Commit evidence must include the object identity, size, and checksum in the private evidence
  system; those values must not be copied into this public runbook.

## Failure Handling

| failure | handling |
|---|---|
| browser adapter cannot start | mark job blocked; no R2 write |
| provider page yields no replay request | mark job blocked; keep only a redacted public proof |
| replay proof shows only an archive prefix | continue only to Rust validation |
| replay request returns non-2xx | fail job; retry only when provider policy allows |
| replay body is HTML, empty, or an invalid archive | reject; no Bronze write |
| archive contains provider HTML | reject; no Bronze write |
| replay identity already exists in diagnostic landing mode | reconcile before retry |
| Bronze database is unavailable | fail before provider replay when commit is enabled |
| staging disk is full | fail task; reduce batch size or increase ephemeral storage |
| private cleanup fails | warn; task storage remains sensitive until task exits |

## Runtime Selection

This runbook does not choose Lambda, Fargate, ECS, ai-server, or another runtime. The selected
runtime must preserve the exact ownership chain above. Runtime choice is a deployment decision made
after bounded container proof, cost review, and operator approval; it is not inferred from a prior
local run.

Fargate remains the clean managed candidate, but it is not selected by this runbook. ai-server is a
lab, not the production collector.

Fargate is suitable for repeatable cloud batches only when the selected adapter and Linux provider
agent run inside the pinned container. The data model does not depend on that choice: a different
execution plane can be introduced without discarding committed R2 Bronze objects or Postgres
catalog rows.

## Linux RAON Agent Container Proof

Use the proof image before selecting a cloud runtime for exact provider bulk files. It starts the
Linux provider agent under Xvfb, opens the provider page through the browser adapter, and writes
only a redacted public proof plus a private task-local replay request. It must not write R2, open
`DATABASE_URL`, or call `foundation-outbox-publisher`.

Build with a runtime-supplied package URL and checksum. Do not commit the package:

```bash
docker build \
  -f services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof \
  --build-arg RAON_DEB_URL="<provider-linux-package-url>" \
  --build-arg RAON_DEB_SHA256="<sha256>" \
  -t foundation-platform/raon-agent-proof:local \
  .
```

Run one approved file without embedding its identity in the command history:

```bash
docker run --rm \
  --env-file "$PROVIDER_ACQUISITION_RUNTIME_ENV" \
  -v "$PWD/target/provider-acquisition-proof:/work/staging" \
  foundation-platform/raon-agent-proof:local
```

Pass criteria:

- public output exists and contains only redacted protocol evidence
- the private replay request exists only in task-local storage
- public output contains no provider token, cookie, signed URL, or replay body
- setup executables and HTML-wrapper archives are not classified as provider data
- the private replay response passes Rust validation before any Bronze write

Store the resulting execution ID, selected provider identity, object key, size, checksum, and
reconciliation result in the private operations evidence system defined by ADR 0007. A previous
result is not evidence that a new provider file, image, runtime, or account binding is valid.
