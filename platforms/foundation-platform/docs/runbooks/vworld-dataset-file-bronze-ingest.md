# VWorld dataset file Bronze ingest runbook

## Purpose

VWorld provider dataset files are collected as immutable Bronze objects. If a dataset has an
official file download path, do not fall back to WFS/OpenAPI collection for the same national raw
snapshot.

## Evidence boundary

Collection plans, provider inventories, object counts, byte totals, checksums, and live-write results are
generated under `target/audit/` and retained in the private operational evidence store. They are not
committed to the public source repository. Re-run the commands below in the target environment before
making a current-state or completion claim.

## Commands

Create the dataset collection plan:

```bash
cargo run -p foundation-outbox-publisher -- plan-vworld-dataset-collection
```

Build the provider file-level inventory:

```bash
cargo run -p foundation-outbox-publisher -- inventory-vworld-dataset-files
```

Run one-file dry-run smoke with automatic login:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="target/audit/vworld-dataset-file-ingest-auto-login-dry-run-evidence.json"
unset FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

Run one-file R2/DB live-write smoke:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE="1"
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

Run the full national file ingest only after smoke evidence is ready:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE="1"
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

Run all currently automatable provider files while deferring RAON/KUpload selection archives:

```bash
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES="1"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_DEFER_PROVIDER_ACQUISITION_BLOCKED="1"
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS
unset FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES
cargo run -p foundation-outbox-publisher -- ingest-vworld-dataset-files
```

`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES=1` removes
`SelectionArchive` inventory items from the selected download set. Those files require the provider
acquisition plane (RAON/KUpload agent or an official alternative) and must not be counted as
successfully collected by the normal dataset-file lane. The full-download confirmation gate still
applies to the remaining eligible files.

`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_DEFER_PROVIDER_ACQUISITION_BLOCKED=1` keeps the normal lane green
when a selected provider file still returns a RAON/KUpload acquisition requirement. Such files are
recorded in evidence with `status=provider_acquisition_blocked`, and the run status becomes
`ready_with_provider_acquisition_deferred`. Real file failures still block the run.

## Parallel Execution

`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS` and
`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES` decide how many items are selected.
`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT` decides how many selected files are downloaded
at the same time.

| Variable | Default | Meaning |
|---|---:|---|
| `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT` | `4` | Concurrent selected file downloads. `0` is rejected. |

The evidence JSON records `max_in_flight`. File reports are written back in inventory order, not
completion order, so audit diffs stay stable even when downloads finish out of order.

## Required Environment

For live writes:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Bronze metadata database |
| `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER` | `r2` or `local` |
| `R2_BUCKET_NAME`, `R2_ENDPOINT`, `R2_REGION`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | R2 object storage |

For VWorld file downloads:

| Variable | Purpose |
|---|---|
| `FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER` | Optional pre-authenticated provider Cookie header |
| `FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME` or `VWORLD_USERNAME` | Provider login username when Cookie header is not supplied |
| `FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD` or `VWORLD_PASSWORD` | Provider login password when Cookie header is not supplied |

The ingestor logs in once per run when a Cookie header is not supplied, then reuses the returned
session Cookie for every selected file. Credentials must not be printed in logs, evidence, or shell
output.

## Safety Gates

- Full national download is blocked unless
  `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1`.
- Live writes are disabled unless `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1`.
- The provider file inventory must be `status=ready` and file counts must match the collection
  plan.
- Download responses that are empty or HTML are rejected and are not stored as Bronze.
