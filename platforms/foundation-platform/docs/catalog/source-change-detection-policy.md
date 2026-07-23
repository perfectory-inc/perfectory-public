# Source Change Detection Policy

Date: 2026-06-02
Owner: foundation-platform

## Decision

Provider payload fields are catalog data, not change-detection truth.

Foundation Platform must preserve every provider field required by the source contract, but it must not
decide whether a source page, feature, file, or logical record changed by trusting provider payload
fields such as creation date, update date, issue date, status date, or similar semantic fields.

Change detection is based on hashes over collected content:

- Raw byte checksum for providers whose byte response is stable for the same content.
- Canonical semantic checksum for providers whose raw response bytes can vary while the logical
  content remains the same.
- File checksum and provider manifest checksum for bulk files.

Provider date parameters may be used only as an operator-approved collection scope optimization.
They are not proof that unchanged data was not missed, and they are not the canonical change
detector. A full or hash-verified snapshot remains the correctness baseline.

## Required Separation

Every source integration must keep these concepts separate:

| Concept | Meaning | Example |
|---|---|---|
| Request fingerprint | Same provider request shape | provider, endpoint, parameters, page, scope |
| Content checksum | Same collected data | raw payload hash or canonical semantic hash |
| Provider metadata | Data supplied by provider | `crtnDay`, update date, status, count |
| Collection scope | What we choose to ask for | full snapshot, file, page window, date window |

Request fingerprint reuse may skip a provider call only when an already validated Bronze object
manifest proves the exact same request and snapshot has been collected.

Content checksum decides whether a newly collected payload is identical to previous content.

Provider metadata is stored and normalized as source data, but does not decide sameness.

### `provider_file_id` content-stability assumption (bulk-file pre-download skip)

Bulk-file ingest (hub.go.kr building-register bulk, V-World dataset files) applies the request
fingerprint reuse above as a *pre-download* optimization: if a Bronze object already exists for the
job's `source_partition_key` — which includes `provider_file_id` — it skips the download entirely
*before* fetching, so a multi-gigabyte file is not re-streamed every run.

This pre-download skip is correct **only because `provider_file_id` is content-stable** for these
providers: a provider assigns a *new* file id when it publishes *new* content (hub.go.kr assigns the
OPN id per published file; V-World dataset files use `{download_ds_id}-{file_no}`), so changed bytes
always arrive under a changed id and never collide with an already-collected one. The first
re-collect against an empty DB never hits this skip.

If a provider ever *reused* a file id with changed bytes, the pre-download skip would miss the
change. The escape is the opt-in flag **`FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1`**: when set, bulk-file
ingest bypasses the pre-download skip and re-downloads, which re-runs the post-download content
checksum — the "full or hash-verified snapshot is the correctness baseline" above. Default off
(unchanged behavior). Identical content writes the same content-addressed object key (idempotent
re-write); different content writes a new object and records the change.

## Storage Policy

Bronze storage should avoid duplicate R2 writes for identical content:

1. Compute checksum after fetching payload bytes.
2. If the content-addressed object already exists, do not write another full raw object.
3. Write or update the manifest/ledger pointer to the existing content object.
4. Preserve request lineage, collection run id, provider scope, and object checksum separately.

This means the system can prove both:

- what request was made;
- what exact content was returned.

## Speed Policy

Download execution should saturate provider-approved limits without exceeding them:

- Use a central provider lane scheduler.
- Run jobs concurrently only through that scheduler.
- Use AIMD token-bucket control per provider lane.
- Increase concurrency while success and latency stay healthy.
- Back off on HTTP throttling, provider quota codes, timeouts, and p95 latency thresholds.
- Defer jobs on throttling instead of dropping them.

Fixed sleeps are allowed only as a conservative override. They are not the primary speed-control
mechanism.

## R2 Cost Policy

R2 writes are billable Class A operations after the monthly free tier. Because every unnecessary
object write can become a paid operation, content-addressed Bronze reuse is a cost-control
requirement, not just a cleanup preference.

Delete operations are free, but storage, write/list operations, and read/head operations may be
billable depending on free-tier usage and storage class.
