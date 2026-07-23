# ADR 0019 - Bronze readable object lake + Postgres catalog SSOT

Status: Accepted
Date: 2026-06-28
Owner: foundation-platform
Supersedes/refines: [ADR 0015](./0015-bronze-object-key-content-addressed-layout.md) (object-key layout)
Related: [ADR 0016](./0016-bronze-commit-protocol.md), [ADR 0017](./0017-bronze-collection-protocol.md),
[ADR 0018](./0018-vworld-collection-channel-strategy.md),
[source-change-detection-policy](../catalog/source-change-detection-policy.md)

## Decision

The R2 object path is a human-readable physical layout for operations. The single source of truth
for identity, integrity, dates, and lineage is the Postgres Bronze Catalog.

## Context

Bronze raw objects need two different kinds of identity:

- **Coverage identity**: which source/request/file piece this object represents. This drives skip,
  coverage, and re-collection. Example: a real-transaction request for `lawd=11680` and
  `deal_ymd=202605`, or a hub bulk file with `provider_file_id=OPN...`.
- **Content identity**: whether the bytes are identical. This is the SHA-256 checksum.

Dates also have two different meanings:

- **Request scope**: a date/month passed to the source to select data. Example:
  `DEAL_YMD=202605`. This belongs in the coverage identity.
- **Descriptive metadata**: a period, 기준일, 갱신일, or fallback collection date stamped on a file
  or inferred from provider inventory. This belongs in typed catalog metadata.

We considered three layouts:

1. **Date-free path**: mechanically normalized, but poor for request-scope dates and less readable.
2. **Content-addressed blob path** (`bronze/blob/sha256/...`): pure, but opaque and expensive for
   streaming bulk files because the final key is unknown until the full digest is known.
3. **Readable path + Postgres catalog truth**: operationally readable while keeping correctness in
   the catalog. This ADR adopts this option.

## Adopted Model

### R2 is physical layout, not truth

R2 keys remain readable:

```text
bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip
bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip
bronze/source=datagokr__real_transaction_industrial_trade/period=2026-05/lawd=11680/page-000001.json
bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json
bronze/source=vworldkr__land_register/pnu=9999900601100010000/page-000001.json
```

The path is useful for humans, R2 browsing, smoke verification, and incident triage. Code must not
parse the path to decide skip, dedupe, coverage, freshness, or lineage.

Request-scope partitions such as `period`, `lawd`, `sigungu`, `bjdong`, and `pnu` stay in
API-page keys when they distinguish one requested coverage slice from another. A bulk file's
provider period, snapshot date, updated date, and fallback collection date are descriptive
metadata, so they do not participate in its physical identity; its provider file id is the leaf.

### Postgres Bronze Catalog is truth

Every recorded Bronze object carries:

```text
source_slug
source_identity_key
object_key
checksum_sha256
snapshot_period
snapshot_date
snapshot_granularity
snapshot_basis
provider_file_id
provider_file_name
provider_updated_at
request_params
ingestion_run_id
collected_at
```

`source_identity_key` answers "which source/request/file piece is this?"
`checksum_sha256` answers "are these bytes identical?"
They are related only through the catalog, not through path conventions.

### Date policy

- `snapshot_period` is the human bucket, such as `2026-05`.
- `snapshot_date` is the canonical as-of date. Month-granularity data uses the first day of the
  month with `snapshot_granularity=month`.
- `snapshot_granularity` is `day` or `month`.
- `snapshot_basis` records why the date exists:
  - `provider_snapshot_date`
  - `provider_file_period`
  - `request_month`
  - `provider_updated_at`
  - `collected_at_fallback`

`snapshot_date` is always populated for new Bronze objects. If the provider has no 기준일, use
갱신일; if it has neither, use `collected_at_fallback`. The basis must make the fallback explicit.

### Identity policy

The source identity is source-specific and generated in one place:

```text
hub/vworld bulk       = provider_file_id
real-transaction API  = lawd + deal_ymd + page + page_size
building-register API = sigungu + bjdong + page + page_size
V-World PNU API       = pnu + page + page_size
V-World cadastral API = pnu/emd/fingerprint + page + page_size
```

Provider request parameters are still preserved in `request_params` as raw lineage. That is not
duplication; it answers a different audit question.

### Dedupe policy

`dedupe_key` is derived from the catalog identity and checksum:

```text
dedupe_key = source_slug + ":" + source_identity_key + ":sha256=" + checksum_sha256
```

No lane may hand-format a dedupe key differently.

## Consequences

- Operators can still read R2 paths directly.
- Filename or provider ID changes do not silently become truth. The catalog and checksum decide.
- Same bytes are recognized by checksum; changed bytes are recorded as a new content state.
- Silver/Gold remain deferred to an Iceberg-style lakehouse. Bronze stays raw files + catalog.
- API/event contract versions remain explicit, while semantic data versions stay out of R2 object
  keys and live in Catalog/Iceberg metadata.

## Non-goals

- Content-addressed blob storage for Bronze (`bronze/blob/sha256/...`) is not adopted.
- Iceberg/Delta/Hudi are not introduced for Bronze.
- R2 path migration alone is not a correctness change; correctness comes from the catalog contract.
