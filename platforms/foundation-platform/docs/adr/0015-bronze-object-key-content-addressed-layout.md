# ADR 0015 - Bronze Object Key Layout (Superseded)

- Status: Superseded by [ADR 0016](./0016-bronze-commit-protocol.md) and
  [ADR 0019](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)
- Date: 2026-06-24
- Owner: foundation-platform

## Why This File Remains

This ADR is kept only to preserve historical links. The original body mixed three
ideas that are now intentionally separated:

1. R2 object keys as physical storage locations.
2. Content checksums for integrity and deduplication.
3. Postgres `bronze_object` rows as the catalog/control-plane source of truth.

That wording made it too easy to treat the object key itself as the truth. The
current contract rejects that model.

## Current Decision

The accepted Bronze contract is now:

- R2 `object_key` is a readable physical location label.
- Postgres `bronze_object` is the SSOT for source identity, snapshot date/period,
  snapshot basis, checksum, lineage, and provider file metadata.
- Bronze writes must pass through `BronzeCommitter`.
- Immutable raw writes use create-only storage plus recoverable commit semantics.
- Production code must not infer catalog truth by parsing `object_key` path
  tokens.

See:

- [ADR 0016 - Bronze Commit Protocol](./0016-bronze-commit-protocol.md)
- [ADR 0019 - Bronze Readable Object Lake + Postgres Catalog SSOT](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)

## Migration Note

Historical R2 migration/audit tools may still parse old `run_id=...` and
`partition=...` path shapes while cleaning pre-ADR-0019 data. That is legacy data
repair code, not the current write contract.
