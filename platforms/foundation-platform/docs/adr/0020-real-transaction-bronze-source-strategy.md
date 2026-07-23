# ADR 0020 - Real-transaction Bronze source strategy

- **Status:** Accepted
- **Date:** 2026-06-30
- **Relates:** [ADR 0016](./0016-bronze-commit-protocol.md),
  [ADR 0017](./0017-bronze-collection-protocol.md),
  [ADR 0019](./0019-bronze-readable-object-lake-postgres-catalog-ssot.md)
- **Evidence boundary:** live scopes, provider counts, object keys, checksums, and commit results follow
  [root ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).

## Context

Foundation Platform has two public channels for Korean real-transaction raw data:

1. `data.go.kr` RTMS Open API: paged records by operation, legal-dong code, deal month, and page.
2. `rt.molit.go.kr` condition-based CSV export: provider CSV by transaction type, contract date
   range, and geographic scope.

The Open API is useful for bounded probes and parity checks. A nationwide collection through pages,
however, multiplies calls by region, month, page, and transaction type. One CSV export can preserve
the same bounded provider slice as one immutable Bronze object.

## Decision

Use `rt.molit.go.kr` CSV export as the **primary Bronze acquisition channel** for real-transaction
raw collection.

Use the `data.go.kr` RTMS API only as:

- a bounded parity and schema-drift check channel;
- a smoke or investigation channel;
- a fallback when the CSV export is unavailable or a scoped comparison proves it incomplete.

Do not routinely acquire the same business facts through both channels. That would duplicate source
ownership, consume provider quota, and create competing raw histories. Both lanes remain
provider-owned sources, and Bronze stores the original response bytes rather than normalized rows.

Use two export planning modes:

- **historical or initial backfill:** explicit, coarse contract-date ranges chosen to minimize
  provider downloads;
- **refresh:** an explicit rolling contract-date window, producing one export job per configured
  real-transaction dataset.

The rolling export is not a provider delta feed. Silver and Gold own row-level change detection,
insert/update/delete interpretation, and current-state projection.

## Required parity evidence

Before enabling a dataset or changing its primary channel, run a bounded comparison over the same:

- transaction type;
- legal geographic scope;
- contract period;
- provider inclusion and cancellation semantics.

Compare logical record counts, stable provider identities where available, schema, and representative
field values. A mismatch blocks promotion until its cause is explained. A match for one scope supports
the channel decision but does not prove permanent equivalence; scheduled drift checks remain required.

The public repository records this procedure and invariant only. Selected scopes, counts, samples,
execution IDs, object identities, checksums, and R2/Postgres reconciliation belong to the private
operations evidence system.

## Object identity contract

Object identity must make the provider source and acquisition scope explicit without hard-coding a
live bucket or execution result in this ADR. Canonical templates are:

```text
bronze/source=<source-slug>/period=<yyyy-mm>/sido=<sido-code>/sigungu=<sigungu-code>/export.csv
bronze/source=<source-slug>/contract_from=<yyyy-mm-dd>/contract_to=<yyyy-mm-dd>/scope=<scope>/export.csv
```

The Bronze key compiler and catalog, not this document, are the executable SSOT for exact key
validation.

## Consequences

- National planning prefers a small number of explicit CSV export units over many provider API
  pages.
- Bronze keeps the provider CSV encoding, headers, and bytes unchanged.
- The API acquisition code remains available for parity, investigation, and fallback.
- Silver and Gold must tolerate repeated rolling windows and reconcile overlapping provider facts
  idempotently.
- Operators must keep parity evidence private, reviewable, and associated with the dataset version
  being promoted.

## Non-goals

- Declaring Silver or Gold normalization complete
- Approving a full historical nationwide collection; expansion remains operator-gated
- Removing the `data.go.kr` RTMS lane
