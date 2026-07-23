# ADR 0018 — V-World Collection Channel Strategy

- **Status:** Accepted
- **Date:** 2026-06-27
- **Relates:** [ADR 0016](./0016-bronze-commit-protocol.md),
  [ADR 0017](./0017-bronze-collection-protocol.md),
  [ADR 0014](./0014-bronze-source-slug-canonical-naming.md)
- **Per-dataset capability SSOT:** [`docs/catalog/vworld/`](../catalog/vworld/README.md)
- **Evidence boundary:** measured scopes, record counts, provider dates, file identities, and run
  results follow
  [root ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).

## Context

V-World may expose the same backing dataset through several channels:

| channel | returns | Foundation Platform use |
|---|---|---|
| bulk download | full and, for some datasets, change files in SHP/CSV form | national collection |
| attribute API | attribute JSON | serving, statistics, bounded comparison |
| spatial API | geometry and attributes | serving, bounded comparison |
| WMS | rendered map image | display only |

The architectural question is whether a second nationwide API collection lane adds fresher facts or
only duplicates a provider snapshot at much higher request volume.

## Decision

Use V-World bulk files as the **primary national Bronze acquisition channel**. Attribute and spatial
APIs are serving, statistics, investigation, and drift-check lanes, not a duplicate nationwide raw
history. WMS is display-only and is never a Bronze source.

Freshness is maintained as follows:

- When the provider exposes a verified native change artifact, ingest an initial full file and then
  the change files.
- Otherwise poll the provider update marker and acquire a new full file when it advances.
- Where there is no trustworthy native delta, compare retained snapshots by the dataset's canonical
  key and content hash to derive changes downstream.

Bronze remains immutable and CreateOnly under ADR 0016. Each dataset has one acquisition watermark.
The per-dataset catalog records which capabilities and cadence are actually supported; pipeline code
must not infer delta support from a UI label alone.

## Decision basis and revalidation

Bounded, like-for-like comparisons did not establish that the API contained fresher records than the
corresponding bulk artifacts. Because a national API pull multiplies requests by region and page,
that evidence did not justify maintaining two national raw histories.

This is not a permanent claim that an API can never be fresher. Revalidate the decision when any of
the following changes:

- provider documentation or delivery behavior;
- the dataset's update marker or native-delta capability;
- a drift check finds facts absent from the corresponding bulk slice;
- bulk availability or legal/operational terms.

A revalidation compares the same dataset, geographic scope, provider period, inclusion rules, and
stable record identities. The public repository keeps the method and promotion gate. Actual scopes,
record counts, dates, request IDs, provider samples, and results stay in private operations evidence.

## Consequences

- New V-World national collection lanes target bulk download by default.
- A per-dataset exception is allowed only when the capability catalog documents why no usable bulk
  system of record exists and records the approved alternative.
- API quota is reserved for serving, bounded checks, and explicit fallback rather than duplicate
  national snapshots.
- Full-file refreshes require downstream idempotent snapshot comparison when the provider supplies no
  trustworthy change feed.
- The capability catalog is the SSOT for what each dataset offers; this ADR is the SSOT for why bulk
  is the collection default.

## Non-goals

- Silver or Gold merge implementation; Bronze continues to preserve provider files
- Approval of a national recollection; scope expansion remains owner-gated
- Assuming that a provider UI control proves the existence of a native delta feed
- Removing API adapters used for serving, drift checks, investigation, or approved fallback
