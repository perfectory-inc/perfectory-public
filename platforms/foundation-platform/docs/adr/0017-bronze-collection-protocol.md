# ADR 0017 — Bronze Collection Protocol (single-seam SourceConnector)

- **Status:** Accepted (2026-06-26)
- **Relates:** [ADR 0016](./0016-bronze-commit-protocol.md) — the WRITE seam (`BronzeCommitter`).
  This ADR is the **FETCH/loop seam** that feeds it. Together they are the two halves of one
  ingestion pipeline: `SourceConnector.collect()` → per unit → `BronzeCommitter.commit()`.
- **Owner directive:** YAGNI explicitly waived for this — invest in the root architecture, not
  band-aids. Scope is still disciplined (see Non-goals): consolidate the EXISTING shapes, not a
  speculative plugin runtime.

## Context

`BronzeCommitter` (ADR 0016) unified Bronze **write** authority. The **collection** side is still
scattered: ~6 hand-written `*_ingest.rs` modules + 5 provider HTTP clients, and the collection
LOOP is verbatim-duplicated across lanes (VERIFIED this session):
- **Page lanes** (building_register, real_transaction, vworld cadastral/ned/land) all run the
  identical loop: `page_requests_for_batch(request, max_pages)` → `client.fetch_page(...)` →
  `committer.commit_*_page(...)` → `schema_profiles_for_plans(...)`. (e.g. real_transaction.rs:96/313/357
  ≡ vworld_cadastral_ingest.rs:120/393/437.)
- **Bulk lanes** (hub.go.kr bulk, vworld dataset file) run a second duplicated loop:
  skip-check (`find_bronze_object_by_source_partition_key`) → `open_file_stream(...)` →
  stream-commit → next job.

This is the FETCH-side parallel of the write-side scatter ADR 0016 fixed. Every new source
reimplements the loop → the same class of jank (the 5-lane schema-profile copy-paste, env-helper
drift, per-lane skip/rate-limit handling) keeps reappearing.

## Decision

Introduce a single **collection seam** with two shapes matching reality:
- **`PageCollector`** — owns the shared page loop once: build page requests → (skip-check) →
  `fetch_page` → hand each `RawFetchResult` to `BronzeCommitter` → schema-profile → next page.
- **`BulkCollector`** — owns the shared bulk loop once: select jobs → skip-check → `open_file_stream`
  → stream-commit via `BronzeCommitter` (streaming path) → next job.

Each is parameterized over a small per-source declaration (a trait) that supplies: the provider
client call, the request builder, and the per-source plan (which already exist). A lane's `run()`
becomes: *declare the source* → *call the shared collector*. The loop, skip-check, rate-limit
acquire/record, the commit handoff, and schema-profiling live in ONE place.

- **Provider HTTP clients stay per-provider** (a genuinely-new provider still needs a client with
  its own auth/parsing/error-envelope). What is unified is the LOOP around them, not the wire
  parsing. This is the honest boundary: the connector removes loop/skip/rate/commit duplication;
  it does NOT auto-parse a new API's response shape.
- **Composition with ADR 0016:** the collector calls `BronzeCommitter` for every unit, so every
  collected object gets CreateOnly + the recoverable commit protocol + the semantic guard for free.

## Basis (honest)

Our **SSOT** (AGENTS.md #6) + the **verified duplication** above + the **ADR 0016 precedent**
(same consolidation, fetch side). The connector/source-declaration model mirrors **Airbyte / Singer
/ Gobblin** at the philosophy level (a source declares its shape; the runtime owns the loop) — referenced,
NOT adopted 1:1 (those are heavyweight JVM frameworks; this is a small in-repo Rust seam).

## Consequences

- **Solves at root** (collection-authority class): the duplicated page/bulk loops collapse into 2
  shared collectors; new page/bulk sources become a thin declaration that inherits skip-check,
  rate-limit, retry, the commit + recovery, and schema-profiling; the 5-lane schema-profile
  copy-paste + per-lane loop drift go away.
- **Does NOT solve** (honest, bounded extension points): a genuinely-NEW API *shape* (cursor
  pagination, GraphQL, token stream, XML, nested/relational) still needs a NEW collector variant +
  a new client — but that is now ONE explicit place to add a shape, not a fork of every lane. And
  the per-provider wire parsing stays per-provider.

## Non-goals (scope fence — disciplined even with YAGNI waived)

Consolidate the TWO existing shapes (page, bulk) only. NO speculative shapes (cursor/GraphQL/etc.)
until a real source needs one. NO generic plugin/registry runtime, NO config-driven DSL, NO
Airbyte/Gobblin adoption. The connector is a small Rust seam + a per-source trait, exactly like the
committer — DRY consolidation of real duplication, not a framework.

## Plan (incremental, mirrors the committer rollout)

1. **Lock the committer write-seam first** (ADR 0016 finish): operation-collapse, semantic guard,
   `no-direct-put` guard — so the collector is built against an enforced write seam.
2. `PageCollector` seam + route ONE page lane through it (proof), then the other page lanes (trivial).
3. `BulkCollector` seam + route hub.go.kr bulk + vworld dataset file (streaming commit).
4. The async data.go.kr lane folds in as a `PageCollector` variant that also writes `bronze_object`
   (ADR 0016 option-a) + recovery.
5. Delete the now-dead per-lane loop bodies. Then page-size D-A, 5 GiB preflight, mini-smoke, re-collect.
