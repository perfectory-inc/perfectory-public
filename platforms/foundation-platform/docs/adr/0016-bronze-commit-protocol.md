# ADR 0016 — Bronze Commit Protocol (single-seam BronzeCommitter)

- **Status:** Accepted (2026-06-25)
- **Supersedes/extends:** [ADR 0015](./0015-bronze-object-key-content-addressed-layout.md)
  (key layout) — corrects 0015's "conditional PUT deferred (option A)" note: this ADR adopts (A).
- **Relates:** [ADR 0013](./0013-adopt-collection-event-fabric.md) (foundation-platform event fabric;
  Kafka layer). Cross-repo context: gongzzang ADR 0046 (Kafka/K8s preliminary), 0047 (event
  fabric), governed by gongzzang ADR 0045 (cross-repo ADR placement). **Those 0046/0047 are
  gongzzang's, NOT foundation-platform's** — foundation-platform's event-fabric decision is ADR 0013.
- **Contract SSOT:** this ADR plus [ADR 0015](./0015-bronze-object-key-content-addressed-layout.md);
  implementation and tests enforce the write protocol.

## Context

Bronze raw object writes are scattered across ~8 `put_object` call sites + per-lane plan/persist,
with duplicated `dedupe_key` (2×), `sha256_hex` (4×), and operation-collapse (2×). Verified
symptoms of this one root: operation= redundancy in some lanes not others; the V-World cadastral
key hashing a plain region code (`filter_sha256`) + constant `filter_kind=attr` + a request-knob
`size=`; an object key with no checksum/page-size axis → silent overwrite on re-collect; the async
data.go.kr lane writing R2 with **no** `bronze_object` DB row while sync lanes write one →
two-track verification + false-positive "orphans"; `put_object` shared by Bronze AND mutable
non-Bronze writers (manifest pointers, PBF artifacts) → a blanket write-once guard would break
legitimate overwrites. Each prior fix attempt was a per-lane patch that surfaced the next
exception — the hallmark of a missing commit boundary. R2 currently holds only smoke objects, so
this is the cheapest moment to fix the contract before national data lands (keys are immutable
once collected).

## Decision

Route **every Bronze raw write through one `BronzeCommitter` seam** that owns, in one place:
key compile + semantic path guard + canonical page-size validation + checksum + CreateOnly write +
DB `bronze_object` + ledger + event + manifest material. `ObjectStorage` stays a dumb low-level
port. Immutable artifacts use `CreateOnly`, stable serving pointers and disposable smoke/scratch
objects may use `OverwriteAllowed`, and Iceberg owns transactional Silver/Gold table commits.

Key sub-decisions:
- **Write-once via conditional PUT.** Bronze raw uses a per-request `write_mode = CreateOnly`
  → R2 `If-None-Match: *`. `412` is NOT a plain failure → reconcile by checksum. Non-streaming
  page PUT stores `x-amz-meta-sha256`; streaming bulk cannot (sha known only post-stream) →
  reconcile via DB/ledger/GET-rehash. `409` → retry.
- **async writes `bronze_object` (option a).** The committer ALWAYS records the DB row, so the
  async data.go.kr lane now has one too (reverses the Slice-3-B "no DB row" deferral) → write
  meaning + verification unify.
- **Recoverable commit protocol.** R2 + Postgres are NOT one transaction (order: R2 write → DB
  record). If R2 succeeds but DB fails, the retry hits `412`; the committer reconciles by checksum
  and **recovers the missing DB row** (same checksum) or **quarantines** (different) — never
  "412 = fail". Without this, R2-success/DB-fail leaves a permanent orphan.
- **DB checksum guard (secondary, sync).** Kept as an app-level invariant check (our own
  defensive code, NOT a sourced pattern).
- **Bucket Lock / WORM (separate 2nd net, deferred).** Applied with explicit prefix + retention
  only AFTER cleanup + mini-smoke + national re-collect + green verification — never in dev/smoke.

## Basis (honest — not borrowed authority)

1. **Our SSOT principle** (AGENTS.md #6) — the committer consolidates the verified duplications.
2. **Repository / Unit-of-Work pattern** (Fowler) — we already have `unit_of_work.rs`; the
   gold-pointer promote already centralizes write+promote. This is textbook design, not a company case.
3. **The only EXACT external 1:1** is the AWS S3 / Cloudflare R2 `If-None-Match` conditional write
   (verified verbatim: AWS conditional-writes doc; R2 S3-API PutObject ✅ If-None-Match; aws-sdk-s3
   `if_none_match("*")`). Hudi (mutable table format) and Gobblin (heavyweight framework) are NOT
   1:1 — referenced as philosophy only, never cited as evidence.

## Consequences

- **Solves at root** (the write-authority class): operation= redundancy, cadastral jank, page-size
  collision, silent overwrite, async/sync recording inconsistency, put_object scattering,
  dedupe_key/operation-collapse duplication, and makes the semantic guard meaningful (one path).
- **Does NOT solve** (different roots, kept separate): 5 GiB single-PUT / multipart (storage
  capability), Silver/observability slug drift, PowerShell/Bazel doc residue, runtime-config jank,
  the 2 Silver `sha256_hex` copies.
- **Kafka is not deprecated** — it stays the upper event-transport layer (foundation-platform ADR 0013;
  cross-repo gongzzang 0046/0047), broker deferred. The committer becomes its single future
  `raw_written` emit point, so building it first makes the eventual Kafka integration clean.

## Acceptance criteria (definition of done)

1. Zero direct `put_object` in Bronze collection modules. 2. Every Bronze lane via `BronzeCommitter`.
3. R2 success → DB fail → retry 412 → same checksum → DB row recovered. 4. 412 + different checksum →
quarantine/fail-loud. 5. `FileObjectStorage` rejects overwrite under `CreateOnly`. 6. Streaming bulk
never requires sha256 pre-upload. 7. Canonical page-size violation fails at plan time (value
evidence-based). 8. V-World cadastral emits no `filter_sha256`/`filter_kind`/`size`. 9. audit/reconcile
operate on the new key contract. 10. 5 GiB file-size preflight before national re-collect.

## Non-goals

No framework / plugin system / event-sourcing. No Kafka / K8s / Temporal in this work. No Silver/Gold
redesign. No `ArtifactWriter` rebuild — mutable writes keep the existing overwrite path.
