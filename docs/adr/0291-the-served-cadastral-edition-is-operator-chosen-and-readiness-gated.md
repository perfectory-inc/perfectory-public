# ADR 0291: The served cadastral edition is operator-chosen, readiness-gated, and rebuilt in lockstep

- Status: Accepted
- Date: 2026-10-21

## Context

`silver.parcel_boundaries` holds several VWorld cadastral editions side by side
(202606, 202609, 202610), and one of them — the `served_edition` — is the edition the
map, the parcel catalog projection, the PostGIS mirror, the parcel panel Gold, the 시군구
crosswalk and every parcel-derived by-PNU kind are built from (FP-ADR-0067, root
ADR-0148). Today that value is a literal in
`infra/lakehouse/contracts/vworld-parcel-source-objects.json` moved only by a reviewed PR,
and the steps that load a new edition to Silver and promote it are a manual runbook. Two
problems followed, both measured 2026-10-21:

1. **Freshness stalls silently.** The daily collection job keeps the newest edition in
   Bronze (202610 collected), but loading it to Silver and promoting it are manual and were
   not done. `served_edition` is still 202606 — the map and all parcel data are four months
   stale while two newer editions sit collected. Nobody could see this: the monitors check
   "a table exists and is consistent", not "a newer edition is ready and unpromoted".

2. **Consumers drift off the served edition.** Because the edition that a reader *should*
   use is a convention, not an enforced one, the 공매 kind producer built its parcel
   universe from every edition's current rows instead of the served one, attaching 632 sales
   to parcels editions 202609/202610 hold and the served 202606 map does not — orphans the
   by-PNU serving gate then refused (fixed for that reader, but the class was unguarded; the
   shared helper `address_pnu.lakehouse_inputs` has the same defect).

The operator must be able to choose which edition everything serves, must be able to see
whether a candidate edition's data is actually all present before choosing it, and must get
every parcel-derived output moved onto that edition together, never a mix.

## Decision

1. **The served edition is the operator's recorded choice.** `served_edition` stays the one
   SSOT value (still in `vworld-parcel-source-objects.json`), but it is set through an
   operator action surfaced in Dawneer, not only by hand-editing the contract. Changing it
   is an operation with evidence, not a silent literal edit.

2. **An edition is served only when it passes a readiness check.** `parcel_edition_readiness`
   (a new check, run on demand and on a schedule) reports, per declared edition, each stage:
   (a) source — every 시도 object present and measured (`measured_at_utc` set, provider not
   re-uploaded after extraction); (b) Silver — loaded into `silver.parcel_boundaries` with a
   row count within `max_row_loss_fraction` of the currently served edition; (c) rehearsal —
   the rehearsal Gold and rehearsal bake for that edition both passed. An edition is
   `ready` only when all three pass; otherwise the check names the first failing stage.
   Dawneer renders this per edition so "is 10월 all there?" is answered by a number, not a
   guess.

3. **Promotion rebuilds every parcel-derived output in lockstep.** Selecting a new served
   edition runs one orchestrated rebuild — parcel panel Gold, map tiles, catalog projection,
   PostGIS mirror, 시군구 crosswalk, and every parcel-derived by-PNU kind (including 공매) —
   onto the chosen edition, and serving flips only after all of them succeed. No output ever
   serves a different edition than another (no mixed-edition serving).

4. **The flip is refused unless readiness passes.** The promotion entry point calls
   `parcel_edition_readiness` first and refuses a not-`ready` edition, naming the failing
   stage. `scripts/guard/parcel-edition-promotion-is-readiness-gated-self-test.sh` plants an
   edition with a missing stage and confirms the refusal (prove-can-fail, root ADR-0001).

5. **Every served-universe read goes through the one served-edition reader.** A parcel
   reader that builds a *served parcel universe* reads it through
   `parcel_panel_silver_to_gold.read_served_parcels` (Spark) / `served_edition_view` (Rust),
   never from `parcel_boundaries` unfiltered. `scripts/guard/parcel-reads-the-served-edition.sh`
   (with its self-test) forbids an unfiltered served-universe read of `parcel_boundaries`
   outside those canonical readers, closing the 공매 / `address_pnu` bug class for good.

## Consequences

- Freshness becomes operator-controlled and observable: the operator sees every edition's
  readiness and picks the served one; the "collected but never promoted" gap is closed.
- This supersedes root ADR-0148's manual-PR promotion with a gated operator control; the
  readiness check is the safety the reviewed PR used to provide, so operator choice stays
  safe (a half-collected or un-rehearsed edition cannot be served).
- A flip costs a national re-bake of every parcel-derived output; it runs in the scheduled
  maintenance window like other heavy jobs and is bounded by the readiness gate.
- The edition-read bug class is enforced away, not just fixed per site.
- Follow-up (tracked, not all in one change): the readiness check and guards (this ADR §2,
  §4, §5), the Dawneer selector + readiness view (§1, §2), the lockstep promotion
  orchestration (§3), and the first real use — promoting 202610 once it reaches `ready`
  (re-collect the provider's re-uploaded objects, load Silver, clear the rehearsal blockers
  `silver.parcel_registry` absence and the maxzoom tile loss, rehearse, then flip).
