# Geography Registry — Wave 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop deriving parcel/building identity from a mutable government code. Ingest the authoritative legal-dong code registry, resolve every source's `(code, vintage)` to the current canonical 법정동 code at one ingest chokepoint, and prove it by resolving 광주+전남 (code 12) back to 29/46 so their buildings attach to the map — then publish nationwide.

**Architecture:** Pillars ①②③ of [ADR-0103](../../adr/0103-place-identity-outlives-administrative-code-changes.md). A lakehouse `reference.legal_dong_code` table (from code.go.kr / data.go.kr, with 생성일/말소일) is the temporal dictionary. A resolver in `foundation-shared-kernel` normalizes any source 시군구 code to the current canonical code before `standard_pnu_from_hub_register_codes` composes a PNU. HUB building silver is re-derived through the fixed chokepoint, then the price/gold/bake/publish chain re-runs. Pillars ④⑤ (steward queue, drift guard) and ⑥ (bitemporal) are Waves 2–3; Wave 1 ships a **minimal drift check** as a seed.

**Tech Stack:** Rust (foundation-shared-kernel, foundation-outbox-publisher hub silver export), Python/Spark (lakehouse jobs), Iceberg on R2, the existing `national-bake.sh` runbook.

**Scope note (honest):** Wave 1 delivers *cross-source consistency at the current point in time* (all sources resolve to the same canonical 법정동 code, so 광주 attaches). The fully opaque, recode-surviving stable ID and "as-of" history need the time dimension and land in Wave 3 (bitemporal). Wave 1 does NOT re-identify all 40M parcels with a new surrogate key; it canonicalizes the code so PNUs already agree.

---

## Task 0: Design decision — where the resolver and crosswalk live

**Files:**
- Decide + record in: `docs/adr/` (a short follow-up note on ADR-0103 if the storage choice is non-obvious) — no new ADR unless the decision diverges from ADR-0103.

- [ ] **Step 1: Choose the registry storage.** Decision: a lakehouse Iceberg table `reference.legal_dong_code` (append-only, bitemporal-ready columns) as the SSOT dictionary, plus a small checked-in contract `infra/lakehouse/contracts/legal-dong-code-source-objects.json` describing the code.go.kr source layout. Rationale: matches how every other dataset is modeled (contract + lakehouse table); the resolver reads a materialized crosswalk derived from it.
- [ ] **Step 2: Choose the resolver's runtime form.** Decision: a pure function + a data table. The *crosswalk* (source 시군구 code → canonical 시군구 code, with valid-from/valid-to) is data; the *resolver* is a `foundation-shared-kernel` function that looks up the crosswalk. The Rust HUB silver export and any Spark job both read the same materialized crosswalk table `reference.sigungu_canonical_crosswalk`.
- [ ] **Step 3: Record the Wave-1 seed source of the crosswalk.** The 광주+전남 12→29/46 entries are seeded from the data-derived mapping already verified this session (27 시군구, pnu-tail join, monotonic + 광양 non-arithmetic case), pending replacement by the authority-derived mapping in Task 1. Write the 27 entries to `infra/lakehouse/contracts/sigungu-canonical-crosswalk.seed.json` with provenance `derived:pnu-tail-join:2026-09`.

---

## Task 1: Ingest the legal-dong code registry (the dictionary)

**Files:**
- Create: `platforms/foundation-platform/infra/lakehouse/contracts/legal-dong-code-source-objects.json`
- Create: `platforms/foundation-platform/infra/lakehouse/spark/jobs/legal_dong_code_registry.py`
- Create: `platforms/foundation-platform/infra/lakehouse/spark/tests/test_legal_dong_code_registry.py`
- Modify: `platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json` (add `reference.legal_dong_code` table contract)

- [ ] **Step 1: Add the contract for `reference.legal_dong_code`.** Columns: `code` (string, 10-digit 법정동코드), `sido_name`, `sigungu_name`, `eupmyeondong_name`, `ri_name` (all string), `created_date` (string YYYYMMDD, 생성일자), `abolished_date` (string YYYYMMDD or empty, 말소일자), `is_current` (bool, derived: abolished_date empty), `source_snapshot_id`, `source_record_id`. Partition by `substring(code,1,2)`. Quality gates: `append_only`, `code_not_null`, `code_10_digits`.
- [ ] **Step 2: Write the failing test** for the parse+normalize kernel that turns one 법정동코드 전체자료 row (pipe- or tab-delimited from code.go.kr / the data.go.kr file) into a contract row, including the `is_current = abolished_date is empty` rule.

```python
# test_legal_dong_code_registry.py
from legal_dong_code_registry import parse_registry_row
def test_current_and_abolished_rows():
    cur = parse_registry_row(["2911010100", "광주광역시 동구 계림동", "20260701", ""])
    assert cur["code"] == "2911010100" and cur["is_current"] is True and cur["abolished_date"] == ""
    old = parse_registry_row(["1211010100", "전라남도 광주시 계림동", "19800101", "19861101"])
    assert old["is_current"] is False and old["abolished_date"] == "19861101"
```

- [ ] **Step 3: Run it, confirm it fails** (`parse_registry_row` undefined).
- [ ] **Step 4: Implement `parse_registry_row` + the Spark loader** that reads the downloaded 전체자료, applies `parse_registry_row`, and appends to `reference.legal_dong_code` via `append_batch_once` (idempotent; ADR-0069). Names are split into sido/sigungu/eupmyeondong/ri where the source provides them; where it provides a single name string, keep it in `sigungu_name` and leave finer columns empty (do not fabricate).
- [ ] **Step 5: Run the test, confirm pass.**
- [ ] **Step 6: Operationally load** the registry once from the code.go.kr 전체다운로드 / data.go.kr file (place under `bronze/source=mois__legal_dong_code/`). Verify row count > 45,000 and that both `29xxx` (current 광주) and `12xxx` (abolished/merged) codes appear with correct `is_current`.
- [ ] **Step 7: Commit.**

---

## Task 2: Derive the canonical 시군구 crosswalk from the registry

**Files:**
- Create: `platforms/foundation-platform/infra/lakehouse/spark/jobs/sigungu_canonical_crosswalk.py`
- Create: `platforms/foundation-platform/infra/lakehouse/spark/tests/test_sigungu_canonical_crosswalk.py`
- Uses seed: `infra/lakehouse/contracts/sigungu-canonical-crosswalk.seed.json` (from Task 0)

- [ ] **Step 1: Write the failing test** for the crosswalk derivation rule: an abolished 시군구 code maps to the current code that (a) shares 읍면동 code sets / name and (b) is current as of the target date; if the registry alone is ambiguous, fall back to the checked-in seed; if still unresolved, emit the code to an `unresolved` set (NOT a silent drop).

```python
from sigungu_canonical_crosswalk import resolve_sigungu
def test_merged_code_resolves_to_current():
    # 12240 (구 광주 서구, abolished) -> 29140 (current 광주 서구)
    assert resolve_sigungu("12240", as_of="20260801").canonical == "29140"
def test_unknown_code_is_flagged_not_dropped():
    r = resolve_sigungu("77999", as_of="20260801")
    assert r.canonical is None and r.unresolved is True
```

- [ ] **Step 2: Run it, confirm it fails.**
- [ ] **Step 3: Implement `resolve_sigungu`** and a Spark job that materializes `reference.sigungu_canonical_crosswalk` (`source_code`, `canonical_code`, `valid_from`, `valid_to`, `provenance`) from `reference.legal_dong_code` + the seed. For Wave 1, cover the 27 광주+전남 entries and identity mappings for all current codes.
- [ ] **Step 4: Run test, confirm pass. Materialize the crosswalk table.**
- [ ] **Step 5: Verify** all 27 legacy 12xxx codes resolve to distinct 29xxx/46xxx codes, matching the session-derived mapping (12210→29110 … 12330→29200; 12110→46110 … 12870→46910; 12190→46230 광양 the non-arithmetic case). Log any divergence.
- [ ] **Step 6: Commit.**

---

## Task 3: Wire the resolver into the PNU chokepoint (Rust)

**Files:**
- Modify: `platforms/foundation-platform/crates/foundation-shared-kernel/src/pnu.rs`
- Create: `platforms/foundation-platform/crates/foundation-shared-kernel/src/sigungu_resolver.rs` (loads the materialized crosswalk; in tests, an in-memory map)
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/hub_register_silver_export.rs` (pass the resolver into `compose_pnu`)

- [ ] **Step 1: Write the failing test** in `pnu.rs` proving a merged code is normalized before composition:

```rust
#[test]
fn resolves_merged_sigungu_before_composing_pnu() {
    let x = crosswalk_from([("12240", "29140")]); // 구 광주 서구 -> 현행
    // 12240 + 읍면동 01100 + 대지 0 + 본 0734 + 부 0000
    assert_eq!(
        standard_pnu_from_hub_register_codes_resolved(&x, "12240", "01100", "0", "0734", "0000").as_deref(),
        Some("2914001100107340000")   // 시군구가 29140 으로 정규화되어 조립됨
    );
}
#[test]
fn unresolved_sigungu_yields_none_not_a_fabricated_pnu() {
    let x = crosswalk_from([]); // 빈 사전
    assert_eq!(
        standard_pnu_from_hub_register_codes_resolved(&x, "77999", "01100", "0", "0734", "0000"),
        None
    );
}
```

- [ ] **Step 2: Run it, confirm it fails** (function undefined).
- [ ] **Step 3: Implement `standard_pnu_from_hub_register_codes_resolved`** that first resolves `sigungu` through the crosswalk (unknown → `None`, never a fabricated PNU), then delegates to the existing composition. Keep the old `standard_pnu_from_hub_register_codes` as a thin caller of the resolved form with an identity crosswalk, so existing callers compile; migrate `hub_register_silver_export::compose_pnu` to pass the real crosswalk.
- [ ] **Step 4: Run tests, confirm pass.** Run the whole shared-kernel + publisher test suites.
- [ ] **Step 5: Commit.**

---

## Task 4: Re-derive 광주+전남 HUB silver through the fixed chokepoint

**Files:**
- Operational (server runbook): `national-bake.sh` (reuse `building-silver-export` / `building-silver-load` steps built from the fixed source)

- [ ] **Step 1:** Rebuild the publisher/silver-export from this branch (fixed `pnu.rs` + resolver) into the server's `fix-build-src` (the spark/publisher mounts).
- [ ] **Step 2:** Re-derive the HUB silver tables for the affected regions — `building_register_titles`, `building_register_floors`, `building_register_units`, `building_register_unit_areas`, `building_register_exclusive_unit`, `building_register_apartment_price` — so their `pnu`/`sigungu_cd` carry 29/46 instead of 12. (Re-derive from bronze is the append-only-respecting path; silver is a derived projection — ADR-0069.)
- [ ] **Step 3: Verify** `silver.building_register_exclusive_unit` now shows `substr(pnu,1,2)` in {29,46} and **no 12** for these regions; counts ≈ pre-merger 광주 563,834 + 전남 507,307.

---

## Task 5: Reload 광주(29) + 전남(46) prices

**Files:**
- Operational: `national-bake.sh price-reload 29` and `price-reload 46` (the resilient step from ADR-0102)

- [ ] **Step 1:** Run `price-reload 29` and `price-reload 46` (now the sources carry 29/46). Confirm `appended:true`, rows > 0, and uniqueness on `(mgm_bldrgst_pk, base_date)`.
- [ ] **Step 2: Verify** `silver.unit_official_price` sido distribution now includes 29 and 46 and totals rise by the 광주+전남 volume.

---

## Task 6: Building gold → bake → publish (nationwide, 광주 included)

- [ ] **Step 1:** Run `building-gold` (assert unique on `(mgm_bldrgst_pk, base_date)` — already fixed).
- [ ] **Step 2:** Bake building/floor/unit R2 objects; publish the building manifest (FIRST_PUBLICATION) to buildings gateway.
- [ ] **Step 3: Verify** a 광주 parcel (pnu `29…`) resolves a building/unit/price object on the edge and matches Postgres field-for-field (the ADR-0100 comparison), i.e. 광주 buildings now attach to the map.

---

## Task 7: Minimal drift check (Wave-1 seed of pillar ⑤)

**Files:**
- Create: `platforms/foundation-platform/infra/lakehouse/spark/tests/test_no_unregistered_sigungu.py` (or a guard invoked in the bake runbook)

- [ ] **Step 1: Write the check**: every distinct `substr(pnu,1,5)` in the HUB silver must exist as a current code in `reference.legal_dong_code`. A code not in the registry → fail with the offending codes listed.
- [ ] **Step 2: Prove it can fail**: seed a row with `sigungu=12xxx` (unregistered-as-current) and confirm the check rejects it (ADR-0001: a check only ever seen passing is untrusted).
- [ ] **Step 3: Run against real silver, confirm pass** (12 is gone after Task 4).
- [ ] **Step 4: Commit.**

---

## Self-Review

- **Spec coverage vs ADR-0103:** ② registry = Task 1; ③ resolver = Tasks 2–3; ① no-identity-from-mutable-code = Task 3 (code canonicalized at the chokepoint; full opaque ID deferred to Wave 3 per scope note); 광주 resolution = Tasks 4–6; ⑤ drift = Task 7 (seed). ④ steward + ⑥ bitemporal are explicitly Waves 2–3.
- **No silent drops:** unknown codes yield `None`/`unresolved` and are flagged, never a fabricated PNU (Tasks 2–3) — consistent with ADR-0023's "no fabricated lot number".
- **Reversibility:** silver re-derivation writes new Iceberg snapshots; old snapshots remain for rollback.
- **Open design items intentionally deferred:** the fully opaque recode-surviving surrogate key and its 40M-parcel re-identification (Wave 3, needs the time axis).
