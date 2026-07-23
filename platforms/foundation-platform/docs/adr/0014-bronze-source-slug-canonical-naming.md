# ADR 0014 - Bronze source-slug canonical naming + single generator

- Status: Accepted (naming + generator)
- Date: 2026-06-23
- Owner: foundation-platform
- Supersedes the "keep physical keys stable" recommendation of
  `docs/architecture/bronze-key-naming-and-catalog-principle.md` §4 **for the source-slug segment only**
  (owner chose a one-time standardization migration; cost accepted).

## Context

Bronze object keys are `bronze/source={source_slug}/run_id=.../partition=.../part-NNNNNN.ext`
(`crates/catalog/catalog-domain/src/bronze.rs:472`). The `source_slug` segment is currently
**inconsistent and produced in 5 disconnected places with no SSOT** (2026-06-23 6-agent audit):

1. **Catalog** `docs/catalog/public-source-endpoint-catalog.v1.json` - 67 hand-authored
   `bronze.source_slug` literals (hyphen-cased, e.g. `hub-building-building-register`,
   `data-go-kr-building-register-getbrtitleinfo`). Only the `building_hub_bulk` lane reads them at runtime.
2. **In-code `OPERATION_SPECS` tables** - `real_transaction_ingest.rs:406-491` (13) and
   `vworld_ned_attribute_ingest.rs:390-454` (7) hardcode slug literals keyed by operation.
3. **Per-binary `DEFAULT_SOURCE_SLUG` constants** - `building_register_ingest.rs:40`
   (`molit-building-register`), `vworld_cadastral_ingest.rs:35` (`vworld-cadastral`),
   `vworld_land_register_ingest.rs:36` (`vworld-land-register`). **These DIVERGE from the catalog**
   for the same data (a separate `molit-*` lineage).
4. **National-run pilot defaults** (`*-national-pilot`) + shard-writer **derived** slugs
   (`vworld-cadastral-national-{sigungu}-{bjdong}`, `national_data_collection_shard_manifest_writer.rs:643`).
5. **Fallback formatter** `building_hub_bulk_collection_plan.rs:318` =
   `hub-go-kr-public-bulk-task-{group}-{code}` (the opaque codes the owner flagged).

Two slugs for the same dataset can already silently diverge. The naming is also not engine-portable:
the slug becomes a future Silver/Gold table name, and dashes break BigQuery datasets / need backticks
in Databricks (sources in `docs/architecture/bronze-key-naming-and-catalog-principle.md` §3).

## Decision

### D1 - Canonical slug format
`source_slug = {providerid}__{dataset_slug}` - all lowercase, `dataset_slug` in `snake_case`, **double
underscore** between provider and dataset. Grounded in dbt `source__entity`, BigQuery (`-` forbidden,
`_` allowed), Databricks (lowercase) - see the principle doc.

### D2 - Provider id map (the only hand-maintained table)
| catalog `provider` | providerid |
|---|---|
| `VWorld` | `vworldkr` |
| `data.go.kr` | `datagokr` |
| `hub.go.kr` | `hubgokr` |
| `juso` | `jusogokr` |
| `mois.go.kr` | `moisgokr` |
| `factoryon.go.kr` | `factoryongokr` |

### D3 - Separate `operation` (API call id) from `dataset_slug` (semantic identity); ONE generator
The slug must NOT be derived from `operation`. For data.go.kr the operation is the raw API method
(`getBrTitleInfo`), but the approved slug is semantic (`datagokr__building_register_main`) - a
`snake_case(operation)` transform would wrongly produce `datagokr__get_br_title_info`. So each source
carries **two distinct identifiers**:
- **`operation`** - provider-native API call id (e.g. `getBrTitleInfo`, `getRTMSDataSvcAptTradeDev`,
  `parcel`). Used to actually call the provider. **Unchanged** by this ADR.
- **`dataset_slug`** (a.k.a. `canonical_source_dataset`) - the canonical semantic dataset identity in
  `snake_case` (e.g. `building_register_main`). Curated per source. For hub/vworld/mois/factoryon it
  usually equals the operation; for data.go.kr it is the meaningful name (the operation->dataset_slug
  map is the data.go.kr part of the rename table).

The single generator is `source_slug(provider, dataset_slug) = {providerid(provider)}__{dataset_slug}`.
Every producer in §Context (catalog authoring, `OPERATION_SPECS`, `DEFAULT_SOURCE_SLUG`, pilot/derived
slugs, fallback formatter) calls it instead of hand-writing literals. A new **`dataset_slug`** field is
added to each endpoint in `public-source-endpoint-catalog.v1.json`; the existing `bronze.source_slug`
becomes a **derived** value, and CI asserts
`bronze.source_slug == source_slug(provider, dataset_slug)` for every in-scope entry. This eliminates the
5-way divergence AND makes the approved data.go.kr semantic names producible.

### D4 - Relax the slug charset validator (blocking prerequisite)
`validate_source_slug` (`bronze.rs:484-503`) currently allows only `[a-z0-9-]` and **rejects `_`** - so
the new `__` slug cannot be written until it is widened to allow `_`. The DB column
`catalog.source_catalog.slug` CHECK already allows `_` (`^[a-z0-9][a-z0-9_-]*$`,
`migrations/20260513000001:27`), so only the Rust validator changes. This is a key-format **contract
change**; once shipped the format is frozen (further changes = new ADR + migration).

### D5 - Migration = re-collect (pre-launch), not in-place rewrite
Because there are **0 users** and no committed Bronze data (all manifest/ledger/evidence are runtime
under `target/audit/`; only `catalog.bronze_object` + `catalog.outbox_event` DB rows persist), the
chosen migration is: land the code -> **re-collect under the new slugs into new R2 prefixes + fresh
`source_catalog` rows** -> verify -> **only then delete old prefixes/rows**. This avoids the risky
R2-copy + DB-rewrite of immutable keys and naturally honors "no deletion before migration verification".
An in-place copy migration (Strategy B) is documented in the plan as the fallback **if** irreplaceable
Bronze data exists.

### D6 - Explicitly OUT of scope (do NOT rename)
- `endpoint_slug` (camelCase routing identity, e.g. `data-go-kr-building-register-getBrTitleInfo`) -
  a different identifier; tests asserting it keep old values. No blanket find/replace.
- `national-data-normalization-contract.v1.json` transformer slugs (a separate Silver namespace).
- `public-data-bronze-lane-registry.v1.json` `lane_id`s and CLI command tokens.
- `mixed_public_source` / POI (10) and unregistered `hub-go-kr-public-bulk-task-*` - deferred
  (registered + named when first used).

## Consequences

- **Claim-check break for any data already written under old keys.** R2 has no rename; the object_key
  is referenced by `bronze_object.object_key`, the `collection.raw_written.bronze_object_key` event,
  ledger/manifests, and Silver/Gold lineage. Re-collect (D5) sidesteps this pre-launch; in-place
  migration would require copying objects + updating rows in lockstep.
- **dedupe_key drift** - slug is embedded in `bronze_object.dedupe_key` (`{slug}:...`,
  `public_data_bronze_plan.rs:186`); re-collect starts a clean dedupe namespace.
- **No bare `datagokr__building_register`** - the divergent per-binary default `molit-building-register`
  (data.go.kr building-register API) must resolve to the SPECIFIC sub-type its run collects (e.g.
  `datagokr__building_register_main` for `getBrTitleInfo`) via the operation->dataset_slug map, NOT a
  bare `datagokr__building_register`, which would collide/ambiguate with the 10 approved building-register
  sub-type slugs. The bare default constant is removed in favor of the generator.
- **catalog sha256 re-pin** - the catalog file is sha256-pinned at plan-compile + execute
  (`national_data_collection_plan_compile.rs:94`, `endpoint_catalog.rs:36`); editing it forces
  regenerating manifests/plans.
- **Runtime guards + many test pins must move** - e.g. `real_transaction.rs:73`
  `starts_with("data-go-kr-real-transaction-")`, plus pinned slug literals across ~15 test files
  (enumerated in the plan). `endpoint_slug` pins stay.
- **gongzzang (downstream consumer)** receives new `bronze_object_key`/`endpoint_slug` values in
  `collection.raw_written`; the event schema is unchanged (value-only change).

## References
- `docs/catalog/bronze-source-slug-rename.v1.md` - old->new + operation->dataset_slug mapping
  (owner-approved SSOT; the generator's human-readable projection and executable migration map).
- `docs/architecture/bronze-key-naming-and-catalog-principle.md` - naming sources + the
  stable-keys principle this ADR overrides for the slug segment.
- Dated impact-audit evidence is retained outside the public code tree under
  [root ADR-0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).
