<!-- public-repository-safety: reviewed-public-contract -->

---
status: current
owner: foundation
doc_type: architecture
last_reviewed: 2026-07-28
---

# 행정구역 경계와 필지 식별자 버전 관리

**상태:** 승인된 구현 계약

This contract is subordinate to [ADR 0006](../adr/0006-object-storage-first-serving.md) and the
[single-source spatial publication architecture](./single-source-spatial-publication.md); it defines
identity/versioning only and does not replace their tile publication CAS rules.

Foundation treats official administrative codes, names, and PNU values as changing facts, not as
the identity of the land. The legal transition establishing **전남광주통합특별시** took effect on
2026-07-01. The [official law](https://www.law.go.kr/lsInfoP.do?efYd=20260701&joNo=029500&lsiSeq=284111&query=%EC%A0%84%EB%82%A8%EA%B4%91%EC%A3%BC%ED%86%B5%ED%95%A9%ED%8A%B9%EB%B3%84%EC%8B%9C+%EC%84%A4%EC%B9%98%EB%A5%BC+%EC%9C%84%ED%95%9C+%ED%8A%B9%EB%B3%84%EB%B2%95)
and [Ministry of the Interior and Safety implementation notice](https://www.mois.go.kr/frt/bbs/type010/commonSelectBoardArticle.do?bbsId=BBSMSTR_000000000008&nttId=126841)
are the source evidence.

## 불변식

1. `catalog.parcel.id` and `catalog.industrial_complex.id` are stable internal UUID identities.
2. PNU, administrative code, and administrative name are effective-dated external identifiers.
3. A **standard cadastral PNU** is never reassigned to another parcel. An old identifier remains resolvable to the same
   stable UUID through the historical lookup view; the existing API response remains unchanged and
   returns the current projection PNU.
4. Rename means a new identifier (a new effective identifier/name fact) on the same stable administrative unit. A
   merger, replacement, or split is an auditable transition between distinct stable units; there is
   no `renamed` self-edge.
5. Every approved fact carries a canonical `data_revision`, source snapshot, and Catalog
   `source_record` UUID. Facts are append-only for the API role; interval closure and the legacy
   `catalog.parcel.pnu` projection can happen only through the publisher function.
6. One public tile source is built from exactly one `data_revision`. Dynamic Martin and static PMTiles
   are projections of that revision, never independent authorities.

`effective_period` uses PostgreSQL half-open date ranges `[start, end)`. The legal effective date is
the lower bound in Korea Standard Time; `infinity` means currently open. There is no midnight UTC
conversion in the identity contract. The current view uses the date of the database transaction,
while historical alias lookup intentionally ignores the date because PNU ownership is permanent.

## Schema and bridge

Migration `20260727000001_administrative_boundary_identity.sql` adds:

- `catalog.administrative_boundary_revision`, the canonical revision ledger. Its UUID is reused as
  the `vector_tile_release.data_revision` when that revision is published. The numeric
  `canonical_iceberg_snapshot_id` is kept separate from the textual `source_snapshot_id`; the new
  migration binds release UUID + numeric snapshot with a composite foreign key.
- `catalog.administrative_unit` plus effective-dated code/name rows in
  `catalog.administrative_unit_identifier`.
- `catalog.administrative_unit_transition` (`replaced_by`, `merged_into`, `split_from`) and the
  effective parent hierarchy in `catalog.administrative_unit_parent`.
- `catalog.parcel_identifier`, keyed to stable `catalog.parcel.id`, and
  `catalog.parcel_administrative_unit` membership facts.
- Exclusion/unique constraints for overlapping intervals, source/revision foreign keys, a cycle
  guard, unit-kind membership guard, and append-only triggers.
- `catalog.parcel_identifier_lookup` (all historical aliases) and
  `catalog.parcel_current_identifier` (the current display/filter PNU).
- `catalog.parcels_missing_temporal_identifier`, an operational health view for the bounded legacy
  fallback; a standard-PNU row in this view is a migration defect, not a valid steady state.

Existing standard-PNU parcels are backfilled with deterministic `foundation.migration` source records
and one published legacy revision beginning at the legacy row's `created_at` date. This is an explicit
compatibility bridge, not invented provenance. Block/register parcels without a standard PNU keep
their existing register-key contract and are not silently fabricated into a PNU alias.
Future ingestion must register the real `catalog.source_record`, create a candidate revision, insert
the complete facts, validate them, project PostGIS, and only then publish the tile manifest through
the existing CAS gate. `catalog.parcel.pnu` is updated in the same controlled publisher transaction;
new repository reads already resolve through the views, so old fixtures and callers continue to work.

The point-lookup artifacts mentioned by ADR-0006 are a separate projection. If JSON/KV PNU artifacts
are enabled, the publisher must emit both the old alias and the new current PNU (or a resolver record)
from the same revision; they are not allowed to become a second identity authority.

## Merger procedure

For a Jeonnam–Gwangju change (all transition edges use predecessor `from_unit_id` → successor
`to_unit_id`; `split_from` is emitted once per new successor):

1. Store the official source object and register its `source_record`.
2. Create a candidate `administrative_boundary_revision` and stable unit(s). Preserve old units and
   their old identifiers; close intervals only through the publisher, never by ad-hoc SQL.
3. Add the new unit/name/code facts and `merged_into`/`replaced_by` edges. A simple rename is a new
   identifier fact on the old stable unit.
4. Reconcile every affected parcel's membership and PNU fact. The parcel UUID, geometry lineage,
   buildings, and listings do not get recreated merely because an administrative label changed.
5. Validate counts, geometry, hierarchy, alias uniqueness, transition coverage, and one current
   membership per level. Project one complete PostGIS snapshot and record the same `data_revision`.
6. Promote dynamic Martin through the runtime-manifest CAS pointer when immediate visibility is
   required. The browser/MapLibre contract is unchanged.
7. Build and validate an immutable PMTiles release from the same revision on the approved schedule,
   then promote it. Old releases remain available for rollback; they are never overwritten.

If a correction arrives during a PMTiles build, the build is marked superseded because its frozen
`data_revision` is no longer current. A newer build starts from the newer revision. This prevents an
old administrative name or polygon from resurfacing.

## Tile and API compatibility

MVT feature property `pnu` remains the current PNU because the existing MapLibre manifest/style and
Foundation marker contracts use it for feature filtering and IDs. The stable parcel UUID is internal
identity/lineage and is also carried where the layer schema allows. No client DTO or renderer change
is required. Static and dynamic sources are still switched as one complete source per the
[single-source spatial publication contract](./single-source-spatial-publication.md).
Anchor rebuilds must carry the stable `parcel_id`; summary reads prefer that UUID and use PNU only as
the legacy fallback, so a PNU transition cannot orphan an existing anchor.

The official-boundary source writer now retains each Polygon/MultiPolygon and a deterministic geometry
hash in the source JSONL. `write-administrative-spatial-scope-registry` validates that evidence, and
`publish-administrative-boundary-postgis` materializes it into the append-only
`serving_postgis.administrative_unit_boundary_publication` table. Martin's checked-in dynamic config
exposes the pointer-selected `admin` view; it is empty until an `admin` dynamic release is included in
a complete runtime-manifest CAS promotion. `promote-administrative-boundary-runtime` creates that
release and a complete next manifest, then delegates the atomic switch to the existing CAS function.
The publisher deliberately does not invent missing government names/parent facts. This keeps
collection, validation, projection, and runtime promotion separate and auditable.
