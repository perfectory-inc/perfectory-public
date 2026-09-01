# ADR 0069: one column holds five kinds of thing

- Status: Accepted
- Date: 2026-09-01

## Context

Root ADR-0068 fixed a caller passing two spellings of one object name. Surveying every Silver and
Gold table afterwards showed that was the smallest version of the problem. `source_record_id` and
`source_snapshot_id` hold five different kinds of value, and the re-run guard reads all of them as
if they were the same kind.

Measured against the live catalog on 2026-09-01:

```
table                            distinct   example
silver.parcel_boundaries              255   30563-23.zip
silver.industrial_complexes          1442   foundation-platform:bronze:bronze/source=
                                            vworldkr__sandan_profile/30138-6.zip#247930
silver.industrial_complex_boundaries    1   bronze/source=vworldkr__sandan_boundary/30137-1.zip
silver.building_register_units          1   remote-building-register-unit-pipeline-full-
                                            20260620-20260709T071712Z
silver.building_register_unit_areas     1   remote-building-register-unit-area-pipeline-full-…
gold.complex_catalog                    1   vworldkr__sandan_profile-202506
```

They are not five spellings of one thing. They are three different things:

- **An object.** `industrial_complex_boundaries` and `parcel_boundaries` name the archive their
  rows came from. This is what the guard assumes it is reading.
- **A row.** `industrial_complexes` carries `…30138-6.zip#247930` — 1,442 distinct values for
  1,442 rows. Every row names itself. One batch of that table would report 1,442 ingested
  objects, and `MAX_BATCH_SOURCE_RECORDS` is 64, so switching the guard on there does not protect
  the table; it refuses the load.
- **A run.** The building-register tables and `gold.complex_catalog` name the pipeline execution
  or the dataset month. One value covers 113,813,264 rows. It is a real identity, just a coarser
  one than an object.

### What the guard actually does today

`read_ingested_objects` reads `foundation.ingest-batch-objects` out of the snapshot summaries.
Only `silver.parcel_boundaries` has any: 287 entries. Every other table records nothing, so the
guard reads an empty registry and concludes nothing has been loaded.

Re-running any of them appends everything a second time. Nothing stops it:

```
silver.building_register_unit_areas   113,813,264 rows   no registry
silver.building_register_units         19,765,555 rows   no registry
silver.parcel_boundaries               39,861,511 rows   registry in two shapes (ADR-0068)
silver.industrial_complexes                 1,442 rows   no registry
silver.industrial_complex_boundaries        1,343 rows   no registry
gold.complex_catalog                        1,442 rows   no registry
```

**Root ADR-0062's protection is live on one table out of six.** Between them the other five hold
133,583,046 rows.

### Why nothing failed loudly

`source_record_id` is `required: false` for both building-register tables and absent from the
`gold.complex_catalog` contract, while it is `required: true` for parcel and industrial complex.
`add_missing_nullable_iceberg_columns` therefore adds it silently on the next load and every
existing row gets `NULL`. The load proceeds, the guard has nothing to compare, and the table
doubles. An earlier reading of this repository — that such a load would fail rather than
duplicate — was wrong, and wrong in the more dangerous direction.

The optional flag is not the defect, though, and saying it was is a second misreading of the same
tables. The column those two identify their loads by is `source_snapshot_id`, and that one is
`required: true`. Measured across all nine contracts, every table that has a load unit names a
required column. The guard was reading a column that means something else on three tables — not
reading an optional column.

## Decision

1. **A table declares the unit one load carries.** The contract for each table gains
   a `load` block whose `unit` is one of:
   - `object` — one collected object per value; the guard compares object identities.
   - `run` — one collection execution per value; the guard compares run identities.
   - `derived` — the table is derived from other tables and has no collected source; the guard does
     not apply and the loader must say so rather than read an empty registry as "not loaded".

   It is declared rather than inferred. Inferring it from the values would make the answer depend
   on what happens to be in the table today, and a table loaded once looks like every kind at
   once.

2. **The column the guard reads is named by the contract, not assumed.** `SOURCE_RECORD_COLUMN`
   is a default inside `lakehouse_ingest`, and three tables do not use it. The contract names the
   column so the guard cannot read a column that means something else.

3. **A value that identifies a row is not a load unit.** `silver.industrial_complexes` carries a
   per-row identity; its unit is `object` and the guard must read the object part, not the
   row part. The same column doing both jobs was split for the building-register handoffs by
   `building_register_row_identity::row_identity`, which keeps the object name and the line number
   apart. `silver.industrial_complexes` has not been through that split, so until it is, the
   contract names the derivation that recovers the object from the value.

4. **A table with a load unit and an empty registry refuses to load.** Today it
   appends. The refusal names the backfill required to proceed, so a run that would double a
   hundred-million-row table stops with an instruction rather than succeeding.

5. **Backfill is one commit that adds no rows**, carrying `foundation.ingest-batch-objects` for
   the identities the table already holds, read from the table's own rows. Probed on a scratch
   table on 2026-08-31: a zero-row `append` does produce a snapshot and does carry the summary; an
   `UPDATE` produces a snapshot with no summary at all. A migration that rewrites values therefore
   needs both operations, and doing only the `UPDATE` would leave the registry untouched while
   appearing to have fixed the table.

6. **The column a load unit names is required.** This already holds — measured across all nine
   contracts, every table with a load unit names a `required: true` column — so the decision is to
   keep it holding rather than to change anything. A unit pointing at an optional column would let
   the loader add it, fill existing rows with nulls, and count nothing while reporting success.

7. **A guard enforces (1) and (6):** every table in the contract artifact declares a load unit;
   every table whose unit is not `derived` names a column; that column exists on the table; and it
   is required. Failing it prevents a table being added with no statement of what a load is, which
   is how five tables came to have no registry without anyone noticing.

## Consequences

Five tables cannot be re-loaded until they are backfilled. That is a change from today only in
that it is now visible: they could not be re-loaded safely before either, and the difference is
whether the loader says so or doubles the table.

The backfill for `silver.industrial_complexes` needs the object part of a per-row value, so it
depends on (3). The building-register tables backfill from a single run identity each, which is
one entry per table.

`gold.complex_catalog` is derived from `silver.industrial_complexes`. Its unit is `derived`:
`industrial_complex_silver_to_gold.py` defaults to `overwrite` and refuses that mode on a
non-`_smoke` table unless `--allow-non-smoke-overwrite` is given, so a re-run replaces the table
rather than adding to it. A registry would be answering a question nobody asks of that table.

This ADR does not perform the backfills. It makes the loader refuse rather than duplicate, which
is the part that stops the next accident.
