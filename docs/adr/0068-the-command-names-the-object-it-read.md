# ADR 0068: the command names the object it read

- Status: Accepted
- Date: 2026-08-31

## Context

On 2026-08-31 a re-run of the national parcel load appended 1,164,467 rows that were already in
`silver.parcel_boundaries`. The table went from 39,861,511 rows to 41,025,978 before the run was
stopped at the second of sixteen batches, and was returned to 39,861,511 by rolling the branch
back to the snapshot preceding the two appends.

The re-run guard (root ADR-0062) compares the source objects a batch carries against the objects
recorded in the table's snapshot summaries. It compares strings. The same file was recorded under
two different strings:

```
loaded 2026-08-28    30563-196.zip
loaded 2026-08-31    bronze/source=vworldkr__parcel/30563-196.zip
```

Neither producer was wrong about the file. `source_record_id` is supplied by the caller through
`FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_RECORD_ID`, the command stores whatever
arrives, and nothing decides what the value should look like. Two callers chose differently, and
the guard read that as two files.

**The bare form is not merely less precise; it is wrong.** Listing the 99 dataset prefixes under
`bronze/` and sampling 3,010 objects found file names that appear in more than one dataset:

```
export.csv          12 datasets   (apartment trade, rent, presale, …)
page-000001.json     4 datasets
```

Under bare names those twelve objects are one object. Loading the first would mark the other
eleven ingested, and eleven datasets would be silently absent from a table that reports success.
The failure this repository already suffered — duplication — is the less damaging of the two.

### What production systems do

None of them let a caller supply the value.

- Snowflake `COPY INTO` maintains its own load metadata per file and skips files it has already
  loaded; when it cannot determine whether a file was loaded it skips rather than guesses, and
  loading a file with expired metadata requires setting `LOAD_UNCERTAIN_FILES` explicitly. The
  file is readable as `METADATA$FILENAME`, which includes the path within the stage.
- Databricks `COPY INTO` is idempotent and ignores previously loaded files, including files
  modified since they were loaded; Auto Loader persists discovered file metadata in RocksDB in
  the stream's checkpoint.
- Spark exposes `_metadata.file_path` — the full path, filled by the engine. Delta Lake is
  replacing `input_file_name()` with it.

Two properties are common to all three: the identity is the **full path**, and it is produced by
the **engine that did the reading**, never typed by a caller.

## Decision

1. **The canonical identity of a source object is its full object key**, exactly as it appears in
   the bucket — `bronze/source=<dataset>/<name>`. Bare file names are not identities.

2. **A command derives the lineage value from the input it actually opened.** It is not accepted
   from the environment or from an argument.
   `FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_RECORD_ID` is removed rather than
   validated: a value a caller cannot supply cannot be supplied wrongly.

3. **A run that read a local path records that path**, and its summary continues to carry
   `read_a_local_file_not_a_collected_object`. Such a run names something real, and publication
   refuses it downstream because it is not a collected object — which is the correct outcome, not
   a gap to paper over.

4. **The re-run registry records canonical identities only.** `foundation.ingest-batch-objects`
   entries must be full object keys. A load offered a bare name is a load whose guard cannot be
   trusted, and it stops.

5. **Existing rows are migrated, not reinterpreted.** The 39,861,511 rows in
   `silver.parcel_boundaries` carry bare names. Each is mapped to the contract entry whose key
   ends in that name; a name matching zero or more than one entry stops the migration. The guard
   is not taught to accept two shapes: two accepted shapes is the defect this ADR exists to
   remove.

6. **Tables holding rows with no registry entry are backfilled from their own rows**, in one
   commit that adds no rows. `building_register_unit_areas` (113,813,264 rows),
   `building_register_units` (19,765,555), `industrial_complexes` (1,442),
   `industrial_complex_boundaries` (1,343) and `gold.complex_catalog` (1,442) were loaded before
   the guard existed and record nothing, so the guard reads an empty registry and would append
   all of them again.

7. **A guard enforces (2) by property, not by spelling:** a command that is given an input to
   open must not also be told what that input is called, in either direction — a script that
   passes both, or a command that reads an input key and takes the name from its environment.
   Failing it prevents the incident above: two values that can disagree, and nothing that notices
   when they do.

   The rule is not "this variable name is forbidden", and the first draft that was learned this
   the hard way. Three publication commands take a variable with the same ending whose value is a
   `catalog.publication_revision` row id;
   `industrial_complex_boundary_runtime_promote.rs` documents the distinction. A guard that
   refused them would be refusing correct code, which is how guards get disabled.

   That collision is real debt: `source_record_id` names the collected object in the ingest path
   and a revision row in the publication path, and one name meaning two things is what makes the
   distinction impossible to enforce mechanically. Splitting it is not attempted here — it
   touches a Postgres column and the publication commands — and it is recorded so the next reader
   does not mistake the narrow guard for the whole answer.

## Consequences

`vworld_cadastral_shapefile_silver_export` takes one less input, and callers that set it must
drop it. The conversion is unaffected otherwise: it already knew the key it opened.

The migration in (5) rewrites `source_record_id` across 39,861,511 rows. It is copy-on-write and
produces a new set of data files; the previous snapshot remains and the change is reversible by
branch rollback, as the incident recovery was.

Rolling back a branch does **not** remove entries from the registry: `.snapshots` lists every
snapshot in the metadata, including ones no longer reachable. The 32 objects appended and rolled
back on 2026-08-31 remain recorded. That is currently harmless — those objects' rows are present
under their bare names — but a rollback must not be assumed to have undone a registry entry.

Nothing here makes the load safe to re-run until (5) and (6) are done. Until then the parcel load
must not be run: the guard would recognise 32 objects and not the other 223.
