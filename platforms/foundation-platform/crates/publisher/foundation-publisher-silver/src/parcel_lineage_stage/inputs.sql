-- The inputs of one lineage group, read once from the pinned snapshots of the lakehouse
-- (root ADR-0113 §1): `derive.sql` reads only these tables.
-- Parameters: {boundaries_from}, {boundaries_to}, {code_changes}, {number_changes} (each table
-- at its pinned snapshot, from `duckdb_parquet_stage::table_at_in`), {from_snapshot_id}, {to_snapshot_id}
-- (parcel editions), {from_sido}, {to_sido}: `pnu` predicates, {from_old_pnu}: the same
-- as {from_sido} on `old_pnu`. A predicate is a range per sido prefix, so
-- the scan skips the files of every other sido by their bounds; `substr(pnu, 1, 2) IN (…)` reads
-- the whole edition. {history}: the land movement history's current rows at its pin in every sido
-- of the group (`history_rows`).

CREATE OR REPLACE TEMP TABLE before_raw AS
SELECT DISTINCT pnu FROM {boundaries_from}
WHERE source_snapshot_id = '{from_snapshot_id}' AND {from_sido};

CREATE OR REPLACE TEMP TABLE after_raw AS
SELECT DISTINCT pnu FROM {boundaries_to}
WHERE source_snapshot_id = '{to_snapshot_id}' AND {to_sido};

CREATE OR REPLACE TEMP TABLE code_changes AS
SELECT kind, old_code, new_code, level, effective_date, source
FROM {code_changes}
WHERE kind IN ('pair', 'supersession') AND level IN ('eupmyeondong', 'ri');

-- The history's current rows, as its contract's `current_row_latest` says (the contract's own query,
-- never a copy): the newest rows per (pnu, transfer_history_seq), every tie; a key whose newest row
-- is a removal marker has none (root ADR-0275).
CREATE OR REPLACE TEMP TABLE history AS {history};

-- The provider's official old -> new PNU records (VWorld 30527) whose old parcel lies in the earlier
-- edition's sido. The table is events: a load appends only the events it does not hold, so every
-- row is read, never one file (its canonical reader `official_links`, root ADR-0318 §8);
-- `derive.sql` keeps the ones the rules admit.
CREATE OR REPLACE TEMP TABLE number_changes AS
SELECT DISTINCT old_pnu, new_pnu, is_dong_level, reason_code, changed_on, quarantine_reason
FROM {number_changes}
WHERE {from_old_pnu};
