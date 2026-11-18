-- The inputs of one lineage group, read once from the pinned snapshots of the lakehouse
-- (root ADR-0113 §1): `derive.sql` reads only these tables.
-- Parameters: {lakehouse} (the attached catalog), {boundaries_from}, {boundaries_to}, {code_changes},
-- {history} (Iceberg snapshot ids), {from_snapshot_id}, {to_snapshot_id} (parcel editions),
-- {from_sido}, {to_sido}, {every_sido}: `pnu` predicates. A predicate is a range per sido prefix, so
-- the scan skips the files of every other sido by their bounds; `substr(pnu, 1, 2) IN (…)` reads
-- the whole edition. {row_state}: the removal column, or a NULL where the pinned snapshot is from
-- before the column.

CREATE OR REPLACE TEMP TABLE before_raw AS
SELECT DISTINCT pnu FROM {lakehouse}.silver.parcel_boundaries AT (VERSION => {boundaries_from})
WHERE source_snapshot_id = '{from_snapshot_id}' AND {from_sido};

CREATE OR REPLACE TEMP TABLE after_raw AS
SELECT DISTINCT pnu FROM {lakehouse}.silver.parcel_boundaries AT (VERSION => {boundaries_to})
WHERE source_snapshot_id = '{to_snapshot_id}' AND {to_sido};

CREATE OR REPLACE TEMP TABLE code_changes AS
SELECT kind, old_code, new_code, level, effective_date, source
FROM {lakehouse}.reference.legal_dong_code_change AT (VERSION => {code_changes})
WHERE kind IN ('pair', 'supersession') AND level IN ('eupmyeondong', 'ri');

-- The history's current rows, as its contract's `current_row_latest` says: the newest row per
-- (pnu, transfer_history_seq) by source_snapshot_id, then ingested_at_utc; rows that tie are all
-- current; a key whose newest row is a removal marker has none (root ADR-0275).
CREATE OR REPLACE TEMP TABLE history AS
SELECT pnu, reason_code, reason, moved_at, erased_at, land_category_code, area_m2, transfer_history_seq
FROM (
  SELECT h.*, rank() OVER (
           PARTITION BY pnu, transfer_history_seq
           ORDER BY source_snapshot_id DESC NULLS LAST, ingested_at_utc DESC NULLS LAST) AS newest
  FROM (
    SELECT pnu, transfer_history_seq, reason_code, reason, moved_at, erased_at, land_category_code, area_m2,
           source_snapshot_id, ingested_at_utc, {row_state} AS row_state
    FROM {lakehouse}.silver.land_transfer_history AT (VERSION => {history})
    WHERE {every_sido}
  ) h
)
WHERE newest = 1 AND (row_state IS NULL OR row_state <> 'removed');
