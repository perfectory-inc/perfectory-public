-- The rows of a newest-per-key table with removal markers to append (root ADR-0275, ADR-0311): every
-- staged key that is new or whose content differs from its current rows, and, when the object holds
-- the whole list, one removal marker per key with a current row the pass no longer holds. It was
-- `silver_release_compare.release_changes`: a key is compared by the multiset of its rows (the staged
-- side holds one), null-safe. The onbid item load's `item_changes.sql`, for any table.
--
-- Parameters: {current} (the table's current rows at the pinned head, every column), {staged},
-- {out}, {key} (the key columns), {key_matches} and {content_matches} (`c.x IS NOT DISTINCT FROM
-- s.x` per key / content column), {complete}, {columns}, {marker_columns} (the current row's
-- columns, with the removal column, `source_snapshot_id`, `ingested_at_utc` and `source_record_id` this
-- load's).
CREATE OR REPLACE TEMP TABLE {out}_current AS {current};

CREATE OR REPLACE TEMP TABLE {out} AS
SELECT {columns} FROM {staged} s
WHERE NOT EXISTS (
  SELECT 1 FROM (SELECT *, count(*) OVER (PARTITION BY {key}) AS foundation_versions FROM {out}_current) c
  WHERE c.foundation_versions = 1 AND {key_matches} AND {content_matches})
UNION ALL
SELECT {marker_columns} FROM (
  SELECT c.*, row_number() OVER (PARTITION BY {key_c} ORDER BY c.source_record_id, c.source_line_number)
           AS foundation_first
  FROM {out}_current c
  WHERE {complete} AND NOT EXISTS (SELECT 1 FROM {staged} s WHERE {key_matches})
) c
WHERE foundation_first = 1;
