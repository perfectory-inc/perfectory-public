-- The rows an edition appends (root ADR-0318): every staged row of a key that is new or whose rows
-- changed, and one removal marker per key with current rows and none staged. It was
-- `silver_release_compare.release_changes`: a key is compared by the multiset of its rows' content
-- (a row keyed by its own SHA-256 may repeat), null-safe.
--
-- Input: file_staged (the linked rows, the table's columns) and file_current (the table's current
-- rows the edition is compared with, the same columns). Parameters: {content} (the content columns),
-- {same} (`c.x IS NOT DISTINCT FROM s.x` per content column), {content_order} (`c.x` per content
-- column), {columns}, {marker_columns} (a current row's columns with the removal column,
-- `source_snapshot_id` and `ingested_at_utc` this load's).
-- Output: file_changes.

CREATE OR REPLACE TEMP TABLE file_changes AS
WITH s AS (SELECT row_key, {content}, count(*) AS n FROM file_staged GROUP BY ALL),
     c AS (SELECT row_key, {content}, count(*) AS n FROM file_current GROUP BY ALL),
     changed AS (
       SELECT s.row_key FROM s
       WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.row_key = s.row_key AND c.n = s.n AND {same})
       UNION
       SELECT c.row_key FROM c
       WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.row_key = c.row_key AND s.n = c.n AND {same})
     )
SELECT {columns} FROM file_staged
WHERE row_key IN (SELECT row_key FROM changed)
UNION ALL
-- One marker per vanished key, made of one of its current rows (the required columns need values).
SELECT {marker_columns} FROM (
  SELECT c.*, row_number() OVER (PARTITION BY c.row_key
                                 ORDER BY {content_order}, c.source_record_id, c.source_line_number) AS file_first
  FROM file_current c
  WHERE c.row_key NOT IN (SELECT row_key FROM file_staged)
) c
WHERE file_first = 1;
