-- The item list rows to append (root ADR-0275, ADR-0311): every staged condition that is new or whose
-- content differs from its current rows, and, when the object holds the whole list, one removal
-- marker per condition with a current row the pass no longer holds. It was
-- `silver_release_compare.release_changes` over the placed rows: a key is compared by the multiset
-- of its rows (the staged side holds one), null-safe.
--
-- Input: onbid_staged (the placed rows, the table's columns). Parameters: {current} (the table's
-- current rows at the head the append is pinned on, every column), {key_matches} and
-- {content_matches} (`c.x IS NOT DISTINCT FROM s.x` per key / content column), {complete}
-- (true when the pass read every combination), {columns}, {marker_columns} (the current row's
-- columns, with the removal column, `source_snapshot_id`, `ingested_at_utc` and `source_record_id` this
-- load's).
-- Output: onbid_changes.

CREATE OR REPLACE TEMP TABLE onbid_current AS {current};

CREATE OR REPLACE TEMP TABLE onbid_changes AS
SELECT {columns} FROM onbid_staged s
WHERE NOT EXISTS (
  SELECT 1 FROM (SELECT *, count(*) OVER (PARTITION BY cltr_mng_no, pbct_cdtn_no) AS onbid_versions FROM onbid_current) c
  WHERE c.onbid_versions = 1 AND {key_matches} AND {content_matches})
UNION ALL
SELECT {marker_columns} FROM (
  SELECT c.*, row_number() OVER (PARTITION BY c.cltr_mng_no, c.pbct_cdtn_no
                                 ORDER BY c.source_record_id, c.source_line_number) AS onbid_first
  FROM onbid_current c
  WHERE {complete} AND NOT EXISTS (SELECT 1 FROM onbid_staged s WHERE {key_matches})
) c
WHERE onbid_first = 1;
