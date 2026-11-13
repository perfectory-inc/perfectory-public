-- The pairs in force of the code change table (legal_dong_code_change_views.pair_rows, root
-- ADR-0145, ADR-0329), read once for every DuckDB stage that reads `reference.legal_dong_code_change`.
-- Input: code_changes(kind, old_code, new_code, level, effective_date, source). A pair a supersession
-- withdraws is left out unless a steward recorded it; one row per (old, new, source), a dated row
-- before an undated.
CREATE OR REPLACE TEMP TABLE pair_rows AS
WITH superseded AS (
  SELECT DISTINCT old_code, new_code FROM code_changes
  WHERE kind = 'supersession' AND coalesce(old_code, '') <> '' AND coalesce(new_code, '') <> ''
)
SELECT c.old_code, c.new_code, coalesce(c.source, '') AS source,
       coalesce(min(nullif(c.effective_date, '')), '') AS effective_date,
       count(DISTINCT nullif(c.effective_date, '')) AS dated_variants,
       arg_min(coalesce(c.level, ''), coalesce(nullif(c.effective_date, ''), '~')) AS level
FROM code_changes c
WHERE c.kind = 'pair' AND coalesce(c.old_code, '') <> '' AND coalesce(c.new_code, '') <> ''
  AND NOT (EXISTS (SELECT 1 FROM superseded s WHERE s.old_code = c.old_code AND s.new_code = c.new_code)
           AND NOT starts_with(coalesce(c.source, ''), 'steward:'))
GROUP BY c.old_code, c.new_code, coalesce(c.source, '');
