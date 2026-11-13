-- {source 동·리 code: the served parcel edition's code for the same place}
-- (legal_dong_code_change_views.renumbered_into, root ADR-0329 §3). Inputs: pair_rows
-- (pair_rows.sql), served_codes(code): the 10-digit codes the served parcel edition holds.
-- Only a 1:1 renumbering is translated, either way: its lots keep their numbers (ADR-0144 §3.1). A split
-- or merged code, or one changed twice, is not one place on both sides and is left for the lineage. A
-- code the served edition holds is never translated.
CREATE OR REPLACE TEMP TABLE renumbering AS
WITH leaf AS (
  SELECT DISTINCT old_code, new_code FROM pair_rows
  WHERE level IN ('eupmyeondong', 'ri') AND old_code <> new_code
),
one_to_one AS (
  SELECT old_code, new_code FROM leaf
  WHERE old_code IN (SELECT old_code FROM leaf GROUP BY old_code HAVING count(*) = 1)
    AND new_code IN (SELECT new_code FROM leaf GROUP BY new_code HAVING count(*) = 1)
),
both_ways AS (
  SELECT old_code AS src, new_code AS dst FROM one_to_one
  UNION ALL
  SELECT new_code AS src, old_code AS dst FROM one_to_one
),
into_served AS (
  SELECT DISTINCT src, dst FROM both_ways
  WHERE dst IN (SELECT code FROM served_codes) AND src NOT IN (SELECT code FROM served_codes)
)
SELECT src, any_value(dst) AS dst FROM into_served GROUP BY src HAVING count(*) = 1;
