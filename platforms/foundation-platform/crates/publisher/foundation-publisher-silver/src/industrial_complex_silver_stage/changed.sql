-- The complexes whose row is new or whose content differs from the table's current row (root
-- ADR-0311: append only the changed keys). The same comparison as the notice attachments
-- (`industrial_complex_notice_stage/changed.sql`): `IS NOT DISTINCT FROM` on the key and every content
-- column. Parameters: {current} (the current-rows query of the table, rendered from its contract, or
-- a query of no rows), {matches} (one `c.x IS NOT DISTINCT FROM r.x` per compared column, joined by
-- AND). Output: complex_rows_changed, in the contract's sort order.
CREATE OR REPLACE TEMP TABLE complex_rows_changed AS
SELECT r.* FROM complex_rows r
WHERE NOT EXISTS (SELECT 1 FROM ({current}) c WHERE {matches})
ORDER BY r.complex_name_normalized, r.official_complex_code;
