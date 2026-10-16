-- Root ADR-0250 step 3b: `catalog.industrial_complex.lakehouse_complex_id` has held the same value
-- as `id` since 20261015090000 (`industrial_complex_lakehouse_complex_id_is_the_id`), and since step
-- 3a no code reads it. The value lives on in `id`, so dropping the column loses nothing.

-- Refuse rather than trust: a row whose mirror differs from its id would be the one fact this drop
-- could lose.
DO $precondition$
DECLARE
    disagreeing bigint;
BEGIN
    SELECT count(*) INTO disagreeing
    FROM catalog.industrial_complex
    WHERE lakehouse_complex_id IS DISTINCT FROM id;
    IF disagreeing > 0 THEN
        RAISE EXCEPTION 'ADR-0250: % industrial_complex rows carry a lakehouse_complex_id that is not their id', disagreeing;
    END IF;
END;
$precondition$;

-- Named, not left to the column drop, so an object this file did not foresee (a view) stops the
-- migration instead of disappearing with the column.
DROP INDEX catalog.industrial_complex_lakehouse_complex_id_idx;
ALTER TABLE catalog.industrial_complex
    DROP CONSTRAINT industrial_complex_lakehouse_complex_id_is_the_id,
    DROP CONSTRAINT industrial_complex_lakehouse_complex_id_is_uuid_v5,
    DROP COLUMN lakehouse_complex_id;
