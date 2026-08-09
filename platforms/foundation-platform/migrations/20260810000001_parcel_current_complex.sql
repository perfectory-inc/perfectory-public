-- One definition of "the complex this parcel is in right now", and a lock on the column it replaces.
--
-- ADR-0019 step 2. Membership became a dated fact in `20260809000001`, which means "the parcels of
-- this complex" stopped being answerable without a point in time. ADR-0022 fixes that point at
-- CURRENT_DATE and refuses an `as_of` parameter while no history exists to query.
--
-- The predicate lives in a view rather than in each caller. `catalog.parcel_current_identifier`
-- (20260727000001) already owns the same shape for the parcel's current PNU, and it is the only
-- other place in this repository that spells CURRENT_DATE. A second copy of a time rule is the
-- defect ADR-0018 names, and when two copies of a time rule disagree nothing records when they
-- began to.

-- SECURITY INVOKER (the default) on purpose: this view must not become a way to read memberships
-- past the base table's grants.
CREATE VIEW catalog.parcel_current_complex AS
SELECT m.parcel_id,
       m.complex_id,
       m.asserted_by,
       m.effective_period,
       m.data_revision
  FROM catalog.parcel_complex_membership AS m
 WHERE m.effective_period @> CURRENT_DATE;

COMMENT ON VIEW catalog.parcel_current_complex IS
    'The industrial complex each parcel belongs to today. Sole definition of "current" for membership (ADR-0022); readers must not restate the CURRENT_DATE predicate.';

-- `catalog.parcel.complex_id` is now read by nothing in production, but it is still NOT NULL and
-- still carries the value the backfill copied out of it. An UPDATE is the only way the column and
-- the membership table can start disagreeing, so it is closed here; the column becomes a frozen
-- snapshot until ADR-0019 step 3 drops it.
--
-- INSERT is deliberately not blocked. The column is NOT NULL until step 3, so every path that
-- creates a parcel — fixtures, seeds, tests — must still supply it. This mirrors
-- `protect_parcel_pnu_projection` exactly, including the "value actually changed" test: an UPDATE
-- that rewrites the row without touching this column is not an attempt to move the parcel.
CREATE OR REPLACE FUNCTION catalog.protect_parcel_complex_projection()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.complex_id IS DISTINCT FROM OLD.complex_id
       AND current_setting('foundation.temporal_publisher', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'catalog.parcel.complex_id is a frozen projection; membership is written to catalog.parcel_complex_membership'
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER parcel_complex_projection_guard
BEFORE UPDATE ON catalog.parcel
FOR EACH ROW EXECUTE FUNCTION catalog.protect_parcel_complex_projection();
