-- Root ADR-0250: an industrial complex has one id, the UUIDv5 the lakehouse derives from its
-- official code. Until now `catalog.industrial_complex.id` was a locally minted UUIDv7 that nothing
-- outside Postgres knew, and the lakehouse id rode beside it in `lakehouse_complex_id`.
--
-- This file re-keys every row onto the derived id through the column that already holds it, keeps
-- each retired v7 in an append-only ledger (nothing is lost), and makes the derivation a CHECK so a
-- minted id cannot be written again. Production on 2026-10-15: 1,442 rows, every one carrying a
-- lakehouse id equal to the derivation; every table referencing the key holds 0 rows.

-- 1. The derivation, in SQL: uuid5(NAMESPACE_URL, prefix || code). Same prefix and vectors as
--    `ComplexId::for_official_complex_code` and the Bronze-to-Silver job.
CREATE FUNCTION catalog.industrial_complex_id_for(official_complex_code text)
RETURNS uuid
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
    SELECT encode(
        set_byte(
            set_byte(hash, 6, (get_byte(hash, 6) & 15) | 80),
            8, (get_byte(hash, 8) & 63) | 128),
        'hex')::uuid
    FROM (
        SELECT substring(
            digest(
                decode('6ba7b8119dad11d180b400c04fd430c8', 'hex')
                    || convert_to('foundation-platform:catalog:industrial_complex:'
                                  || official_complex_code, 'UTF8'),
                'sha1')
            FROM 1 FOR 16) AS hash
    ) AS derived;
$function$;

-- 2. Refuse rather than guess: a stored lakehouse id that is not the derivation means a producer
--    derived differently, and choosing either value would hide that.
DO $precondition$
DECLARE
    disagreeing bigint;
BEGIN
    SELECT count(*) INTO disagreeing
    FROM catalog.industrial_complex
    WHERE lakehouse_complex_id IS NOT NULL
      AND lakehouse_complex_id <> catalog.industrial_complex_id_for(official_complex_code);
    IF disagreeing > 0 THEN
        RAISE EXCEPTION 'ADR-0250: % industrial_complex rows carry a lakehouse_complex_id that is not derived from their official_complex_code', disagreeing;
    END IF;
END;
$precondition$;

CREATE TEMP TABLE industrial_complex_rekey ON COMMIT DROP AS
SELECT id AS retired_id, catalog.industrial_complex_id_for(official_complex_code) AS complex_id
FROM catalog.industrial_complex
WHERE id <> catalog.industrial_complex_id_for(official_complex_code);
ALTER TABLE industrial_complex_rekey ADD PRIMARY KEY (retired_id);
ANALYZE industrial_complex_rekey;

-- 3. Append-only and sealed tables are not rewritten. Where they reference a key that moves, stop.
DO $sealed$
BEGIN
    IF EXISTS (SELECT 1 FROM catalog.parcel_complex_membership m
               JOIN industrial_complex_rekey r ON r.retired_id = m.complex_id)
       OR EXISTS (SELECT 1 FROM serving_postgis.parcel_boundary_mirror m
                  JOIN industrial_complex_rekey r ON r.retired_id = m.complex_id
                  WHERE m.complex_id IS NOT NULL) THEN
        RAISE EXCEPTION 'ADR-0250: an append-only or sealed table references an industrial complex id that is being retired';
    END IF;
END;
$sealed$;

CREATE TEMP TABLE industrial_complex_rekey_counts ON COMMIT DROP AS
SELECT 'industrial_complex' AS relation, count(*) AS row_count FROM catalog.industrial_complex
UNION ALL SELECT 'allowed_industry', count(*) FROM catalog.allowed_industry
UNION ALL SELECT 'blueprint', count(*) FROM catalog.blueprint
UNION ALL SELECT 'complex_attachment', count(*) FROM catalog.complex_attachment
UNION ALL SELECT 'complex_notice', count(*) FROM catalog.complex_notice
UNION ALL SELECT 'digital_twin_asset', count(*) FROM catalog.digital_twin_asset
UNION ALL SELECT 'industrial_complex_gold_pointer', count(*) FROM catalog.industrial_complex_gold_pointer
UNION ALL SELECT 'industry_group', count(*) FROM catalog.industry_group
UNION ALL SELECT 'spatial_layer', count(*) FROM catalog.spatial_layer;

-- 4. Re-key parent and editorial children in one deferred check. The two sealed tables keep
--    their immediate keys: step 3 proved no row of theirs points at a key that moves.
ALTER TABLE catalog.allowed_industry ALTER CONSTRAINT allowed_industry_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.blueprint ALTER CONSTRAINT blueprint_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.complex_attachment ALTER CONSTRAINT complex_attachment_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.complex_notice ALTER CONSTRAINT complex_notice_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.digital_twin_asset ALTER CONSTRAINT digital_twin_asset_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.industrial_complex_gold_pointer
    ALTER CONSTRAINT industrial_complex_gold_pointer_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.industry_group ALTER CONSTRAINT industry_group_complex_id_fkey DEFERRABLE;
ALTER TABLE catalog.spatial_layer ALTER CONSTRAINT spatial_layer_complex_id_fkey DEFERRABLE;
SET CONSTRAINTS ALL DEFERRED;

UPDATE catalog.industrial_complex c SET id = r.complex_id
FROM industrial_complex_rekey r WHERE c.id = r.retired_id;
UPDATE catalog.allowed_industry t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.blueprint t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.complex_attachment t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.complex_notice t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.digital_twin_asset t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.industrial_complex_gold_pointer t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.industry_group t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;
UPDATE catalog.spatial_layer t SET complex_id = r.complex_id
FROM industrial_complex_rekey r WHERE t.complex_id = r.retired_id;

SET CONSTRAINTS ALL IMMEDIATE;
ALTER TABLE catalog.allowed_industry ALTER CONSTRAINT allowed_industry_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.blueprint ALTER CONSTRAINT blueprint_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.complex_attachment ALTER CONSTRAINT complex_attachment_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.complex_notice ALTER CONSTRAINT complex_notice_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.digital_twin_asset ALTER CONSTRAINT digital_twin_asset_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.industrial_complex_gold_pointer
    ALTER CONSTRAINT industrial_complex_gold_pointer_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.industry_group ALTER CONSTRAINT industry_group_complex_id_fkey NOT DEFERRABLE;
ALTER TABLE catalog.spatial_layer ALTER CONSTRAINT spatial_layer_complex_id_fkey NOT DEFERRABLE;

UPDATE catalog.industrial_complex SET lakehouse_complex_id = id
WHERE lakehouse_complex_id IS DISTINCT FROM id;

-- 5. Every retired id stays, pointing at the id that replaced it. History written under a v7
--    (`catalog.outbox_event` payloads, `catalog.normalization_application.target_id`) is not
--    rewritten; this ledger is how it resolves.
CREATE TABLE catalog.industrial_complex_retired_id (
    retired_id uuid PRIMARY KEY,
    complex_id uuid NOT NULL REFERENCES catalog.industrial_complex(id),
    retired_at timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE catalog.industrial_complex_retired_id IS
    'Root ADR-0250: locally minted UUIDv7 ids an industrial complex had before its id became the derived UUIDv5. Append-only.';
CREATE INDEX industrial_complex_retired_id_complex_idx
    ON catalog.industrial_complex_retired_id (complex_id);
CREATE TRIGGER industrial_complex_retired_id_append_only
BEFORE UPDATE OR DELETE ON catalog.industrial_complex_retired_id
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();
INSERT INTO catalog.industrial_complex_retired_id (retired_id, complex_id)
SELECT retired_id, complex_id FROM industrial_complex_rekey;

-- 6. The invariant, so no writer can mint again.
ALTER TABLE catalog.industrial_complex
    ALTER COLUMN lakehouse_complex_id SET NOT NULL,
    ADD CONSTRAINT industrial_complex_id_is_derived_from_official_code
        CHECK (id = catalog.industrial_complex_id_for(official_complex_code)),
    ADD CONSTRAINT industrial_complex_lakehouse_complex_id_is_the_id
        CHECK (lakehouse_complex_id = id);

-- 7. Prove nothing was lost.
DO $counts$
DECLARE
    drift text;
BEGIN
    SELECT string_agg(before.relation, ', ') INTO drift
    FROM industrial_complex_rekey_counts before
    JOIN (
        SELECT 'industrial_complex' AS relation, count(*) AS row_count FROM catalog.industrial_complex
        UNION ALL SELECT 'allowed_industry', count(*) FROM catalog.allowed_industry
        UNION ALL SELECT 'blueprint', count(*) FROM catalog.blueprint
        UNION ALL SELECT 'complex_attachment', count(*) FROM catalog.complex_attachment
        UNION ALL SELECT 'complex_notice', count(*) FROM catalog.complex_notice
        UNION ALL SELECT 'digital_twin_asset', count(*) FROM catalog.digital_twin_asset
        UNION ALL SELECT 'industrial_complex_gold_pointer', count(*) FROM catalog.industrial_complex_gold_pointer
        UNION ALL SELECT 'industry_group', count(*) FROM catalog.industry_group
        UNION ALL SELECT 'spatial_layer', count(*) FROM catalog.spatial_layer
    ) after USING (relation)
    WHERE before.row_count <> after.row_count;
    IF drift IS NOT NULL THEN
        RAISE EXCEPTION 'ADR-0250: row counts changed in %', drift;
    END IF;
    IF (SELECT count(*) FROM catalog.industrial_complex_retired_id)
       <> (SELECT count(*) FROM industrial_complex_rekey) THEN
        RAISE EXCEPTION 'ADR-0250: the retired-id ledger does not hold every retired id';
    END IF;
END;
$counts$;
