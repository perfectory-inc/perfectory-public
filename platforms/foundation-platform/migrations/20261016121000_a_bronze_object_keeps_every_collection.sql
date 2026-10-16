-- A re-collected Bronze object keeps the registry row it replaced (ADR-0264).
--
-- `catalog.bronze_object` holds one row per (source_catalog_id, dedupe_key). Re-ingesting the same
-- dedupe key rewrites that row in place: object_key, checksum_sha256, collected_at,
-- ingestion_run_id and provider_file_* take the new run's values, and the earlier collection
-- survives only as an `orphan_candidate` outbox event that names the displaced key and nothing
-- else. Every reader expects the current row per key, so the grain stays; the replaced row is
-- copied here by the database itself, whichever statement replaced it.

CREATE TABLE catalog.bronze_object_version (
    id bigint GENERATED ALWAYS AS IDENTITY,
    bronze_object_id uuid NOT NULL,
    source_catalog_id uuid NOT NULL,
    dedupe_key text NOT NULL,
    change text NOT NULL,
    superseded_at timestamptz NOT NULL DEFAULT now(),
    -- The whole replaced row, so a column added to bronze_object later is kept without a change here.
    prior_row jsonb NOT NULL,
    CONSTRAINT bronze_object_version_pkey PRIMARY KEY (id),
    CONSTRAINT bronze_object_version_change_check CHECK (change IN ('update', 'delete')),
    CONSTRAINT bronze_object_version_prior_row_check CHECK (jsonb_typeof(prior_row) = 'object')
);

CREATE INDEX bronze_object_version_key_idx
    ON catalog.bronze_object_version (source_catalog_id, dedupe_key, superseded_at);

CREATE INDEX bronze_object_version_object_idx
    ON catalog.bronze_object_version (bronze_object_id);

CREATE FUNCTION catalog.keep_replaced_bronze_object()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NULL;
    END IF;
    INSERT INTO catalog.bronze_object_version
        (bronze_object_id, source_catalog_id, dedupe_key, change, prior_row)
    VALUES (OLD.id, OLD.source_catalog_id, OLD.dedupe_key, lower(TG_OP), to_jsonb(OLD));
    RETURN NULL;
END;
$function$;

CREATE TRIGGER bronze_object_keeps_replaced_rows
AFTER UPDATE OR DELETE ON catalog.bronze_object
FOR EACH ROW EXECUTE FUNCTION catalog.keep_replaced_bronze_object();

CREATE TRIGGER bronze_object_version_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog.bronze_object_version
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_bronze_object_member_mutation();

COMMENT ON TABLE catalog.bronze_object_version IS
    'Append-only copy of every catalog.bronze_object row a re-collection or deletion replaced; the registry keeps the current row per dedupe key (ADR-0264).';
