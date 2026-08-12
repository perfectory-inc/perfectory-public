-- Makes ADR-0025's evidence append boundary an explicit, transaction-local capability and freezes
-- every terminal tuple that can otherwise be edited after a successful seal.

CREATE OR REPLACE FUNCTION catalog.require_parcel_publication_evidence_sealer()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, catalog, pg_temp
AS $function$
BEGIN
    IF current_setting('foundation.parcel_publication_evidence_sealer', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'parcel publication evidence requires the evidence sealer capability'
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER parcel_publication_source_evidence_capability
BEFORE INSERT ON catalog.parcel_publication_source_evidence
FOR EACH ROW EXECUTE FUNCTION catalog.require_parcel_publication_evidence_sealer();

-- A run is born without claims. Its existing state machine is the only path to a terminal tuple;
-- direct INSERT of running/succeeded rows would otherwise bypass every transition check.
CREATE OR REPLACE FUNCTION serving_postgis.protect_parcel_boundary_mirror_rebuild_run()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'planned'
           OR NEW.loaded_row_count <> 0
           OR NEW.rejected_row_count <> 0
           OR NEW.finished_at IS NOT NULL
           OR NEW.error_message IS NOT NULL
        THEN
            RAISE EXCEPTION 'parcel mirror rebuild run must be inserted as planned'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.status IN ('succeeded', 'failed', 'cancelled') THEN
        RAISE EXCEPTION 'terminal parcel mirror rebuild run is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'UPDATE'
       AND NEW.status IS DISTINCT FROM OLD.status
       AND NOT (
           (OLD.status = 'planned' AND NEW.status IN ('running', 'failed', 'cancelled'))
           OR (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed', 'cancelled'))
       )
    THEN
        RAISE EXCEPTION 'invalid parcel mirror rebuild run state transition: % -> %',
            OLD.status, NEW.status
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

DROP TRIGGER parcel_boundary_mirror_rebuild_run_state_guard
    ON serving_postgis.parcel_boundary_mirror_rebuild_run;
CREATE TRIGGER parcel_boundary_mirror_rebuild_run_state_guard
BEFORE INSERT OR UPDATE OR DELETE ON serving_postgis.parcel_boundary_mirror_rebuild_run
FOR EACH ROW EXECUTE FUNCTION serving_postgis.protect_parcel_boundary_mirror_rebuild_run();

CREATE OR REPLACE FUNCTION serving_postgis.protect_terminal_spatial_projection_load()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, pg_temp
AS $function$
BEGIN
    IF OLD.status IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'terminal spatial projection load tuple is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER spatial_projection_load_terminal_tuple_guard
BEFORE UPDATE OR DELETE ON serving_postgis.spatial_projection_load
FOR EACH ROW EXECUTE FUNCTION serving_postgis.protect_terminal_spatial_projection_load();

CREATE OR REPLACE FUNCTION serving_postgis.reject_parcel_boundary_publication_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'parcel boundary publication rows of terminal loads are append-only'
            USING ERRCODE = '55000';
    END IF;

    PERFORM 1
      FROM serving_postgis.spatial_projection_load AS load
     WHERE load.id = OLD.projection_load_id
       AND load.status IN ('succeeded', 'failed')
     FOR UPDATE;
    IF FOUND THEN
        RAISE EXCEPTION 'parcel boundary publication rows of terminal loads are append-only'
            USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.projection_load_id IS DISTINCT FROM OLD.projection_load_id THEN
        PERFORM 1
          FROM serving_postgis.spatial_projection_load AS load
         WHERE load.id = NEW.projection_load_id
           AND load.status IN ('succeeded', 'failed')
         FOR UPDATE;
        IF FOUND THEN
            RAISE EXCEPTION 'parcel boundary publication rows of terminal loads are append-only'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER parcel_boundary_publication_terminal_load_guard
BEFORE UPDATE OR DELETE ON serving_postgis.parcel_boundary_publication
FOR EACH ROW EXECUTE FUNCTION serving_postgis.reject_parcel_boundary_publication_mutation();

CREATE TRIGGER parcel_boundary_publication_truncate_guard
BEFORE TRUNCATE ON serving_postgis.parcel_boundary_publication
FOR EACH STATEMENT EXECUTE FUNCTION serving_postgis.reject_parcel_boundary_publication_mutation();
