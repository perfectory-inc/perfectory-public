\set ON_ERROR_STOP on

REVOKE ALL ON SCHEMA public FROM foundation_api;

-- The one exception to the revoke above: `/readyz` compares the migration ledger against the
-- list the binary shipped with, because for six weeks the database held 4 of 33 migrations
-- while readiness asked only `SELECT 1` and kept passing (root ADR-0071). Reading the ledger
-- needs USAGE on the schema that holds it plus SELECT on that one table — nothing else in
-- `public` is granted, and USAGE alone opens no other table. Without this, the probe reports
-- `unknown`, `/readyz` refuses, and the healthcheck times every deployment out.
GRANT USAGE ON SCHEMA public TO foundation_api;
GRANT SELECT ON TABLE public._sqlx_migrations TO foundation_api;

GRANT USAGE ON SCHEMA catalog, serving_postgis TO foundation_api;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA catalog, serving_postgis
    TO foundation_api;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA catalog, serving_postgis
    TO foundation_api;

-- Temporal legal-identity facts are immutable evidence. The API may read them and
-- may call the publisher function, but it cannot rewrite history directly.
--
-- The two revision ledgers are on this list for a reason the fact tables are not: their append-only
-- triggers guard UPDATE and DELETE, and `catalog.administrative_boundary_revision`'s guards only
-- those — so before this line the grant above let `foundation_api` INSERT a revision claiming any
-- canonical snapshot, with no evidence behind it and nothing to refuse it.
-- `catalog.publication_revision` additionally carries a BEFORE INSERT capability trigger, because a
-- grant is a role boundary and stops protecting the moment someone adds a role.
REVOKE INSERT, UPDATE, DELETE ON TABLE
    catalog.administrative_unit_identifier,
    catalog.administrative_unit_transition,
    catalog.administrative_unit_parent,
    catalog.parcel_identifier,
    catalog.parcel_administrative_unit,
    catalog.administrative_boundary_revision,
    catalog.publication_revision,
    catalog.parcel_publication_source_evidence,
    serving_postgis.parcel_boundary_mirror_rebuild_run,
    serving_postgis.parcel_boundary_mirror,
    serving_postgis.spatial_projection_load,
    serving_postgis.parcel_boundary_publication
    FROM foundation_api;
GRANT EXECUTE ON FUNCTION catalog.publish_parcel_identifier(uuid, text, date, uuid, uuid, text, uuid)
    TO foundation_api;

ALTER DEFAULT PRIVILEGES FOR ROLE foundation_migrator IN SCHEMA catalog, serving_postgis
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO foundation_api;
ALTER DEFAULT PRIVILEGES FOR ROLE foundation_migrator IN SCHEMA catalog, serving_postgis
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO foundation_api;
