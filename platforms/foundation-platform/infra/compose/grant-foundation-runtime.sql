\set ON_ERROR_STOP on

REVOKE ALL ON SCHEMA public FROM foundation_api;
GRANT USAGE ON SCHEMA catalog, serving_postgis TO foundation_api;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA catalog, serving_postgis
    TO foundation_api;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA catalog, serving_postgis
    TO foundation_api;

-- Temporal legal-identity facts are immutable evidence. The API may read them and
-- may call the publisher function, but it cannot rewrite history directly.
REVOKE INSERT, UPDATE, DELETE ON TABLE
    catalog.administrative_unit_identifier,
    catalog.administrative_unit_transition,
    catalog.administrative_unit_parent,
    catalog.parcel_identifier,
    catalog.parcel_administrative_unit
    FROM foundation_api;
GRANT EXECUTE ON FUNCTION catalog.publish_parcel_identifier(uuid, text, date, uuid, uuid, text, uuid)
    TO foundation_api;

ALTER DEFAULT PRIVILEGES FOR ROLE foundation_migrator IN SCHEMA catalog, serving_postgis
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO foundation_api;
ALTER DEFAULT PRIVILEGES FOR ROLE foundation_migrator IN SCHEMA catalog, serving_postgis
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO foundation_api;
