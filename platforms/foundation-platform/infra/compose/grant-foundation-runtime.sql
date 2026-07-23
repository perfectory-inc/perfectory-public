\set ON_ERROR_STOP on

REVOKE ALL ON SCHEMA public FROM foundation_api;
GRANT USAGE ON SCHEMA catalog, serving_postgis TO foundation_api;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA catalog, serving_postgis
    TO foundation_api;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA catalog, serving_postgis
    TO foundation_api;

ALTER DEFAULT PRIVILEGES FOR ROLE foundation_migrator IN SCHEMA catalog, serving_postgis
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO foundation_api;
ALTER DEFAULT PRIVILEGES FOR ROLE foundation_migrator IN SCHEMA catalog, serving_postgis
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO foundation_api;
