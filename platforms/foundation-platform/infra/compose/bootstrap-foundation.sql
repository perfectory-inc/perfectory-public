\set ON_ERROR_STOP on
\getenv foundation_migrator_password FOUNDATION_MIGRATOR_PASSWORD
\getenv foundation_api_password FOUNDATION_API_PASSWORD

SELECT 'CREATE ROLE foundation_migrator'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'foundation_migrator')
\gexec
ALTER ROLE foundation_migrator WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'foundation_migrator_password';

SELECT 'CREATE ROLE foundation_api'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'foundation_api')
\gexec
ALTER ROLE foundation_api WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'foundation_api_password';

REVOKE CONNECT ON DATABASE foundation FROM PUBLIC;
GRANT CONNECT ON DATABASE foundation TO foundation_migrator, foundation_api;
GRANT CREATE ON DATABASE foundation TO foundation_migrator;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO foundation_migrator;

CREATE EXTENSION IF NOT EXISTS postgis;
-- The extension COMMENT lives here (superuser) — the migrator role that runs the
-- sqlx migrations is NOT the extension owner, so it cannot COMMENT/ALTER it
-- (would raise 42501). This is the single privileged home for postgis metadata.
COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';
ALTER SCHEMA catalog OWNER TO foundation_migrator;
CREATE SCHEMA IF NOT EXISTS serving_postgis AUTHORIZATION foundation_migrator;
ALTER SCHEMA serving_postgis OWNER TO foundation_migrator;
GRANT USAGE, CREATE ON SCHEMA catalog, serving_postgis TO foundation_migrator;
