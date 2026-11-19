\set ON_ERROR_STOP on
\getenv foundation_migrator_password FOUNDATION_MIGRATOR_PASSWORD
\getenv foundation_api_password FOUNDATION_API_PASSWORD
\getenv dawneer_session_password DAWNEER_SESSION_DATABASE_PASSWORD

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

-- The role the audit importer writes its sources as (root ADR-0569 §3, §6). `audit.append` takes a
-- source only from the role listed as its writer, so the importer cannot write the console's events
-- and the API cannot write a deploy. It cannot log in until the importer is deployed with its own
-- credential (ADR-0569 stage 4); until then it exists so the grants below name a real role.
SELECT 'CREATE ROLE foundation_audit_importer'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'foundation_audit_importer')
\gexec
ALTER ROLE foundation_audit_importer WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS;

-- The console's staff sessions (root ADR-0116 §7, tower-sessions-sqlx-store 0.16): one role that
-- owns one schema and its one table, and nothing else. The store's `migrate()` would run
-- `CREATE SCHEMA IF NOT EXISTS`, which Postgres refuses without CREATE on the database even when the
-- schema exists (measured on postgres:17, 2026-11-19), so the table is made here in the store's
-- shape and the console never migrates. Its password is generated on the host by the release
-- (runtime-secrets contract `generated`); where it is not set (the staging smoke) the role cannot
-- log in.
SELECT 'CREATE ROLE dawneer_session'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dawneer_session')
\gexec
\if :{?dawneer_session_password}
ALTER ROLE dawneer_session WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'dawneer_session_password';
\else
ALTER ROLE dawneer_session WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS;
\endif
CREATE SCHEMA IF NOT EXISTS tower_sessions AUTHORIZATION dawneer_session;
CREATE TABLE IF NOT EXISTS tower_sessions.session (
    id          text        PRIMARY KEY NOT NULL,
    data        bytea       NOT NULL,
    expiry_date timestamptz NOT NULL
);
ALTER TABLE tower_sessions.session OWNER TO dawneer_session;

-- The database psql is connected to (PGDATABASE): `foundation` under compose, `foundation_staging`
-- when the staging smoke bootstraps its own (root ADR-0177). One file, whichever database it is.
REVOKE CONNECT ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"DBNAME" TO foundation_migrator, foundation_api, foundation_audit_importer,
    dawneer_session;
GRANT CREATE ON DATABASE :"DBNAME" TO foundation_migrator;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO foundation_migrator;

CREATE EXTENSION IF NOT EXISTS postgis;
-- The extension COMMENT lives here (superuser) — the migrator role that runs the
-- sqlx migrations is NOT the extension owner, so it cannot COMMENT/ALTER it
-- (would raise 42501). This is the single privileged home for postgis metadata.
COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';
-- Created here, not only altered. This step runs before the migrations that would create
-- it (`20260719000001` line 32), so on a database that has never been migrated the ALTER
-- below had nothing to alter and bootstrap exited 3 — measured 2026-09-01, which is to say
-- this stack could not be stood up from empty. It went unseen because the deployment host
-- was restored from a dump that already had the schema. `serving_postgis` on the next line
-- was always created this way; `catalog` was the asymmetric one.
CREATE SCHEMA IF NOT EXISTS catalog AUTHORIZATION foundation_migrator;
ALTER SCHEMA catalog OWNER TO foundation_migrator;
CREATE SCHEMA IF NOT EXISTS serving_postgis AUTHORIZATION foundation_migrator;
ALTER SCHEMA serving_postgis OWNER TO foundation_migrator;
GRANT USAGE, CREATE ON SCHEMA catalog, serving_postgis TO foundation_migrator;
