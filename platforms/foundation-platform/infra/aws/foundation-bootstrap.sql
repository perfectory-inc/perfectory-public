-- The foundation database on RDS (root ADR-0616 A1b): the Bronze ledger's place, with the schema of the
-- data host's `foundation`, made by the same sqlx migrations (the rollout's migrate task runs next).
-- The roles and schemas are the ones infra/compose/bootstrap-foundation.sql makes and the migrations and
-- infra/compose/grant-foundation-runtime.sql name. The console's session role is not here: AWS runs no
-- console. SUPERUSER, REPLICATION and BYPASSRLS are left out: the RDS master may not name them, a role it
-- creates never has them, and finalize-foundation.sql checks that. Run by the rollout's bootstrap task
-- as the master user, connected to `postgres`, before every migration (infra/aws/pre-deploy.sh).
-- Idempotent. psql variables: db_name, migrator_password, runtime_password (from their secrets).
\set ON_ERROR_STOP on

SELECT format('CREATE DATABASE %I', :'db_name')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'db_name') \gexec

SELECT 'CREATE ROLE foundation_migrator'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'foundation_migrator') \gexec
ALTER ROLE foundation_migrator WITH LOGIN NOINHERIT NOCREATEDB NOCREATEROLE PASSWORD :'migrator_password';

-- The runtime role, under the name the grants give it (on the data host the collectors connect as it too).
SELECT 'CREATE ROLE foundation_api'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'foundation_api') \gexec
ALTER ROLE foundation_api WITH LOGIN NOINHERIT NOCREATEDB NOCREATEROLE PASSWORD :'runtime_password';

-- Named by the audit migration and the grants; it cannot log in (root ADR-0569 §6).
SELECT 'CREATE ROLE foundation_audit_importer'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'foundation_audit_importer') \gexec
ALTER ROLE foundation_audit_importer WITH NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE;

-- Making a schema the migrator owns needs the master to be able to act as it (PostgreSQL 16+).
GRANT foundation_migrator TO CURRENT_USER WITH INHERIT FALSE, SET TRUE;

\connect :"db_name"
\set ON_ERROR_STOP on
-- The three roles get what PUBLIC has on the data host, where only CONNECT is revoked from PUBLIC:
-- migrations and the runtime make temporary tables (20261015090000 does), so TEMPORARY too.
REVOKE CONNECT, TEMPORARY ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE :"db_name" TO foundation_migrator, foundation_api, foundation_audit_importer;
-- The migrations create schemas and trusted extensions; finalize-foundation.sql takes CREATE back.
GRANT CREATE ON DATABASE :"db_name" TO foundation_migrator;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO foundation_migrator;
-- PostGIS is not a trusted extension: it is made here, as on the data host, and the migration's
-- CREATE EXTENSION IF NOT EXISTS finds it.
CREATE EXTENSION IF NOT EXISTS postgis;
COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';
CREATE SCHEMA IF NOT EXISTS catalog AUTHORIZATION foundation_migrator;
ALTER SCHEMA catalog OWNER TO foundation_migrator;
CREATE SCHEMA IF NOT EXISTS serving_postgis AUTHORIZATION foundation_migrator;
ALTER SCHEMA serving_postgis OWNER TO foundation_migrator;
GRANT USAGE, CREATE ON SCHEMA catalog, serving_postgis TO foundation_migrator;
