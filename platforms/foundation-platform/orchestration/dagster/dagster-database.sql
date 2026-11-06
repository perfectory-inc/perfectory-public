-- The Dagster instance's own role and database on the platform's Postgres (root ADR-0333 stage 0).
-- airflow-runtime.sh runs this as the database's administrator, with the role's password in the psql
-- variable `password` (from its runtime env file; never on a command line). Idempotent: a second run
-- only sets the password again. The role owns its database and nothing else; the foundation database
-- already refuses CONNECT to PUBLIC (infra/compose/bootstrap-foundation.sql).
\set ON_ERROR_STOP on
SELECT format('CREATE ROLE dagster LOGIN PASSWORD %L', :'password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dagster') \gexec
ALTER ROLE dagster WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'password';
SELECT 'CREATE DATABASE dagster OWNER dagster'
 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster') \gexec
REVOKE ALL ON DATABASE dagster FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE dagster TO dagster;
