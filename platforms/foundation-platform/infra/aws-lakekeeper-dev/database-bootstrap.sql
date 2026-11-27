-- The Lakekeeper dev rig's two logins and databases on the platform's RDS instance (infra/aws-lakekeeper-dev/
-- database.tf): the catalog's (Lakekeeper) and authz's (OpenFGA). Each login owns its database and nothing else,
-- with a connection limit, so the rig cannot starve Dagster on the shared instance. Run by the bootstrap task as
-- the RDS master user, connected to `postgres`. Idempotent. Names, limits and passwords come from the task's
-- environment (the passwords from their secrets, injected by ECS), read with \getenv so none is on a command line.
-- The same moves as infra/aws/database-bootstrap.sql, which makes Dagster's.
\set ON_ERROR_STOP on
\getenv catalog_user CATALOG_USER
\getenv catalog_db CATALOG_DB
\getenv catalog_limit CATALOG_LIMIT
\getenv catalog_password CATALOG_PASSWORD
\getenv authz_user AUTHZ_USER
\getenv authz_db AUTHZ_DB
\getenv authz_limit AUTHZ_LIMIT
\getenv authz_password AUTHZ_PASSWORD

SELECT format('CREATE ROLE %I LOGIN', :'catalog_user')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'catalog_user') \gexec
SELECT format('CREATE ROLE %I LOGIN', :'authz_user')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'authz_user') \gexec

-- A role this master creates never has SUPERUSER, REPLICATION or BYPASSRLS, and the master may not name them.
ALTER ROLE :"catalog_user" WITH LOGIN NOINHERIT NOCREATEDB NOCREATEROLE CONNECTION LIMIT :catalog_limit PASSWORD :'catalog_password';
ALTER ROLE :"authz_user" WITH LOGIN NOINHERIT NOCREATEDB NOCREATEROLE CONNECTION LIMIT :authz_limit PASSWORD :'authz_password';

-- Handing a database to a role needs the master to be able to act as it (PostgreSQL 16+).
GRANT :"catalog_user" TO CURRENT_USER WITH INHERIT FALSE, SET TRUE;
GRANT :"authz_user" TO CURRENT_USER WITH INHERIT FALSE, SET TRUE;

SELECT format('CREATE DATABASE %I OWNER %I', :'catalog_db', :'catalog_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'catalog_db') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'authz_db', :'authz_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'authz_db') \gexec
ALTER DATABASE :"catalog_db" OWNER TO :"catalog_user";
ALTER DATABASE :"authz_db" OWNER TO :"authz_user";

-- Only its owner connects to each database. The revoke runs as the owner: PUBLIC's default rights are the
-- owner's grant, and a grant is revoked only by its grantor.
SET ROLE :"catalog_user";
REVOKE ALL ON DATABASE :"catalog_db" FROM PUBLIC;
RESET ROLE;
SET ROLE :"authz_user";
REVOKE ALL ON DATABASE :"authz_db" FROM PUBLIC;
RESET ROLE;
