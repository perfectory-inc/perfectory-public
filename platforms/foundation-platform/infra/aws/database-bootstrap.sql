-- The Dagster database's own role (root ADR-0616 A1, least privilege): the Dagster service and every
-- run task connect as it, never as the RDS master user. Run by the bootstrap task as the master user
-- (infra/aws/foundation-database.tf, docs/runbooks/aws-platform.md) when its definition or a password's
-- version changes. Idempotent. psql variables: app_user, db_name; the password is the environment's
-- APP_PASSWORD (the role's secret, injected by ECS), read with \getenv so it is on no command line.
\set ON_ERROR_STOP on
\getenv app_password APP_PASSWORD

SELECT format('CREATE ROLE %I LOGIN', :'app_user')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'app_user') \gexec

-- SUPERUSER, REPLICATION and BYPASSRLS may be named only by a role that has them, and the RDS master has none
-- (PostgreSQL 16+ refuses even NOSUPERUSER from it). A role this master creates never has them.
ALTER ROLE :"app_user" WITH LOGIN NOCREATEDB NOCREATEROLE PASSWORD :'app_password';

-- Handing the database to the role needs the master to be able to act as it (PostgreSQL 16+); the
-- master already holds ADMIN OPTION on a role it created.
GRANT :"app_user" TO CURRENT_USER WITH INHERIT FALSE, SET TRUE;
ALTER DATABASE :"db_name" OWNER TO :"app_user";

-- Only the role connects to its database. The revoke runs as the owner: a grant is revoked only by its
-- grantor, and PUBLIC's default rights are the owner's grant. The owner holds every right already.
SET ROLE :"app_user";
REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
RESET ROLE;
