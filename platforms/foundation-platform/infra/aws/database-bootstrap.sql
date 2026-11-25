-- The Dagster database's own role (root ADR-0616 A1, least privilege): the Dagster service and every
-- run task connect as it, never as the RDS master user. Run by the bootstrap task as the master user
-- (infra/aws/dagster.tf, docs/runbooks/aws-platform.md), once after the first apply and again after the
-- role's password changes. Idempotent. psql variables: app_user, db_name, app_password (from the role's
-- secret; never written anywhere else).
\set ON_ERROR_STOP on

SELECT format('CREATE ROLE %I LOGIN', :'app_user')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'app_user') \gexec

ALTER ROLE :"app_user" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'app_password';

-- Handing the database to the role needs the master to be able to act as it (PostgreSQL 16+); the
-- master already holds ADMIN OPTION on a role it created.
GRANT :"app_user" TO CURRENT_USER WITH INHERIT FALSE, SET TRUE;
ALTER DATABASE :"db_name" OWNER TO :"app_user";

-- Only the role connects to its database (the master keeps its own rights as the instance's admin).
REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE :"db_name" TO :"app_user";
