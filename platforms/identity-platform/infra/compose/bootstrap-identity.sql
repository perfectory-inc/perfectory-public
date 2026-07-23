\set ON_ERROR_STOP on
\getenv identity_migrator_password IDENTITY_MIGRATOR_PASSWORD
\getenv identity_api_password IDENTITY_API_PASSWORD
\getenv identity_policy_worker_password IDENTITY_POLICY_WORKER_PASSWORD
\getenv identity_provisioner_password IDENTITY_PROVISIONER_PASSWORD

SELECT 'CREATE ROLE identity_migrator'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'identity_migrator')
\gexec
ALTER ROLE identity_migrator WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'identity_migrator_password';

SELECT 'CREATE ROLE identity_api'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'identity_api')
\gexec
ALTER ROLE identity_api WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'identity_api_password';

SELECT 'CREATE ROLE identity_policy_worker'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'identity_policy_worker')
\gexec
ALTER ROLE identity_policy_worker WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'identity_policy_worker_password';

SELECT 'CREATE ROLE identity_provisioner'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'identity_provisioner')
\gexec
ALTER ROLE identity_provisioner WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'identity_provisioner_password';

SELECT 'CREATE ROLE identity_recovery'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'identity_recovery')
\gexec
ALTER ROLE identity_recovery WITH NOLOGIN NOSUPERUSER NOCREATEDB CREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS;

SELECT 'CREATE ROLE identity_operations_admin'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'identity_operations_admin')
\gexec
ALTER ROLE identity_operations_admin WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS;

CREATE TEMP TABLE identity_database_connect_allowlist (
    role_name name NOT NULL PRIMARY KEY,
    migration_create boolean NOT NULL
) ON COMMIT PRESERVE ROWS;
INSERT INTO identity_database_connect_allowlist (role_name, migration_create) VALUES
    ('identity_migrator', true),
    ('identity_api', false),
    ('identity_policy_worker', false),
    ('identity_provisioner', false),
    ('identity_recovery', false),
    ('identity_operations_admin', false);
\i /workspace/normalize-identity-database-acl.sql
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO identity_migrator;
