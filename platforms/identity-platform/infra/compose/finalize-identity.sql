\set ON_ERROR_STOP on

SELECT format(
    'REVOKE CREATE ON DATABASE %I FROM identity_migrator',
    current_database()
)
\gexec
REVOKE CREATE ON SCHEMA public FROM identity_migrator;

DO $identity_compose_role_contract$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname IN (
            'identity_migrator',
            'identity_api',
            'identity_policy_worker',
            'identity_provisioner'
        )
          AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR rolreplication OR rolbypassrls)
    ) THEN
        RAISE EXCEPTION 'Identity runtime role hardening contract failed';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace AS namespace
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = namespace.nspowner
        WHERE namespace.nspname = 'identity'
          AND owner.rolname = 'identity_migrator'
    ) THEN
        RAISE EXCEPTION 'Identity schema is not owned by the migrator';
    END IF;
END
$identity_compose_role_contract$;
