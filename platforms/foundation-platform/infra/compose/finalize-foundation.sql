\set ON_ERROR_STOP on

REVOKE CREATE ON DATABASE foundation FROM foundation_migrator;
REVOKE CREATE ON SCHEMA public FROM foundation_migrator;

DO $foundation_compose_role_contract$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname IN ('foundation_migrator', 'foundation_api')
          AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR rolreplication OR rolbypassrls)
    ) THEN
        RAISE EXCEPTION 'Foundation runtime role hardening contract failed';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace AS namespace
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = namespace.nspowner
        WHERE namespace.nspname = 'catalog'
          AND owner.rolname = 'foundation_migrator'
    ) THEN
        RAISE EXCEPTION 'Foundation Catalog schema is not owned by the migrator';
    END IF;
END
$foundation_compose_role_contract$;
