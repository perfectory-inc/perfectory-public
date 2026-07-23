DO $identity_database_acl$
DECLARE
    role_count bigint;
    marked_migrator_count bigint;
    null_count bigint;
    duplicate_count bigint;
    missing_role_count bigint;
    migration_role name;
    unauthorized_role name;
    allowed_role name;
BEGIN
    SELECT count(*),
           count(*) FILTER (WHERE migration_create),
           count(*) FILTER (WHERE role_name IS NULL OR migration_create IS NULL),
           count(*) - count(DISTINCT role_name)
    INTO role_count, marked_migrator_count, null_count, duplicate_count
    FROM identity_database_connect_allowlist;

    SELECT count(*)
    INTO missing_role_count
    FROM identity_database_connect_allowlist AS allowed
    LEFT JOIN pg_catalog.pg_roles AS role ON role.rolname = allowed.role_name
    WHERE role.oid IS NULL;

    IF role_count <> 6
       OR marked_migrator_count <> 1
       OR null_count <> 0
       OR duplicate_count <> 0
       OR missing_role_count <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = 'check_violation',
            MESSAGE = 'identity database ACL allowlist is invalid';
    END IF;

    SELECT role_name
    INTO STRICT migration_role
    FROM identity_database_connect_allowlist
    WHERE migration_create;

    FOR unauthorized_role IN
        SELECT DISTINCT grantee.rolname
        FROM pg_catalog.pg_database AS database
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
        ) AS privilege
        JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = privilege.grantee
        LEFT JOIN identity_database_connect_allowlist AS allowed
          ON allowed.role_name = grantee.rolname
        WHERE database.datname = current_database()
          AND privilege.grantee <> database.datdba
          AND privilege.privilege_type IN ('CONNECT', 'CREATE', 'TEMPORARY')
          AND allowed.role_name IS NULL
    LOOP
        EXECUTE format(
            'REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE %I FROM %I',
            current_database(), unauthorized_role
        );
    END LOOP;

    EXECUTE format(
        'REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC',
        current_database()
    );

    FOR allowed_role IN SELECT role_name FROM identity_database_connect_allowlist
    LOOP
        EXECUTE format(
            'REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE %I FROM %I',
            current_database(), allowed_role
        );
        EXECUTE format(
            'GRANT CONNECT ON DATABASE %I TO %I', current_database(), allowed_role
        );
    END LOOP;

    EXECUTE format(
        'GRANT CREATE ON DATABASE %I TO %I', current_database(), migration_role
    );
END
$identity_database_acl$;
