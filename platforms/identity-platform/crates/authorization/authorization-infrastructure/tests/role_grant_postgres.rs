//! Live `PostgreSQL` contract for least-privilege atomic role assignment.

use std::env;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

use authorization_application::ports::{RoleGrantPersistenceError, RoleGrantUnitOfWork};
use authorization_domain::RoleCode;
use authorization_infrastructure::PgRoleGrantUnitOfWork;
use identity_shared_kernel::StaffId;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("../../../migrations");

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with role and database creation privileges"]
async fn api_role_assigns_atomically_without_staff_update_privilege() -> TestResult {
    let base_url = env::var("IDENTITY_ROLE_GRANT_TEST_DATABASE_URL")?;
    let suffix = Uuid::new_v4().simple().to_string();
    let database = format!("identity_role_grant_{suffix}");
    let api_role = format!("identity_api_{suffix}");
    let worker_role = format!("identity_worker_{suffix}");
    let provisioner_role = format!("identity_provisioner_{suffix}");
    let password = format!("role_grant_{suffix}");
    let base_options = PgConnectOptions::from_str(&base_url)?;
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base_options.clone())
        .await?;

    for role in [&api_role, &worker_role, &provisioner_role] {
        admin_pool
            .execute(
                format!(
                    "CREATE ROLE \"{role}\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE \
                     NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
                )
                .as_str(),
            )
            .await?;
    }
    admin_pool
        .execute(format!("CREATE DATABASE \"{database}\"").as_str())
        .await?;

    let result = run_contract(
        &base_options,
        &database,
        &api_role,
        &worker_role,
        &provisioner_role,
        &password,
    )
    .await;

    admin_pool
        .execute(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = '{database}' AND pid <> pg_backend_pid()"
            )
            .as_str(),
        )
        .await?;
    admin_pool
        .execute(format!("DROP DATABASE \"{database}\"").as_str())
        .await?;
    for role in [&api_role, &worker_role, &provisioner_role] {
        admin_pool
            .execute(format!("DROP ROLE \"{role}\"").as_str())
            .await?;
    }
    admin_pool.close().await;

    result
}

async fn run_contract(
    base_options: &PgConnectOptions,
    database: &str,
    api_role: &str,
    worker_role: &str,
    provisioner_role: &str,
    password: &str,
) -> TestResult {
    let database_options = base_options.clone().database(database);
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(database_options.clone())
        .await?;
    MIGRATOR.run(&admin_pool).await?;
    let legacy_grant = format!("GRANT UPDATE ON identity.staff TO \"{api_role}\"");
    sqlx::query(&legacy_grant).execute(&admin_pool).await?;
    assert!(has_staff_update_privilege(&admin_pool, api_role).await?);
    apply_runtime_grants(&admin_pool, api_role, worker_role, provisioner_role).await?;
    assert!(!has_staff_update_privilege(&admin_pool, api_role).await?);

    let grantor = StaffId::new(Uuid::now_v7());
    let target = StaffId::new(Uuid::now_v7());
    seed_staff(&admin_pool, grantor, target).await?;

    let api_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            database_options
                .clone()
                .username(api_role)
                .password(password),
        )
        .await?;
    assert_assignment_contract(&admin_pool, &api_pool, grantor, target).await?;
    assert_profile_updates_denied(&api_pool, target).await?;

    api_pool.close().await;
    admin_pool.close().await;
    Ok(())
}

async fn has_staff_update_privilege(pool: &PgPool, role: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT has_table_privilege($1, 'identity.staff', 'UPDATE')")
        .bind(role)
        .fetch_one(pool)
        .await
}

async fn seed_staff(pool: &PgPool, grantor: StaffId, target: StaffId) -> TestResult {
    for (staff_id, subject, email, role) in [
        (grantor, "grantor", "grantor@example.test", "MASTER_ADMIN"),
        (target, "target", "target@example.test", "CATALOG_ADMIN"),
    ] {
        sqlx::query(
            "INSERT INTO identity.staff \
             (id, zitadel_subject, email, display_name, primary_role_code) \
             VALUES ($1, $2, $3, 'Role grant contract', $4)",
        )
        .bind(staff_id.as_uuid())
        .bind(format!("{subject}-{}", staff_id.as_uuid()))
        .bind(email)
        .bind(role)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn assert_assignment_contract(
    admin_pool: &PgPool,
    api_pool: &PgPool,
    grantor: StaffId,
    target: StaffId,
) -> TestResult {
    let unit_of_work = Arc::new(PgRoleGrantUnitOfWork::new(api_pool.clone()));
    let role = RoleCode::parse("LAKEHOUSE_ADMIN")?;
    let (first, second) = tokio::join!(
        unit_of_work.assign_role(target, &role, grantor),
        unit_of_work.assign_role(target, &role, grantor),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        usize::from(matches!(
            first,
            Err(RoleGrantPersistenceError::DuplicateRole)
        )) + usize::from(matches!(
            second,
            Err(RoleGrantPersistenceError::DuplicateRole)
        )),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM identity.staff_role WHERE staff_id = $1 AND role_code = $2",
        )
        .bind(target.as_uuid())
        .bind(role.as_str())
        .fetch_one(admin_pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM identity.outbox_event \
             WHERE type = 'identity.staff.role_assigned.v1' \
               AND payload ->> 'staff_id' = $1",
        )
        .bind(target.as_uuid().to_string())
        .fetch_one(admin_pool)
        .await?,
        1
    );

    let missing = StaffId::new(Uuid::now_v7());
    assert!(matches!(
        unit_of_work.assign_role(missing, &role, grantor).await,
        Err(RoleGrantPersistenceError::StaffNotFound(id))
            if id == missing.as_uuid().to_string()
    ));
    Ok(())
}

async fn assert_profile_updates_denied(api_pool: &PgPool, target: StaffId) -> TestResult {
    for statement in [
        "UPDATE identity.staff SET email = 'changed@example.test' WHERE id = $1",
        "UPDATE identity.staff SET display_name = 'Changed' WHERE id = $1",
        "UPDATE identity.staff SET primary_role_code = 'MASTER_ADMIN' WHERE id = $1",
        "UPDATE identity.staff SET version = version + 1 WHERE id = $1",
    ] {
        let Err(error) = sqlx::query(statement)
            .bind(target.as_uuid())
            .execute(api_pool)
            .await
        else {
            return Err(std::io::Error::other(
                "identity_api unexpectedly updated a staff profile field",
            )
            .into());
        };
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("42501")
        );
    }
    Ok(())
}

async fn apply_runtime_grants(
    pool: &PgPool,
    api_role: &str,
    worker_role: &str,
    provisioner_role: &str,
) -> TestResult {
    // The grants file uses psql `:"var"` identifier placeholders (one per runtime
    // role). sqlx::raw_sql does NOT interpolate psql variables, so EVERY placeholder
    // must be substituted here; a leftover `:"..."` reaches Postgres verbatim and
    // fails with 42601 (syntax error at or near ":"). Keep this in lockstep with the
    // role set in grant-identity-runtime-access.sql.
    let grants = include_str!("../../../../infra/compose/grant-identity-runtime-access.sql")
        .replace(":\"identity_api_role\"", format!("\"{api_role}\"").as_str())
        .replace(
            ":\"identity_policy_worker_role\"",
            format!("\"{worker_role}\"").as_str(),
        )
        .replace(
            ":\"identity_provisioner_role\"",
            format!("\"{provisioner_role}\"").as_str(),
        );
    sqlx::raw_sql(&grants).execute(pool).await?;
    Ok(())
}
