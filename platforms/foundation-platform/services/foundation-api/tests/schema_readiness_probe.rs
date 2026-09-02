//! Proves the schema probe can refuse, not just that it can pass.
//!
//! What real incident does failing this prevent? For six weeks the deployment host held 4 of the
//! 33 migrations this binary shipped with. `/readyz` asked the database only `SELECT 1`, which
//! succeeded the whole time, so the endpoint said ready, the container healthcheck passed, and
//! the API answered 500 to every catalog request (root ADR-0071). The probe now compares applied
//! migrations against the embedded migrator — and these tests plant each way that comparison can
//! come out, including the broken ones, because a check only ever observed passing has not been
//! shown to be able to fail. This week alone, three checks that "passed" turned out to be looking
//! at nothing.

use foundation_api::{probe_schema, SchemaReadiness, MIGRATOR};
use foundation_disposable_database::{run_in_disposable_database, TestResult};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_fully_migrated_database_is_ready() -> TestResult {
    run_in_disposable_database("schema_probe_ready", |pool| async move {
        MIGRATOR.run(&pool).await?;

        let probe = probe_schema(&pool).await;

        assert_eq!(
            probe,
            SchemaReadiness::Ready {
                shipped: MIGRATOR.iter().count()
            },
            "every shipped migration is applied, so the probe must say ready"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_database_missing_one_migration_is_behind() -> TestResult {
    run_in_disposable_database("schema_probe_behind", |pool| async move {
        MIGRATOR.run(&pool).await?;
        // Plant the incident: the ledger says the newest migration never ran. This is exactly
        // what a deployment that skipped `foundation-migrate` looks like to the probe — rows
        // behind the binary. Deleting from the ledger only, not dropping objects, is enough:
        // the probe's question is "does the ledger account for everything I shipped".
        sqlx::query(
            "DELETE FROM _sqlx_migrations
             WHERE version = (SELECT max(version) FROM _sqlx_migrations)",
        )
        .execute(&pool)
        .await?;

        let probe = probe_schema(&pool).await;

        assert_eq!(
            probe,
            SchemaReadiness::Behind {
                shipped: MIGRATOR.iter().count(),
                missing: 1
            },
            "one shipped migration is unaccounted for, so the probe must refuse"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_empty_database_is_behind_by_everything() -> TestResult {
    // The six-week incident in miniature: a database that exists and answers `SELECT 1`
    // but holds none of what this binary needs.
    run_in_disposable_database("schema_probe_empty", |pool| async move {
        let probe = probe_schema(&pool).await;

        assert_eq!(
            probe,
            SchemaReadiness::Behind {
                shipped: MIGRATOR.iter().count(),
                missing: MIGRATOR.iter().count()
            },
            "a reachable but never-migrated database must read as behind, not as ready"
        );
        Ok(())
    })
    .await
}

/// No `PostgreSQL` needed: the pool is lazy and points at a port nobody listens on.
#[tokio::test]
async fn an_unreachable_database_is_unknown_not_ready() -> TestResult {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://nobody@127.0.0.1:1/never")?;

    let probe = probe_schema(&pool).await;

    assert_eq!(
        probe,
        SchemaReadiness::Unknown,
        "an unanswered question must be reported as unknown — never as ready"
    );
    assert!(
        !probe.is_ready(),
        "unknown must not count as ready anywhere a boolean is derived from it"
    );
    Ok(())
}
