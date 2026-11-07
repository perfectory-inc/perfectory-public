//! Every query `/metrics` runs is accepted by the migrated schema (root ADR-0443).
//!
//! What real incident does failing this prevent? The ingestion query read
//! `catalog.bronze_object.payload_size_bytes`; the column is `size_bytes`. `/metrics` turns a failed
//! query into no rows, so from its first day no `foundation_platform_ingestion_run_*` series existed
//! and the stale, failed and slow ingestion alerts could never fire (2026-11-07 readiness sweep, root
//! ADR-0442 §9). PostgreSQL resolves every column when it parses a query, so running each one on
//! the empty migrated database is enough to refuse a wrong name in CI.

use foundation_api::{METRICS_QUERIES, MIGRATOR};
use foundation_disposable_database::{run_in_disposable_database, TestResult};

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn every_metrics_query_runs_on_the_migrated_schema() -> TestResult {
    run_in_disposable_database("metrics_queries_schema", |pool| async move {
        MIGRATOR.run(&pool).await?;
        for (name, sql) in METRICS_QUERIES {
            if let Err(error) = sqlx::query(sql).fetch_all(&pool).await {
                panic!("the {name} metrics query does not run on the migrated schema: {error}");
            }
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn the_incidents_column_is_refused() -> TestResult {
    run_in_disposable_database("metrics_queries_refuse", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_, ingestion) = METRICS_QUERIES
            .into_iter()
            .find(|(name, _)| *name == "ingestion runs")
            .ok_or("no ingestion runs query")?;
        // Plant the incident: the column the query named before the fix.
        let planted = ingestion.replace("SUM(size_bytes)", "SUM(payload_size_bytes)");
        assert_ne!(
            planted, ingestion,
            "the planted query must differ from the real one"
        );
        assert!(
            sqlx::query(&planted).fetch_all(&pool).await.is_err(),
            "a query naming a column the schema lacks must fail, or this test proves nothing"
        );
        Ok(())
    })
    .await
}
