//! `PostgreSQL` coverage of the lossless reference-date projection and migration.

#[path = "../src/unit_official_price_publication.rs"]
mod publication;

use foundation_shared_kernel::pnu::Pnu;
use publication::{prepare_stage, promote};
use sqlx::Executor;

#[tokio::test]
#[ignore = "requires PostgreSQL with CREATE DATABASE; cargo xtask integration foundation"]
async fn reference_date_migration_applies_after_the_complete_migration_chain(
) -> foundation_disposable_database::TestResult {
    foundation_disposable_database::run_in_disposable_database(
        "unit_price_schema",
        |pool| async move {
            sqlx::migrate!("../../migrations").run(&pool).await?;
            let columns: Vec<String> = sqlx::query_scalar(
                "SELECT column_name FROM information_schema.columns
             WHERE table_schema = 'catalog' AND table_name = 'unit_official_price'",
            )
            .fetch_all(&pool)
            .await?;
            assert!(columns.iter().any(|name| name == "base_date"));
            assert!(!columns.iter().any(|name| name == "base_year"));
            Ok(())
        },
    )
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL with CREATE DATABASE; cargo xtask integration foundation"]
async fn projection_preserves_reference_dates_batches_and_legacy_rows(
) -> foundation_disposable_database::TestResult {
    use catalog_application::ports::CatalogRepository;
    use catalog_infrastructure::PgCatalogRepository;
    foundation_disposable_database::run_in_disposable_database("unit_price", |pool| async move {
        let mut conn = pool.acquire().await?;
        conn.execute("CREATE SCHEMA catalog").await?;
        conn.execute(include_str!("../../../migrations/20260908010000_unit_official_price_projection.sql")).await?;
        conn.execute("INSERT INTO catalog.unit_official_price VALUES
            ('9999900000100000001','101동','101호',2010,36000000,'legacy')").await?;
        conn.execute(include_str!("../../../migrations/20260911010000_unit_official_price_reference_dates.sql")).await?;
        let legacy: (i16, i64) = sqlx::query_as(
            "SELECT base_year,price_won FROM catalog.unit_official_price_legacy_year",
        ).fetch_one(&mut *conn).await?;
        assert_eq!(legacy, (2010, 36_000_000));
        prepare_stage(&mut conn).await?;
        conn.execute(
            "INSERT INTO unit_official_price_stage VALUES
            ('9999900000100000001','101동','101호','20100101',36000000,'fixture-one'),
            ('9999900000100000001','101동','101호','20100601',35000000,'fixture-one'),
            ('9999900000100000002','101동','101호','20100601',500,'fixture-one')",
        )
        .await?;
        assert_eq!(promote(&mut conn).await?, (3, 3));
        assert_eq!(promote(&mut conn).await?, (0, 3), "replay keeps immutable rows");
        let repo = PgCatalogRepository::new(pool.clone());
        let pnu = Pnu::parse("9999900000100000001".to_owned())?;
        let prices = repo.list_unit_official_prices_by_pnu(&pnu).await?;
        assert_eq!(prices.len(), 2);
        assert_eq!(
            (prices[0].price.base_date.as_str(), prices[0].price.price_won),
            ("20100601", 35_000_000)
        );
        assert_eq!(prices[1].price.base_date, "20100101");
        // A changed or incomplete retry cannot reuse the immutable source ID.
        conn.execute("UPDATE unit_official_price_stage SET price_won = 1 WHERE base_date = '20100101'").await?;
        assert!(promote(&mut conn).await.is_err());
        conn.execute("DELETE FROM unit_official_price_stage WHERE base_date = '20100101'").await?;
        assert!(promote(&mut conn).await.is_err());
        conn.execute("TRUNCATE unit_official_price_stage").await?;
        assert!(
            promote(&mut conn).await.is_err(),
            "an empty publication must roll back"
        );
        assert_eq!(repo.list_unit_official_prices_by_pnu(&pnu).await?.len(), 2);
        conn.execute(
            "INSERT INTO unit_official_price_stage VALUES
            ('9999900000100000001','101동','101호','20100101',36000000,'fixture-two'),
            ('9999900000100000001','101동','101호','20100601',34000000,'fixture-two')",
        )
        .await?;
        assert_eq!(promote(&mut conn).await?, (2, 2));
        let prices = repo.list_unit_official_prices_by_pnu(&pnu).await?;
        assert_eq!(
            prices.len(),
            2,
            "the selected complete source retains both reference dates"
        );
        assert_eq!(prices[0].price.price_won, 34_000_000);
        let retained: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM catalog.unit_official_price").fetch_one(&mut *conn).await?;
        assert_eq!(retained, 5, "old source batches are never deleted");
        conn.execute("TRUNCATE unit_official_price_stage").await?;
        conn.execute("INSERT INTO unit_official_price_stage SELECT * FROM catalog.unit_official_price WHERE source_snapshot_id = 'fixture-one'").await?;
        assert_eq!(promote(&mut conn).await?, (0, 3));
        assert_eq!(repo.list_unit_official_prices_by_pnu(&pnu).await?[0].price.price_won, 35_000_000);
        conn.execute("INSERT INTO unit_official_price_stage SELECT * FROM unit_official_price_stage LIMIT 1").await?;
        assert!(promote(&mut conn).await.is_err(), "same-date duplicates are never folded");
        for table in ["unit_official_price", "unit_official_price_publication", "unit_official_price_legacy_year"] {
            for mutation in [format!("UPDATE catalog.{table} SET source_snapshot_id = source_snapshot_id"),
                             format!("DELETE FROM catalog.{table}"), format!("TRUNCATE catalog.{table}")] {
                assert!(conn.execute(mutation.as_str()).await.is_err(), "{mutation}");
            }
        }
        Ok(())
    })
    .await
}
