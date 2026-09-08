use super::*;

fn price() -> PriceRow {
    PriceRow {
        pnu: "9999900000100000001".to_owned(),
        dong_name: "101동".to_owned(),
        ho_name: "101호".to_owned(),
        base_year: 2026,
        price_won: 232_000_000,
    }
}

#[test]
fn copy_preserves_labels_without_turning_them_into_columns() -> anyhow::Result<()> {
    let mut row = price();
    row.dong_name = "동\t이름\n\\N".to_owned();
    let line = row.copy_line("fixture")?;
    assert_eq!(line.split('\t').count(), 6);
    assert_eq!(line.lines().count(), 1);
    assert!(line.contains("동\\t이름\\n\\\\N"));
    Ok(())
}

#[test]
fn invalid_identity_year_and_price_are_refused_before_copy() {
    let mut row = price();
    row.pnu = "bad".to_owned();
    assert!(row.copy_line("fixture").is_err());
    row = price();
    row.base_year = 0;
    assert!(row.copy_line("fixture").is_err());
    row = price();
    row.price_won = -1;
    assert!(row.copy_line("fixture").is_err());
}

#[tokio::test]
#[ignore = "requires PostgreSQL with CREATE DATABASE; cargo xtask integration foundation"]
async fn projection_folds_prices_replaces_history_and_reads_through_the_port(
) -> foundation_disposable_database::TestResult {
    use catalog_application::ports::CatalogRepository;
    use catalog_infrastructure::PgCatalogRepository;
    foundation_disposable_database::run_in_disposable_database("unit_price", |pool| async move {
        sqlx::migrate!("../../migrations").run(&pool).await?;
        let mut conn = pool.acquire().await?;
        prepare_stage(&mut conn).await?;
        conn.execute(
            "INSERT INTO unit_official_price_stage VALUES
            ('9999900000100000001','101동','101호',2026,100,'fixture-one'),
            ('9999900000100000001','101동','101호',2026,100,'fixture-one'),
            ('9999900000100000001','101동','101호',2026,200,'fixture-one'),
            ('9999900000100000001','101동','101호',2025,90,'fixture-one'),
            ('9999900000100000002','101동','101호',2026,500,'fixture-one')",
        )
        .await?;
        assert_eq!(promote(&mut conn).await?, (3, 1));
        let repo = PgCatalogRepository::new(pool.clone());
        let pnu = Pnu::parse("9999900000100000001".to_owned())?;
        let prices = repo.list_unit_official_prices_by_pnu(&pnu).await?;
        assert_eq!(prices.len(), 2);
        assert_eq!(
            (prices[0].price.base_year, prices[0].price.price_won),
            (2026, 200)
        );
        conn.execute("TRUNCATE unit_official_price_stage").await?;
        assert!(
            promote(&mut conn).await.is_err(),
            "an empty replacement must roll back"
        );
        assert_eq!(repo.list_unit_official_prices_by_pnu(&pnu).await?.len(), 2);
        conn.execute(
            "INSERT INTO unit_official_price_stage VALUES
            ('9999900000100000001','101동','101호',2026,300,'fixture-two')",
        )
        .await?;
        assert_eq!(promote(&mut conn).await?, (1, 0));
        let prices = repo.list_unit_official_prices_by_pnu(&pnu).await?;
        assert_eq!(
            prices.len(),
            1,
            "a replaced vintage must not retain obsolete annual rows"
        );
        assert_eq!(prices[0].price.price_won, 300);
        Ok(())
    })
    .await
}
