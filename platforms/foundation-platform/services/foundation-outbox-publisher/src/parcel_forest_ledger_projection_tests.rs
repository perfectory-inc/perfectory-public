use super::*;

fn contract() -> SourceContract {
    serde_json::from_str(include_str!(
        "../../../infra/lakehouse/contracts/vworld-land-forest-source-objects.json"
    ))
    .expect("measured AL_D003 CSV inventory")
}

#[test]
fn national_selection_requires_the_newest_complete_csv_sido_inventory() -> anyhow::Result<()> {
    let original = contract();
    assert_eq!(original.objects.len(), 204);
    assert_eq!(selected_objects(&original)?.len(), 17);
    assert_eq!(original.selected_vintage, "20260607");

    let mut partial = contract();
    let selected = partial
        .objects
        .iter()
        .position(|object| object.vintage == partial.selected_vintage)
        .unwrap();
    partial.objects.remove(selected);
    assert!(selected_objects(&partial).is_err());

    let mut duplicate = contract();
    duplicate
        .objects
        .push(duplicate.objects.last().unwrap().clone());
    assert!(selected_objects(&duplicate).is_err());

    let mut stale = contract();
    stale.selected_vintage = "20260507".into();
    assert!(selected_objects(&stale).is_err());

    let mut change_feed = contract();
    change_feed.dataset_series = "CH_D003".into();
    assert!(selected_objects(&change_feed).is_err());

    let mut wrong_member = contract();
    wrong_member.objects[0].dataset_name = "CH_D003_11_20250715.csv".into();
    assert!(selected_objects(&wrong_member).is_err());

    let mut placeholder = contract();
    placeholder.coordinator_fills_real_inventory = true;
    assert!(selected_objects(&placeholder).is_err());
    Ok(())
}

#[test]
fn handoff_location_is_owned_only_by_the_inventory_contract() -> anyhow::Result<()> {
    let mut contract = contract();
    contract.handoff_prefix = "SYNTHETIC-relocated-forest-handoff".into();
    contract.handoff_suffix = ".ndjson.gz".into();
    let object = SourceObject {
        object_key: "bronze/source=vworldkr__land_forest/SYNTHETIC_AL_D003_99_20260607.zip".into(),
        dataset_name: "AL_D003_99_20260607.csv".into(),
        region_code: "99".into(),
        vintage: "20260607".into(),
    };
    assert_eq!(
        handoff_key(&contract, &object)?,
        "SYNTHETIC-relocated-forest-handoff/SYNTHETIC_AL_D003_99_20260607.ndjson.gz"
    );
    contract.handoff_prefix.clear();
    assert!(handoff_key(&contract, &object).is_err());
    Ok(())
}

#[tokio::test]
async fn projection_reads_the_csv_export_with_category_code_and_nullable_count(
) -> anyhow::Result<()> {
    let body = crate::land_use_silver_export::forest_csv_tests::fixture_handoff().await?;
    let row: HandoffRow = serde_json::from_str(body.trim())?;
    assert_eq!(row.pnu, "9999938029204450003");
    assert_eq!(row.land_category.as_deref(), Some("임야"));
    assert_eq!(row.ownership_kind_code.as_deref(), Some("01"));
    assert_eq!(row.co_owner_count, Some(2));
    assert!(valid_row(&row));
    let mut nullable = row;
    nullable.co_owner_count = None;
    assert!(valid_row(&nullable));
    nullable.co_owner_count = Some(-1);
    assert!(!valid_row(&nullable));
    Ok(())
}

#[test]
fn copy_escaping_preserves_source_text_and_nulls() {
    assert_eq!(copy_text(Some("a\tb\nc\r\\N")), "a\\tb\\nc\\r\\\\N");
    assert_eq!(copy_text(None), "\\N");
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL through DATABASE_URL"]
async fn copy_merge_preserves_forest_fields_newest_vintage_and_rolls_back(
) -> foundation_disposable_database::TestResult {
    use catalog_application::ports::CatalogRepository;

    let body = crate::land_use_silver_export::forest_csv_tests::fixture_handoff().await?;
    foundation_disposable_database::run_in_disposable_database("forest_ledger_projection", |pool| async move {
        let mut conn = pool.acquire().await?;
        conn.execute("CREATE SCHEMA catalog").await?;
        // The absence of catalog.parcel proves this attribute lane has no parcel FK.
        conn.execute(include_str!("../../../migrations/20260907000000_a_parcel_wears_its_forest_ledger.sql")).await?;
        let mut tx = conn.begin().await?;
        prepare_stage(&mut tx).await?;
        let base: HandoffRow = serde_json::from_str(body.trim())?;
        let object = SourceObject { object_key: base.source_record_id.clone(),
            dataset_name: "AL_D003_99_20260607.csv".into(), region_code: "99".into(),
            vintage: "20260607".into() };
        assert_eq!(stage_rows(&mut tx, std::io::Cursor::new(format!("{body}{body}")), &object).await?, (2, 0));
        assert_eq!(merge_stage(&mut tx).await?, 1);
        assert_eq!(merge_stage(&mut tx).await?, 0);

        let mut newer = object.clone();
        newer.vintage = "20260707".into();
        let mut new_row: serde_json::Value = serde_json::from_str(body.trim())?;
        new_row["area_m2"] = serde_json::json!(456.78);
        new_row["ownership_kind_code"] = serde_json::json!("07");
        new_row["co_owner_count"] = serde_json::Value::Null;
        stage_rows(&mut tx, std::io::Cursor::new(new_row.to_string()), &newer).await?;
        assert_eq!(merge_stage(&mut tx).await?, 1);
        stage_rows(&mut tx, std::io::Cursor::new(&body), &object).await?;
        assert_eq!(merge_stage(&mut tx).await?, 0);

        let (area, ownership, count, vintage): (f64, Option<String>, Option<i32>, NaiveDate) =
            sqlx::query_as("SELECT area_m2::double precision, ownership_kind, co_owner_count, source_vintage FROM catalog.parcel_forest_ledger WHERE pnu = $1::character(19)")
                .bind(&base.pnu).fetch_one(&mut *tx).await?;
        assert!((area - 456.78).abs() < f64::EPSILON);
        assert_eq!(ownership.as_deref(), Some("07"));
        assert_eq!(count, None);
        assert_eq!(vintage, parse_vintage("20260707")?);
        let mut wrong_lineage = object.clone();
        wrong_lineage.object_key = "bronze/source=vworldkr__land_forest/wrong-object.zip".into();
        assert!(stage_rows(&mut tx, std::io::Cursor::new(&body), &wrong_lineage)
            .await
            .is_err());
        tx.rollback().await?;
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.parcel_forest_ledger")
            .fetch_one(&mut *conn).await?;
        assert_eq!(rows, 0);

        for invalid in ["empty", "invalid_area", "negative_count", "wrong_region", "empty_snapshot"] {
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            let mut bad: serde_json::Value = serde_json::from_str(body.trim())?;
            let mut source = object.clone();
            match invalid {
                "invalid_area" => bad["area_m2"] = serde_json::json!(0),
                "negative_count" => bad["co_owner_count"] = serde_json::json!(-1),
                "wrong_region" => source.region_code = "98".into(),
                "empty_snapshot" => bad["source_snapshot_id"] = serde_json::json!(" "),
                _ => {}
            }
            let input = if invalid == "empty" { String::new() } else { bad.to_string() };
            assert!(stage_rows(&mut tx, std::io::Cursor::new(input), &source).await.is_err(), "{invalid}");
            tx.rollback().await?;
        }
        // Follow the committed CSV projection through the production read adapter.
        let mut tx = conn.begin().await?;
        prepare_stage(&mut tx).await?;
        stage_rows(&mut tx, std::io::Cursor::new(&body), &object).await?;
        assert_eq!(merge_stage(&mut tx).await?, 1);
        tx.commit().await?;
        let repository = catalog_infrastructure::PgCatalogRepository::new(pool.clone());
        let ledger = repository.find_parcel_forest_ledger_by_pnu(&Pnu::parse(&base.pnu)?)
            .await?.expect("committed forest ledger");
        assert_eq!(ledger.land_category, base.land_category);
        assert_eq!(ledger.area_m2, base.area_m2);
        assert_eq!(ledger.ownership_kind, base.ownership_kind_code);
        assert_eq!(ledger.co_owner_count, base.co_owner_count);
        assert_eq!(ledger.source_snapshot_id, base.source_snapshot_id);
        assert!(repository.find_parcel_forest_ledger_by_pnu(&Pnu::parse("9999938029204450004")?)
            .await?.is_none());
        Ok(())
    }).await
}
