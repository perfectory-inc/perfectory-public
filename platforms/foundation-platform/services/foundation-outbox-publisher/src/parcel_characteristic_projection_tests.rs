use super::*;

fn contract() -> SourceContract {
    serde_json::from_str(include_str!(
        "../../../infra/lakehouse/contracts/vworld-land-characteristic-source-objects.json"
    ))
    .expect("measured CSV inventory")
}

#[test]
fn national_selection_requires_the_newest_complete_csv_sido_inventory() -> anyhow::Result<()> {
    let original = contract();
    assert_eq!(original.objects.len(), 51);
    assert_eq!(selected_objects(&original)?.len(), 17);
    assert_eq!(original.selected_vintage, "20260519");
    let mut partial = contract();
    let selected = partial
        .objects
        .iter()
        .position(|o| o.vintage == partial.selected_vintage)
        .unwrap();
    partial.objects.remove(selected);
    assert!(selected_objects(&partial).is_err());
    let mut lost_region = contract();
    let region = lost_region.objects[0].region_code.clone();
    lost_region.objects.retain(|o| o.region_code != region);
    assert!(selected_objects(&lost_region).is_err());
    let mut duplicate = contract();
    duplicate
        .objects
        .push(duplicate.objects.last().unwrap().clone());
    assert!(selected_objects(&duplicate).is_err());
    let mut duplicate_region = contract();
    let last = duplicate_region.objects.len() - 1;
    duplicate_region.objects[last].region_code =
        duplicate_region.objects[last - 1].region_code.clone();
    duplicate_region.objects[last].dataset_name =
        duplicate_region.objects[last - 1].dataset_name.clone();
    assert!(selected_objects(&duplicate_region).is_err());
    let mut stale = contract();
    stale.selected_vintage = "20250813".into();
    assert!(selected_objects(&stale).is_err());
    let mut shapefile = contract();
    shapefile.dataset_series = "AL_D194".into();
    assert!(selected_objects(&shapefile).is_err());
    let mut wrong_member = contract();
    wrong_member.objects[0].dataset_name = "AL_D194_99999_20250813.dbf".into();
    assert!(selected_objects(&wrong_member).is_err());
    let mut wrong_granularity = contract();
    wrong_granularity.load_granularity = "sigungu".into();
    assert!(selected_objects(&wrong_granularity).is_err());
    let mut placeholder = contract();
    placeholder.coordinator_fills_real_inventory = true;
    assert!(selected_objects(&placeholder).is_err());
    Ok(())
}

#[test]
fn handoff_location_is_owned_only_by_the_inventory_contract() -> anyhow::Result<()> {
    let mut contract = contract();
    contract.handoff_prefix = "SYNTHETIC-relocated-handoff".into();
    contract.handoff_suffix = ".ndjson.gz".into();
    let object = SourceObject {
        object_key: "bronze/source=vworldkr__land_characteristic/SYNTHETIC_AL_D195_99_20260519.zip"
            .into(),
        dataset_name: "AL_D195_99_20260519.csv".into(),
        region_code: "99".into(),
        vintage: "20260519".into(),
    };
    assert_eq!(
        handoff_key(&contract, &object)?,
        "SYNTHETIC-relocated-handoff/SYNTHETIC_AL_D195_99_20260519.ndjson.gz"
    );
    contract.handoff_prefix.clear();
    assert!(handoff_key(&contract, &object).is_err());
    Ok(())
}

#[tokio::test]
async fn projection_reads_the_csv_export_without_a_price_or_dbf_vintage() -> anyhow::Result<()> {
    let body = crate::land_use_silver_export::characteristic_csv_tests::fixture_handoff().await?;
    let row: HandoffRow = serde_json::from_str(body.trim())?;
    assert_eq!(row.pnu, "9999938029104450003");
    assert_eq!(row.land_category.as_deref(), Some("대"));
    assert!(valid_area(&row));
    Ok(())
}

#[test]
fn copy_escaping_preserves_source_text_and_nulls() {
    assert_eq!(copy_text(Some("a\tb\nc\r\\N")), "a\\tb\\nc\\r\\\\N");
    assert_eq!(copy_text(None), "\\N");
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL through DATABASE_URL"]
async fn copy_merge_preserves_fractional_area_and_newest_vintage_and_rolls_back(
) -> foundation_disposable_database::TestResult {
    let body = crate::land_use_silver_export::characteristic_csv_tests::fixture_handoff().await?;
    foundation_disposable_database::run_in_disposable_database("characteristic_projection", |pool| async move {
        let mut conn = pool.acquire().await?;
        conn.execute("CREATE SCHEMA catalog").await?;
        // No parcel_price table exists: characteristics are independent of price ingestion.
        conn.execute(include_str!("../../../migrations/20260906000002_a_parcel_wears_its_characteristics.sql")).await?;
        let mut tx = conn.begin().await?;
        prepare_stage(&mut tx).await?;
        let base: HandoffRow = serde_json::from_str(body.trim())?;
        let object = SourceObject { object_key: base.source_record_id.clone(), dataset_name: "AL_D195_99_20260519.csv".into(), region_code: "99".into(), vintage: "20260519".into() };
        let duplicate = format!("{body}{body}");
        assert_eq!(stage_rows(&mut tx, std::io::Cursor::new(duplicate), &object).await?, (2, 0));
        assert_eq!(merge_stage(&mut tx).await?, 1);
        assert_eq!(merge_stage(&mut tx).await?, 0);
        let mut newer = object.clone(); newer.vintage = "20260609".into();
        let mut new_row: serde_json::Value = serde_json::from_str(body.trim())?;
        new_row["area_m2"] = serde_json::json!(456.78);
        stage_rows(&mut tx, std::io::Cursor::new(new_row.to_string()), &newer).await?;
        assert_eq!(merge_stage(&mut tx).await?, 1);
        stage_rows(&mut tx, std::io::Cursor::new(&body), &object).await?;
        assert_eq!(merge_stage(&mut tx).await?, 0);
        let (area, vintage): (f64, NaiveDate) = sqlx::query_as("SELECT area_m2::double precision, source_vintage FROM catalog.parcel_characteristic WHERE pnu = $1::character(19)")
            .bind(&base.pnu).fetch_one(&mut *tx).await?;
        assert!((area - 456.78).abs() < f64::EPSILON);
        assert_eq!(vintage, parse_vintage("20260609")?);
        let mut wrong = object.clone(); wrong.object_key = "wrong-object".into();
        assert!(stage_rows(&mut tx, std::io::Cursor::new(&body), &wrong).await.is_err());
        tx.rollback().await?;
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.parcel_characteristic").fetch_one(&mut *conn).await?;
        assert_eq!(rows, 0);

        for invalid in ["empty", "invalid_area", "wrong_region", "empty_snapshot"] {
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            let mut bad: serde_json::Value = serde_json::from_str(body.trim())?;
            let mut source = object.clone();
            match invalid {
                "invalid_area" => bad["area_m2"] = serde_json::json!(0),
                "wrong_region" => source.region_code = "98".into(),
                "empty_snapshot" => bad["source_snapshot_id"] = serde_json::json!(" "),
                _ => {}
            }
            let input = if invalid == "empty" { String::new() } else { bad.to_string() };
            assert!(stage_rows(&mut tx, std::io::Cursor::new(input), &source).await.is_err(), "{invalid}");
            tx.rollback().await?;
        }
        Ok(())
    }).await
}
