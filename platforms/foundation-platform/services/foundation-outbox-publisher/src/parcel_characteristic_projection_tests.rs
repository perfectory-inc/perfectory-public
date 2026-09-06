use super::*;

fn row() -> HandoffRow {
    HandoffRow {
        pnu: "9999938029104450003".into(),
        land_category: Some("대".into()),
        area_m2: Some(123.45),
        land_use_situation: Some("공업용".into()),
        terrain_height: Some("평지".into()),
        terrain_shape: Some("정방형".into()),
        road_contact: Some("광대로".into()),
        verification_price_per_m2: "81700".into(),
        base_year: "2025".into(),
        base_month: "1".into(),
        source_vintage: "20260526".into(),
        source_snapshot_id: "land-characteristic:20260526".into(),
        source_record_id:
            "bronze/source=vworldkr__land_characteristic/SYNTHETIC_AL_D194_99999_20260526.zip"
                .into(),
    }
}

fn contract() -> SourceContract {
    SourceContract {
        schema_version: 1,
        coordinator_fills_real_inventory: false,
        granularity_counts: RegionCounts { sigungu: 2 },
        selected_vintage: "20260526".into(),
        handoff_prefix: "silver-handoff/vworldkr__land_characteristic".into(),
        handoff_suffix: ".jsonl.gz".into(),
        objects: [
            ("99999", "20260210"),
            ("99998", "20260210"),
            ("99999", "20260526"),
            ("99998", "20260526"),
        ]
        .into_iter()
        .map(|(region, vintage)| SourceObject {
            object_key: format!(
                "bronze/source=vworldkr__land_characteristic/SYNTHETIC_AL_D194_{region}_{vintage}.zip"
            ),
            region_code: region.into(),
            vintage: vintage.into(),
        })
        .collect(),
    }
}

#[test]
fn national_selection_rejects_partial_duplicate_stale_and_unmeasured_inventory(
) -> anyhow::Result<()> {
    let original = contract();
    assert_eq!(selected_objects(&original)?.len(), 2);
    let mut partial = contract();
    partial.objects.pop();
    assert!(selected_objects(&partial)
        .expect_err("partial country")
        .to_string()
        .contains("partial_country"));
    let mut lost_region = contract();
    lost_region.objects.retain(|o| o.region_code == "99999");
    assert!(selected_objects(&lost_region).is_err());
    let mut duplicate = contract();
    duplicate.objects.push(duplicate.objects[2].clone());
    assert!(selected_objects(&duplicate).is_err());
    let mut stale = contract();
    stale.selected_vintage = "20260210".into();
    assert!(selected_objects(&stale).is_err());
    let mut placeholder = contract();
    placeholder.coordinator_fills_real_inventory = true;
    assert!(selected_objects(&placeholder)
        .expect_err("placeholder must refuse")
        .to_string()
        .contains("inventory_unmeasured"));
    let disk: SourceContract = serde_json::from_str(include_str!(
        "../../../infra/lakehouse/contracts/vworld-land-characteristic-source-objects.json"
    ))?;
    if disk.coordinator_fills_real_inventory {
        assert!(selected_objects(&disk).is_err());
    } else {
        assert!(!selected_objects(&disk)?.is_empty());
    }
    Ok(())
}

#[test]
fn handoff_location_is_owned_only_by_the_inventory_contract() -> anyhow::Result<()> {
    let mut contract = contract();
    contract.handoff_prefix = "SYNTHETIC-relocated-handoff".into();
    contract.handoff_suffix = ".ndjson.gz".into();
    assert_eq!(
        handoff_key(&contract, &contract.objects[0])?,
        "SYNTHETIC-relocated-handoff/SYNTHETIC_AL_D194_99999_20260210.ndjson.gz"
    );
    contract.handoff_prefix.clear();
    assert!(handoff_key(&contract, &contract.objects[0]).is_err());
    Ok(())
}

#[test]
fn mapping_gate_proves_shifted_columns_fail_and_enforces_threshold_edges() -> anyhow::Result<()> {
    let mut sample = vec![row(); 100];
    validate_mapping(&sample)?;
    sample[0].pnu = "invalid".into();
    validate_mapping(&sample)?;
    sample[1].pnu = "invalid".into();
    assert!(validate_mapping(&sample)
        .expect_err("98% pnu must fail")
        .to_string()
        .contains("mapping_pnu_failed"));
    let mut sample = vec![row(); 100];
    sample[0].area_m2 = Some(0.0);
    validate_mapping(&sample)?;
    sample[1].area_m2 = Some(f64::NAN);
    assert!(validate_mapping(&sample)
        .expect_err("98% area must fail")
        .to_string()
        .contains("mapping_area_failed"));
    let mut sample = vec![row(); 100];
    for entry in &mut sample[..5] {
        entry.land_category = Some("평지".into());
    }
    validate_mapping(&sample)?;
    sample[5].land_category = None;
    assert!(validate_mapping(&sample)
        .expect_err("94% category must fail")
        .to_string()
        .contains("mapping_land_category_failed"));
    assert!(validate_mapping(&[]).is_err());
    Ok(())
}

#[test]
fn price_gate_compares_only_identical_assessment_vintages_and_can_fail() -> anyhow::Result<()> {
    let mut check = PriceCheck::default();
    check.observe((2025, 1, 81_700), Some((2026, 1, 123)));
    check.observe((2025, 1, 81_700), Some((2025, 7, 123)));
    check.observe((2025, 1, 81_700), None);
    check.validate()?;
    assert_eq!(check.comparable, 0);
    assert_eq!(check.different_vintage, 2);
    for _ in 0..99 {
        check.observe((2025, 1, 81_700), Some((2025, 1, 81_700)));
    }
    check.observe((2025, 1, 81_700), Some((2025, 1, 123)));
    check.validate()?; // Exactly 1% is not a material fraction.
    check.observe((2025, 1, 81_700), Some((2025, 1, 123)));
    assert!(check
        .validate()
        .expect_err("a changed A25 must fail")
        .to_string()
        .contains("price_mapping_mismatch"));
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
    foundation_disposable_database::run_in_disposable_database("characteristic_projection", |pool| async move {
        let mut conn = pool.acquire().await?;
        conn.execute("CREATE SCHEMA catalog").await?;
        conn.execute(include_str!("../../../migrations/20260906000001_a_parcel_wears_its_price.sql")).await?;
        conn.execute(include_str!("../../../migrations/20260906000002_a_parcel_wears_its_characteristics.sql")).await?;
        sqlx::query("INSERT INTO catalog.parcel_price (pnu, price_per_m2, base_year, base_month, source_snapshot_id) VALUES ($1::character(19), 81700, 2025, 1, 'price:test')")
            .bind(&row().pnu).execute(&mut *conn).await?;
        let mut tx = conn.begin().await?;
        prepare_stage(&mut tx).await?;
        let base = row();
        let object = SourceObject { object_key: base.source_record_id.clone(), region_code: "99999".into(), vintage: base.source_vintage.clone() };
        let json_row = |area: f64, vintage: &str| -> String {
            serde_json::json!({"pnu":base.pnu,"land_category":"대","area_m2":area,
                "land_use_situation":"공업용","terrain_height":"평지","terrain_shape":"정방형","road_contact":"광대로",
                "verification_price_per_m2":"81700","base_year":"2025","base_month":"1",
                "source_record_id":base.source_record_id,"source_snapshot_id":"characteristic:test","source_vintage":vintage}).to_string()
        };
        let raw = format!("{}\n{}\n", json_row(123.45, "20260526"), json_row(123.45, "20260526"));
        let (rows, sample, skipped) = stage_rows(&mut tx, std::io::Cursor::new(raw), &object, 1000).await?;
        assert_eq!((rows, skipped), (2, 0));
        validate_mapping(&sample)?;
        compare_prices(&mut tx, &sample).await?.validate()?;
        assert_eq!(merge_stage(&mut tx).await?, 1);
        assert_eq!(merge_stage(&mut tx).await?, 0);
        let mut newer = object.clone(); newer.vintage = "20260609".into();
        stage_rows(&mut tx, std::io::Cursor::new(json_row(456.78, "20260609")), &newer, 1000).await?;
        assert_eq!(merge_stage(&mut tx).await?, 1);
        stage_rows(&mut tx, std::io::Cursor::new(json_row(1.0, "20260526")), &object, 1000).await?;
        assert_eq!(merge_stage(&mut tx).await?, 0);
        let area: f64 = sqlx::query_scalar("SELECT area_m2::double precision FROM catalog.parcel_characteristic WHERE pnu = $1::character(19)")
            .bind(&base.pnu).fetch_one(&mut *tx).await?;
        assert!((area - 456.78).abs() < f64::EPSILON);
        let mut wrong = sample; wrong[0].verification_price_per_m2 = "1".into();
        let check = compare_prices(&mut tx, &wrong).await?;
        assert!(check.validate().is_err());
        tx.rollback().await?;
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.parcel_characteristic").fetch_one(&mut *conn).await?;
        assert_eq!(rows, 0);
        Ok(())
    }).await
}
