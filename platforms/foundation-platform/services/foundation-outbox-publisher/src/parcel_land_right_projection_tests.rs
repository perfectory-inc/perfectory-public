use super::*;

fn contract() -> SourceContract {
    serde_json::from_str(include_str!(
        "../../../infra/lakehouse/contracts/vworld-land-right-registration-source-objects.json"
    ))
    .expect("measured AL_D006 CSV inventory")
}

#[test]
fn national_selection_requires_the_newest_complete_official_csv_sido_inventory(
) -> anyhow::Result<()> {
    let original = contract();
    assert_eq!(original.objects.len(), 204);
    assert_eq!(selected_objects(&original)?.len(), 17);
    assert_eq!(original.selected_vintage, "20260609");

    let mut newer_partial = contract();
    let mut future = newer_partial.objects[0].clone();
    future.vintage = "20990101".into();
    future.object_key =
        "bronze/source=vworldkr__land_right_registration/SYNTHETIC-future.zip".into();
    future.dataset_name = format!("AL_D006_{}_20990101.csv", future.region_code);
    newer_partial.objects.push(future);
    assert_eq!(selected_objects(&newer_partial)?.len(), 17);

    let mut partial = contract();
    let selected = partial
        .objects
        .iter()
        .position(|o| o.vintage == partial.selected_vintage)
        .unwrap();
    partial.objects.remove(selected);
    assert!(selected_objects(&partial).is_err());

    let mut duplicate = contract();
    duplicate
        .objects
        .push(duplicate.objects.last().unwrap().clone());
    assert!(selected_objects(&duplicate).is_err());

    let mut stale = contract();
    stale.selected_vintage = "20260527".into();
    assert!(selected_objects(&stale).is_err());

    let mut change_feed = contract();
    change_feed.dataset_series = "CH_D006".into();
    assert!(selected_objects(&change_feed).is_err());

    let mut wrong_delimiter = contract();
    wrong_delimiter.csv_delimiter = ",".into();
    assert!(selected_objects(&wrong_delimiter).is_err());

    let mut invented_region = contract();
    let selected = invented_region
        .objects
        .iter_mut()
        .find(|o| o.vintage == invented_region.selected_vintage)
        .unwrap();
    selected.region_code = "99".into();
    selected.dataset_name = format!("AL_D006_99_{}.csv", selected.vintage);
    assert!(selected_objects(&invented_region).is_err());
    Ok(())
}

#[tokio::test]
async fn projection_preserves_serials_closure_and_nullable_unit_names() -> anyhow::Result<()> {
    let body = crate::land_use_silver_export::right_csv_tests::fixture_handoff().await?;
    let rows: Vec<HandoffRow> = body
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].pnu, rows[1].pnu);
    assert_eq!(rows[0].right_serial_no, "0001");
    assert_eq!(rows[1].right_serial_no, "214748364800000000000");
    assert_eq!(
        rows[0].building_name.as_deref(),
        Some(" 가상|건물,명 \"별관\" ")
    );
    assert_eq!(rows[0].right_ratio.as_deref(), Some(" 3분의1 "));
    assert_eq!(rows[0].closure_kind_code.as_deref(), Some("1"));
    assert_eq!(rows[0].closure_kind_name.as_deref(), Some("폐쇄"));
    assert!(rows[1].room_name.is_none());
    Ok(())
}

#[test]
fn copy_escaping_preserves_source_text_and_nulls() {
    assert_eq!(copy_text(Some("a\tb\nc\r\\N")), "a\\tb\\nc\\r\\\\N");
    assert_eq!(copy_text(None), "\\N");
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL through DATABASE_URL"]
async fn constrained_stage_refuses_duplicate_or_invalid_rights_and_serves_full_ordered_list(
) -> foundation_disposable_database::TestResult {
    use catalog_application::ports::CatalogRepository;

    let body = crate::land_use_silver_export::right_csv_tests::fixture_handoff().await?;
    foundation_disposable_database::run_in_disposable_database(
        "land_right_projection",
        |pool| async move {
            let mut conn = pool.acquire().await?;
            conn.execute("CREATE SCHEMA catalog").await?;
            conn.execute(include_str!(
                "../../../migrations/20260907030000_a_parcel_keeps_its_land_rights.sql"
            ))
            .await?;
            let first_line = body.lines().next().unwrap();
            let base: HandoffRow = serde_json::from_str(first_line)?;
            let object = SourceObject {
                object_key: base.source_record_id.clone(),
                dataset_name: "AL_D006_99_20260609.csv".into(),
                region_code: "99".into(),
                vintage: "20260609".into(),
            };

            for changed in [false, true] {
                let mut tx = conn.begin().await?;
                prepare_stage(&mut tx).await?;
                assert_eq!(
                    stage_rows(&mut tx, std::io::Cursor::new(&body), &object).await?,
                    2
                );
                assert_eq!(merge_stage(&mut tx).await?, 2);
                let mut duplicate: serde_json::Value = serde_json::from_str(first_line)?;
                if changed {
                    duplicate["building_name"] = serde_json::json!("conflict");
                }
                let error = stage_rows(
                    &mut tx,
                    std::io::Cursor::new(duplicate.to_string()),
                    &object,
                )
                .await
                .expect_err("duplicate key must refuse");
                let database_error = error
                    .downcast_ref::<sqlx::Error>()
                    .and_then(sqlx::Error::as_database_error)
                    .expect("PostgreSQL constraint error");
                assert_eq!(database_error.code().as_deref(), Some("23505"));
                tx.rollback().await?;
                assert_eq!(
                    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM catalog.parcel_land_right")
                        .fetch_one(&mut *conn)
                        .await?,
                    0
                );
            }

            // A duplicate contained in one COPY must be refused by the inherited primary key.
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert!(stage_rows(
                &mut tx,
                std::io::Cursor::new(format!("{body}{first_line}\n")),
                &object,
            )
            .await
            .is_err());
            tx.rollback().await?;

            for invalid in [
                "empty",
                "wrong_region",
                "wrong_lineage",
                "empty_snapshot",
                "invalid_pnu",
                "empty_serial",
                "nondigit_serial",
            ] {
                let mut tx = conn.begin().await?;
                prepare_stage(&mut tx).await?;
                let mut bad: serde_json::Value = serde_json::from_str(first_line)?;
                let mut source = object.clone();
                match invalid {
                    "wrong_region" => source.region_code = "98".into(),
                    "wrong_lineage" => source.object_key = "SYNTHETIC-wrong-object".into(),
                    "empty_snapshot" => bad["source_snapshot_id"] = serde_json::json!(" "),
                    "invalid_pnu" => bad["pnu"] = serde_json::json!("invalid"),
                    "empty_serial" => bad["right_serial_no"] = serde_json::json!(""),
                    "nondigit_serial" => bad["right_serial_no"] = serde_json::json!("12A"),
                    _ => {}
                }
                let input = if invalid == "empty" {
                    String::new()
                } else {
                    bad.to_string()
                };
                assert!(
                    stage_rows(&mut tx, std::io::Cursor::new(input), &source)
                        .await
                        .is_err(),
                    "{invalid}"
                );
                tx.rollback().await?;
            }

            let mut serial_rows = String::new();
            for serial in ["1", "10", "2"] {
                let mut row: serde_json::Value = serde_json::from_str(first_line)?;
                row["right_serial_no"] = serde_json::json!(serial);
                serial_rows.push_str(&row.to_string());
                serial_rows.push('\n');
            }
            let full = format!("{body}{serial_rows}");
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert_eq!(
                stage_rows(&mut tx, std::io::Cursor::new(&full), &object).await?,
                5
            );
            assert_eq!(merge_stage(&mut tx).await?, 5);
            assert_eq!(merge_stage(&mut tx).await?, 0);
            tx.commit().await?;

            // A later source replay with the same key cannot rewrite the first fact.
            let mut changed: serde_json::Value = serde_json::from_str(first_line)?;
            changed["building_name"] = serde_json::json!("would overwrite");
            changed["dong_name"] = serde_json::Value::Null;
            changed["floor_name"] = serde_json::Value::Null;
            changed["ho_name"] = serde_json::Value::Null;
            changed["room_name"] = serde_json::Value::Null;
            changed["right_ratio"] = serde_json::json!("changed ratio");
            changed["closure_kind_code"] = serde_json::Value::Null;
            changed["closure_kind_name"] = serde_json::Value::Null;
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert_eq!(
                stage_rows(&mut tx, std::io::Cursor::new(changed.to_string()), &object,).await?,
                1
            );
            assert_eq!(merge_stage(&mut tx).await?, 0);
            tx.commit().await?;

            let repository = catalog_infrastructure::PgCatalogRepository::new(pool.clone());
            let rights = repository
                .list_parcel_land_rights_by_pnu(&Pnu::parse(&base.pnu)?)
                .await?;
            assert_eq!(
                rights
                    .iter()
                    .map(|r| r.right_serial_no.as_str())
                    .collect::<Vec<_>>(),
                vec!["0001", "1", "10", "2", "214748364800000000000"]
            );
            assert_eq!(rights[0].building_name, base.building_name);
            assert_eq!(rights[0].dong_name, base.dong_name);
            assert_eq!(rights[0].floor_name, base.floor_name);
            assert_eq!(rights[0].ho_name, base.ho_name);
            assert_eq!(rights[0].room_name, base.room_name);
            assert_eq!(rights[0].closure_kind_code, base.closure_kind_code);
            assert_eq!(rights[0].closure_kind, base.closure_kind_name);
            assert_eq!(rights[0].right_ratio, base.right_ratio);
            assert!(repository
                .list_parcel_land_rights_by_pnu(&Pnu::parse("9999938029104450004")?)
                .await?
                .is_empty());
            Ok(())
        },
    )
    .await
}
