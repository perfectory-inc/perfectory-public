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
async fn duplicates_collapse_deterministically_and_the_unit_key_serves_a_bounded_page(
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
            conn.execute(include_str!(
                "../../../migrations/20260907050000_a_land_right_belongs_to_a_unit.sql"
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

            // Byte-identical provider duplicates collapse inside one COPY, and the
            // collapse is visible in the counters instead of aborting the load
            // (root ADR-0093 mirrors ADR-0090).
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert_eq!(
                stage_rows(
                    &mut tx,
                    std::io::Cursor::new(format!("{body}{first_line}\n")),
                    &object,
                )
                .await?,
                3
            );
            assert_eq!(conflicting_groups(&mut tx).await?, 0);
            assert_eq!(merge_stage(&mut tx).await?, 2);
            tx.rollback().await?;

            // Same unit key with a different payload is a counted conflict, and the
            // survivor is deterministic: staging the variants in either order keeps
            // the same winning row.
            let mut variant: serde_json::Value = serde_json::from_str(first_line)?;
            variant["building_name"] = serde_json::json!("conflict");
            let variant_line = variant.to_string();
            let mut winners = Vec::new();
            for input in [
                format!("{first_line}\n{variant_line}\n"),
                format!("{variant_line}\n{first_line}\n"),
            ] {
                let mut tx = conn.begin().await?;
                prepare_stage(&mut tx).await?;
                assert_eq!(
                    stage_rows(&mut tx, std::io::Cursor::new(input), &object).await?,
                    2
                );
                assert_eq!(conflicting_groups(&mut tx).await?, 1);
                assert_eq!(merge_stage(&mut tx).await?, 1);
                let winner: String =
                    sqlx::query_scalar("SELECT building_name FROM catalog.parcel_land_right")
                        .fetch_one(&mut *tx)
                        .await?;
                winners.push(winner);
                tx.rollback().await?;
            }
            assert_eq!(winners[0], winners[1]);

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

            // Rows that differ only in unit designation are distinct facts under the
            // six-column key: 205 ho variants all land, the read serves a bounded
            // 200-row page, and the total reports what the page truncated.
            let mut unit_rows = String::new();
            for ho in 0..205 {
                let mut row: serde_json::Value = serde_json::from_str(first_line)?;
                row["ho_name"] = serde_json::json!(format!("{}호", ho + 301));
                unit_rows.push_str(&row.to_string());
                unit_rows.push('\n');
            }
            let full = format!("{body}{unit_rows}");
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert_eq!(
                stage_rows(&mut tx, std::io::Cursor::new(&full), &object).await?,
                207
            );
            assert_eq!(conflicting_groups(&mut tx).await?, 0);
            assert_eq!(merge_stage(&mut tx).await?, 207);
            assert_eq!(merge_stage(&mut tx).await?, 0);
            tx.commit().await?;

            // A later source replay with the same unit key cannot rewrite the first fact.
            let mut changed: serde_json::Value = serde_json::from_str(first_line)?;
            changed["building_name"] = serde_json::json!("would overwrite");
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
            let page = repository
                .list_parcel_land_rights_by_pnu(&Pnu::parse(&base.pnu)?)
                .await?;
            assert_eq!(page.total, 207);
            assert_eq!(page.rights.len(), 200);
            assert_eq!(page.rights[0].building_name, base.building_name);
            assert_eq!(
                page.rights[0].dong_name,
                base.dong_name.clone().unwrap_or_default()
            );
            assert_eq!(
                page.rights[0].floor_name,
                base.floor_name.clone().unwrap_or_default()
            );
            assert_eq!(page.rights[0].closure_kind_code, base.closure_kind_code);
            assert_eq!(page.rights[0].closure_kind, base.closure_kind_name);
            assert_eq!(page.rights[0].right_ratio, base.right_ratio);
            let mut served: Vec<(String, String)> = page
                .rights
                .iter()
                .map(|r| (r.right_serial_no.clone(), r.ho_name.clone()))
                .collect();
            let sorted = {
                let mut copy = served.clone();
                copy.sort();
                copy
            };
            assert_eq!(served, sorted, "page order must be deterministic");
            served.dedup();
            assert_eq!(served.len(), 200, "unit keys must stay distinct");

            let empty = repository
                .list_parcel_land_rights_by_pnu(&Pnu::parse("9999938029104450004")?)
                .await?;
            assert_eq!(empty.total, 0);
            assert!(empty.rights.is_empty());
            Ok(())
        },
    )
    .await
}
