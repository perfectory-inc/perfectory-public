use super::*;

fn contract() -> SourceContract {
    serde_json::from_str(include_str!(
        "../../../infra/lakehouse/contracts/vworld-land-transfer-history-source-objects.json"
    ))
    .expect("measured AL_D157 CSV inventory")
}

#[test]
fn national_selection_requires_the_newest_complete_csv_sido_inventory() -> anyhow::Result<()> {
    let original = contract();
    assert_eq!(original.objects.len(), 153);
    assert_eq!(selected_objects(&original)?.len(), 17);
    assert_eq!(original.selected_vintage, "20260531");

    let mut newer_partial = contract();
    let mut future = newer_partial.objects[0].clone();
    future.vintage = "20990101".into();
    future.object_key = "bronze/source=vworldkr__land_transfer_history/SYNTHETIC-future.zip".into();
    future.dataset_name = format!("AL_D157_{}_20990101.csv", future.region_code);
    newer_partial.objects.push(future);
    assert_eq!(selected_objects(&newer_partial)?.len(), 17);

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
    stale.selected_vintage = "20260430".into();
    assert!(selected_objects(&stale).is_err());

    let mut change_feed = contract();
    change_feed.dataset_series = "CH_D157".into();
    assert!(selected_objects(&change_feed).is_err());

    let mut wrong_member = contract();
    wrong_member.objects[0].dataset_name = "CH_D157_11_20250715.csv".into();
    assert!(selected_objects(&wrong_member).is_err());

    let mut placeholder = contract();
    placeholder.coordinator_fills_real_inventory = true;
    assert!(selected_objects(&placeholder).is_err());
    Ok(())
}

#[test]
fn handoff_location_is_owned_only_by_the_inventory_contract() -> anyhow::Result<()> {
    let mut contract = contract();
    contract.handoff_prefix = "SYNTHETIC-relocated-transfer-handoff".into();
    contract.handoff_suffix = ".ndjson.gz".into();
    let object = SourceObject {
        object_key:
            "bronze/source=vworldkr__land_transfer_history/SYNTHETIC_AL_D157_99_20260531.zip".into(),
        dataset_name: "AL_D157_99_20260531.csv".into(),
        region_code: "99".into(),
        vintage: "20260531".into(),
    };
    assert_eq!(
        handoff_key(&contract, &object)?,
        "SYNTHETIC-relocated-transfer-handoff/SYNTHETIC_AL_D157_99_20260531.ndjson.gz"
    );
    contract.handoff_prefix.clear();
    assert!(handoff_key(&contract, &object).is_err());
    Ok(())
}

#[tokio::test]
async fn projection_reads_all_events_from_the_csv_export() -> anyhow::Result<()> {
    let body = crate::land_use_silver_export::transfer_csv_tests::fixture_handoff().await?;
    let rows: Vec<HandoffRow> = body
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].pnu, rows[1].pnu);
    assert_eq!(rows[0].transfer_history_seq, 1);
    assert_eq!(rows[1].transfer_history_seq, 2_147_483_648);
    assert_eq!(rows[0].erased_at.as_deref(), Some("2005-06-08"));
    assert_eq!(rows[0].closure_seq.as_deref(), Some("01"));
    Ok(())
}

#[test]
fn copy_escaping_preserves_source_text_and_nulls() {
    assert_eq!(copy_text(Some("a\tb\nc\r\\N")), "a\\tb\\nc\\r\\\\N");
    assert_eq!(copy_text(None), "\\N");
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL through DATABASE_URL"]
async fn copy_refuses_duplicate_events_and_serves_the_full_append_only_timeline(
) -> foundation_disposable_database::TestResult {
    use catalog_application::ports::CatalogRepository;

    let body = crate::land_use_silver_export::transfer_csv_tests::fixture_handoff().await?;
    foundation_disposable_database::run_in_disposable_database(
        "transfer_event_projection",
        |pool| async move {
            let mut conn = pool.acquire().await?;
            conn.execute("CREATE SCHEMA catalog").await?;
            // No catalog.parcel table: the history can arrive before boundaries.
            conn.execute(include_str!(
                "../../../migrations/20260907010000_a_parcel_keeps_its_transfer_events.sql"
            ))
            .await?;
            let first_line = body.lines().next().unwrap();
            let base: HandoffRow = serde_json::from_str(first_line)?;
            let object = SourceObject {
                object_key: base.source_record_id.clone(),
                dataset_name: "AL_D157_99_20260531.csv".into(),
                region_code: "99".into(),
                vintage: "20260531".into(),
            };

            // Both byte-identical and conflicting values for the same event must fail with
            // the database's unique_violation, not be collapsed or hidden by DO NOTHING.
            for changed in [false, true] {
                let mut tx = conn.begin().await?;
                prepare_stage(&mut tx).await?;
                assert_eq!(
                    stage_rows(&mut tx, std::io::Cursor::new(&body), &object).await?,
                    2
                );
                // Prove rollback removes an earlier merge too, not just the failing COPY.
                assert_eq!(merge_stage(&mut tx).await?, 2);
                let mut duplicate: serde_json::Value = serde_json::from_str(first_line)?;
                if changed {
                    duplicate["reason"] = serde_json::json!("conflicting reason");
                }
                let error = stage_rows(
                    &mut tx,
                    std::io::Cursor::new(duplicate.to_string()),
                    &object,
                )
                .await
                .expect_err("duplicate event key must refuse");
                let database_error = error
                    .downcast_ref::<sqlx::Error>()
                    .and_then(sqlx::Error::as_database_error)
                    .expect("PostgreSQL constraint error");
                assert_eq!(database_error.code().as_deref(), Some("23505"));
                tx.rollback().await?;
                let rows: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM catalog.parcel_transfer_event")
                        .fetch_one(&mut *conn)
                        .await?;
                assert_eq!(rows, 0);
            }
            // A duplicate inside one COPY (rather than successive calls) is equally refused.
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert!(stage_rows(
                &mut tx,
                std::io::Cursor::new(format!("{body}{body}")),
                &object
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
                "missing_seq",
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
                    "missing_seq" => bad["transfer_history_seq"] = serde_json::Value::Null,
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

            // Add tied dates and a null date to drive the production ORDER BY, without a cap.
            let mut tied: serde_json::Value = serde_json::from_str(first_line)?;
            tied["transfer_history_seq"] = serde_json::json!(3);
            tied["moved_at"] = serde_json::json!("2005-06-08");
            tied["area_m2"] = serde_json::json!(0);
            let mut undated = tied.clone();
            undated["transfer_history_seq"] = serde_json::json!(4);
            undated["moved_at"] = serde_json::Value::Null;
            undated["area_m2"] = serde_json::Value::Null;
            let full = format!("{body}{tied}\n{undated}\n");
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            assert_eq!(
                stage_rows(&mut tx, std::io::Cursor::new(&full), &object).await?,
                4
            );
            assert_eq!(merge_stage(&mut tx).await?, 4);
            assert_eq!(merge_stage(&mut tx).await?, 0);
            tx.commit().await?;

            // A new load with an existing key never overwrites the preserved historical fact.
            let mut repeated: serde_json::Value = serde_json::from_str(first_line)?;
            repeated["reason"] = serde_json::json!("would overwrite history");
            repeated["erased_at"] = serde_json::Value::Null;
            let mut tx = conn.begin().await?;
            prepare_stage(&mut tx).await?;
            stage_rows(&mut tx, std::io::Cursor::new(repeated.to_string()), &object).await?;
            assert_eq!(merge_stage(&mut tx).await?, 0);
            tx.commit().await?;

            let repository = catalog_infrastructure::PgCatalogRepository::new(pool.clone());
            let events = repository
                .list_parcel_transfer_events_by_pnu(&Pnu::parse(&base.pnu)?)
                .await?;
            assert_eq!(
                events
                    .iter()
                    .map(|e| e.transfer_history_seq)
                    .collect::<Vec<_>>(),
                vec![2_147_483_648, 3, 1, 4]
            );
            assert_eq!(events[2].reason, base.reason);
            assert_eq!(events[2].reason_code, base.reason_code);
            assert_eq!(events[2].erased_at, base.erased_at);
            assert_eq!(events[2].closure_seq, base.closure_seq);
            assert_eq!(events[2].land_category, base.land_category);
            assert_eq!(events[2].area_m2, base.area_m2);
            assert_eq!(events[2].source_snapshot_id, base.source_snapshot_id);
            assert_eq!(events[1].area_m2, Some(0.0));
            assert_eq!(events[3].area_m2, None);
            assert!(repository
                .list_parcel_transfer_events_by_pnu(&Pnu::parse("9999938029104450004")?)
                .await?
                .is_empty());
            Ok(())
        },
    )
    .await
}
