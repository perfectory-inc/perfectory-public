use super::{tests::*, *};

fn basis_line(pk: &str, parent: &str, kind: &str) -> String {
    let mut fields = vec![String::new(); 30];
    fields[0] = pk.to_owned();
    fields[1] = parent.to_owned();
    fields[4] = kind.to_owned();
    fields.join("|")
}

fn title_line(pk: &str, dong: &str) -> String {
    let mut fields = vec![String::new(); 45];
    fields[0] = pk.to_owned();
    fields[3] = "3".to_owned();
    fields[8] = "99999".to_owned();
    fields[9] = "00401".to_owned();
    fields[10] = "0".to_owned();
    fields[11] = "0089".to_owned();
    fields[12] = "0004".to_owned();
    fields[22] = dong.to_owned();
    fields[24] = "main".to_owned();
    fields[40] = "20".to_owned();
    fields.join("|")
}

fn fixture(label: &str, basis_rows: Vec<String>) -> anyhow::Result<UnitExportConfig> {
    let root = temp_root(label);
    let name = "OPN209912310000000003.zip";
    for (slug, entry, rows) in [
        (
            DEFAULT_SOURCE_SLUG,
            "mart_djy_09.txt",
            vec![unit_line("unit-opaque", "A", "101", "20", "above", "1")],
        ),
        (
            DEFAULT_TITLE_SOURCE_SLUG,
            "mart_djy_03.txt",
            vec![title_line("building-a", "A"), title_line("building-b", "B")],
        ),
        (DEFAULT_BASIS_SOURCE_SLUG, "mart_djy_01.txt", basis_rows),
    ] {
        write_zip_file(
            &root.join(format!("bronze/source={slug}/{name}")),
            entry,
            rows.join("\n").as_bytes(),
        )?;
    }
    Ok(UnitExportConfig {
        bronze_local_object_root: root.clone(),
        source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
        source_object: Some(name.to_owned()),
        title_source_slug: Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
        title_source_object: Some(name.to_owned()),
        basis_source_slug: DEFAULT_BASIS_SOURCE_SLUG.to_owned(),
        basis_source_object: Some(name.to_owned()),
        output_path: root.join("out.jsonl"),
        summary_path: Some(root.join("summary.json")),
        source_snapshot_id: "synthetic-parent-key-run".to_owned(),
        valid_from_utc: DateTime::parse_from_rfc3339("2099-12-31T00:00:00Z")?.to_utc(),
        max_rows: None,
        output_format: OutputFormat::Jsonl,
        chunk_rows: None,
        active_overrides: Vec::new(),
    })
}

fn valid_basis() -> Vec<String> {
    vec![
        basis_line("unit-opaque", "building-b", "4"),
        basis_line("building-a", "", "3"),
        basis_line("building-b", "", "3"),
    ]
}

fn exported_row(config: &UnitExportConfig) -> anyhow::Result<serde_json::Value> {
    let report = export_handoff(config)?;
    assert_eq!(report.row_count, 1);
    Ok(serde_json::from_str(
        fs::read_to_string(&config.output_path)?.trim(),
    )?)
}

#[test]
fn explicit_parent_key_wins_over_a_different_name_match_in_real_export() -> anyhow::Result<()> {
    let config = fixture("foundation-parent-key-export", valid_basis())?;
    let row = exported_row(&config)?;
    assert_eq!(row["building_mgm_bldrgst_pk"], "building-b");
    assert_eq!(row["building_link_method"], "parent_key");
    assert_eq!(
        row["building_link_source_record_id"],
        format!("bronze/source={DEFAULT_BASIS_SOURCE_SLUG}/OPN209912310000000003.zip#line-000001")
    );
    assert!(row["building_link_reason"].is_null());
    assert_eq!(row["building_main_or_annex"], "main");
    assert_eq!(row["building_title_unit_count"], 20);
    let summary: serde_json::Value =
        serde_json::from_slice(&fs::read(config.summary_path.as_ref().expect("summary"))?)?;
    let sources = summary["source"]["inputs"]
        .as_array()
        .expect("input manifest");
    assert_eq!(sources.len(), 3);
    assert_eq!(
        row["building_link_input_sha256"],
        summary["source"]["building_link_input_sha256"]
    );
    let digest = row["building_link_input_sha256"].as_str().expect("digest");
    assert_eq!(digest.len(), 64);
    for source in sources {
        assert_eq!(source["sha256"].as_str().expect("sha256").len(), 64);
    }
    fs::remove_dir_all(config.bronze_local_object_root)?;
    Ok(())
}

#[test]
fn explicit_bad_relationships_never_fall_back_to_the_matching_name() -> anyhow::Result<()> {
    for (basis, reason) in [
        (
            vec![basis_line("unit-opaque", "missing", "4")],
            "basis_parent_missing",
        ),
        (
            vec![basis_line("unit-opaque", "unit-opaque", "4")],
            "building_link_self_reference",
        ),
        (
            vec![basis_line("unit-opaque", "building-a", "3")],
            "basis_unit_kind_mismatch",
        ),
        (
            vec![
                basis_line("unit-opaque", "building-a", "4"),
                basis_line("building-a", "", "1"),
            ],
            "basis_parent_kind_mismatch",
        ),
        (
            vec![
                basis_line("unit-opaque", "building-a", "4"),
                basis_line("building-a", "", "2"),
            ],
            "basis_parent_kind_mismatch",
        ),
        (
            vec![
                basis_line("unit-opaque", "building-a", "4"),
                basis_line("unit-opaque", "building-b", "4"),
            ],
            "basis_unit_conflict",
        ),
        (
            vec![
                basis_line("unit-opaque", "building-a", "4"),
                basis_line("building-a", "", "3"),
                basis_line("building-a", "", "4"),
            ],
            "basis_parent_conflict",
        ),
        (
            vec![
                basis_line("unit-opaque", "no-title", "4"),
                basis_line("no-title", "", "3"),
            ],
            "parent_title_missing",
        ),
    ] {
        let config = fixture("foundation-parent-key-rejection", basis)?;
        let row = exported_row(&config)?;
        assert!(row["building_mgm_bldrgst_pk"].is_null(), "{reason}");
        assert_eq!(row["building_link_method"], "unresolved");
        assert_eq!(row["building_link_reason"], reason);
        assert!(row["building_main_or_annex"].is_null());
        assert!(row["building_title_unit_count"].is_null());
        assert_eq!(row["unit_name_raw"], "101");
        assert_eq!(row["unit_number"], 101);
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn absent_parent_fact_never_confirms_a_matching_name() -> anyhow::Result<()> {
    for (basis, reason) in [
        (vec![], "basis_unit_missing"),
        (
            vec![basis_line("unit-opaque", "", "4")],
            "basis_parent_key_missing",
        ),
    ] {
        let config = fixture("foundation-parent-key-absent", basis)?;
        let row = exported_row(&config)?;
        assert!(row["building_mgm_bldrgst_pk"].is_null());
        assert_eq!(row["building_link_method"], "unresolved");
        assert_eq!(row["building_link_reason"], reason);
        assert!(row["building_main_or_annex"].is_null());
        assert!(row["building_title_unit_count"].is_null());
        assert_eq!(row["unit_name_raw"], "101");
        let mut payload = row.clone();
        let checksum = payload
            .as_object_mut()
            .expect("row")
            .remove("row_checksum_sha256")
            .expect("checksum");
        use sha2::{Digest, Sha256};
        assert_eq!(
            checksum,
            format!("{:x}", Sha256::digest(serde_json::to_vec(&payload)?))
        );
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn only_building_on_a_parcel_is_not_parent_evidence() -> anyhow::Result<()> {
    let config = fixture(
        "foundation-parent-key-single-parcel",
        vec![
            basis_line("unit-opaque", "", "4"),
            basis_line("building-a", "", "3"),
        ],
    )?;
    let object = "OPN209912310000000003.zip";
    write_zip_file(
        &config
            .bronze_local_object_root
            .join(format!("bronze/source={DEFAULT_SOURCE_SLUG}/{object}")),
        "mart_djy_09.txt",
        unit_line("unit-opaque", "", "101", "20", "above", "1").as_bytes(),
    )?;
    write_zip_file(
        &config.bronze_local_object_root.join(format!(
            "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/{object}"
        )),
        "mart_djy_03.txt",
        title_line("building-a", "").as_bytes(),
    )?;
    let row = exported_row(&config)?;
    assert!(row["building_mgm_bldrgst_pk"].is_null());
    assert_eq!(row["building_link_method"], "unresolved");
    assert_eq!(row["building_link_reason"], "basis_parent_key_missing");
    assert_eq!(row["unit_name_raw"], "101");
    fs::remove_dir_all(config.bronze_local_object_root)?;
    Ok(())
}

#[test]
fn approved_parent_or_null_clears_previous_automatic_parent_evidence_and_attributes(
) -> anyhow::Result<()> {
    for parent in [Some("building-a"), None] {
        let mut config = fixture("foundation-parent-key-override", valid_basis())?;
        let before = exported_row(&config)?;
        config.active_overrides = vec![BuildingRegisterUnitSilverOverride {
            target_unit_row_id: before["unit_row_id"].as_str().expect("row id").to_owned(),
            target_mgm_bldrgst_pk: before["mgm_bldrgst_pk"]
                .as_str()
                .expect("unit pk")
                .to_owned(),
            application_id: Some("approved-application".to_owned()),
            unit_number: Some(101),
            unit_label_ko: None,
            building_mgm_bldrgst_pk: parent.map(str::to_owned),
            building_link_method: if parent.is_some() {
                "canonical_dong"
            } else {
                "unresolved"
            }
            .to_owned(),
            normalization_status: "accepted".to_owned(),
            normalization_reason: "accepted_numeric_unit".to_owned(),
        }];
        let after = exported_row(&config)?;
        assert_eq!(after["building_mgm_bldrgst_pk"], serde_json::json!(parent));
        assert_eq!(
            after["normalization_application_id"],
            "approved-application"
        );
        for key in [
            "building_link_source_record_id",
            "building_link_input_sha256",
            "building_link_reason",
            "building_main_or_annex",
            "building_title_unit_count",
        ] {
            assert!(after[key].is_null(), "{key}");
        }
        assert_eq!(before["unit_row_id"], after["unit_row_id"]);
        assert_ne!(before["row_checksum_sha256"], after["row_checksum_sha256"]);
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn required_reference_inputs_fail_before_output_is_created() -> anyhow::Result<()> {
    for mode in [
        "basis-missing",
        "title-missing",
        "basis-mixed-date",
        "title-mixed-date",
        "unit-mixed-date",
        "title-disabled",
        "basis-ambiguous",
        "unsafe-pin",
        "unsafe-slug",
        "malformed-basis",
        "malformed-title",
        "malformed-unit",
        "empty-snapshot",
        "invalid-zip",
    ] {
        let mut config = fixture("foundation-parent-key-input-failure", valid_basis())?;
        let basis_path = config.bronze_local_object_root.join(format!(
            "bronze/source={DEFAULT_BASIS_SOURCE_SLUG}/OPN209912310000000003.zip"
        ));
        match mode {
            "basis-missing" => fs::remove_file(&basis_path)?,
            "title-missing" => fs::remove_file(config.bronze_local_object_root.join(format!(
                "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/OPN209912310000000003.zip"
            )))?,
            "basis-mixed-date" => {
                config.basis_source_object = Some("OPN209912300000000003.zip".to_owned())
            }
            "title-mixed-date" => {
                config.title_source_object = Some("OPN209912300000000003.zip".to_owned())
            }
            "unit-mixed-date" => {
                config.source_object = Some("OPN209912300000000003.zip".to_owned())
            }
            "title-disabled" => config.title_source_slug = None,
            "basis-ambiguous" => {
                fs::copy(
                    &basis_path,
                    basis_path.with_file_name("OPN209912310000000011.zip"),
                )?;
                config.basis_source_object = None;
            }
            "unsafe-pin" => {
                config.basis_source_object = Some("../OPN209912310000000003.zip".to_owned())
            }
            "unsafe-slug" => config.basis_source_slug = "../basis".to_owned(),
            "malformed-basis" => write_zip_file(&basis_path, "mart_djy_01.txt", b"short|row")?,
            "malformed-title" => write_zip_file(
                &config.bronze_local_object_root.join(format!(
                    "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/OPN209912310000000003.zip"
                )),
                "mart_djy_03.txt",
                b"short|row",
            )?,
            "malformed-unit" => write_zip_file(
                &config.bronze_local_object_root.join(format!(
                    "bronze/source={DEFAULT_SOURCE_SLUG}/OPN209912310000000003.zip"
                )),
                "mart_djy_09.txt",
                b"short|row",
            )?,
            "empty-snapshot" => config.source_snapshot_id = " ".to_owned(),
            "invalid-zip" => fs::write(&basis_path, b"not a ZIP archive")?,
            _ => unreachable!(),
        }
        assert!(export_handoff(&config).is_err(), "{mode}");
        assert!(!config.output_path.exists(), "{mode}");
        assert!(
            !config.summary_path.as_ref().expect("summary").exists(),
            "{mode}"
        );
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn input_mutation_is_detected_and_next_run_gets_a_different_digest() -> anyhow::Result<()> {
    let config = fixture("foundation-parent-key-input-mutation", valid_basis())?;
    let inputs = ParentInputs::load(
        &config,
        &foundation_shared_kernel::pnu::SigunguCrosswalk::identity(),
    )?;
    let original_digest = inputs.basis.input_sha256().to_owned();
    let path = config.bronze_local_object_root.join(format!(
        "bronze/source={DEFAULT_BASIS_SOURCE_SLUG}/OPN209912310000000003.zip"
    ));
    let mut changed = valid_basis();
    changed.push(basis_line("another-unit", "building-a", "4"));
    write_zip_file(&path, "mart_djy_01.txt", changed.join("\n").as_bytes())?;
    assert!(inputs.verify_unchanged().is_err());
    let reloaded = ParentInputs::load(
        &config,
        &foundation_shared_kernel::pnu::SigunguCrosswalk::identity(),
    )?;
    assert_ne!(original_digest, reloaded.basis.input_sha256());
    reloaded.verify_unchanged()?;
    fs::remove_dir_all(config.bronze_local_object_root)?;
    Ok(())
}

pub(super) fn stage_empty_references(root: &Path) -> anyhow::Result<()> {
    for (slug, entry) in [
        (DEFAULT_TITLE_SOURCE_SLUG, "mart_djy_03.txt"),
        (DEFAULT_BASIS_SOURCE_SLUG, "mart_djy_01.txt"),
    ] {
        write_zip_file(
            &root.join(format!("bronze/source={slug}/OPN209912310000000003.zip")),
            entry,
            b"",
        )?;
    }
    Ok(())
}

#[test]
fn jsonl_and_parquet_preserve_parent_provenance_and_rejection_nulls() -> anyhow::Result<()> {
    use arrow_array::Array;
    for basis in [
        valid_basis(),
        vec![basis_line("unit-opaque", "missing", "4")],
    ] {
        let mut config = fixture("foundation-parent-key-parquet", basis)?;
        let json = exported_row(&config)?;
        config.output_path = config.bronze_local_object_root.join("out.parquet");
        config.output_format = OutputFormat::Parquet;
        export_handoff(&config)?;
        let batch = read_first_parquet_batch(&config.output_path)?;
        for key in [
            "unit_row_id",
            "unit_designation",
            "unit_designation_normalized",
            "building_mgm_bldrgst_pk",
            "building_link_method",
            "building_link_source_record_id",
            "building_link_input_sha256",
            "building_link_reason",
            "building_main_or_annex",
        ] {
            let column = batch.column_by_name(key).expect("column");
            assert_eq!(column.is_null(0), json[key].is_null(), "{key}");
            if !json[key].is_null() {
                assert_eq!(
                    string_value(&batch, key, 0)?,
                    json[key].as_str().expect("string"),
                    "{key}"
                );
            }
        }
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn parent_linking_preserves_current_main_unit_normalization() -> anyhow::Result<()> {
    for (name, reason) in [
        ("D07-01호", "accepted_numeric_unit"),
        ("1층", "accepted_floor_space"),
        ("401,2", "merged_unit_name"),
    ] {
        let config = fixture("foundation-parent-key-current-unit-contract", valid_basis())?;
        let path = config.bronze_local_object_root.join(format!(
            "bronze/source={DEFAULT_SOURCE_SLUG}/OPN209912310000000003.zip"
        ));
        let line = unit_line("unit-opaque", "A", name, "20", "지상", "1");
        write_zip_file(&path, "mart_djy_09.txt", line.as_bytes())?;
        let row = exported_row(&config)?;
        let normalized = foundation_normalization_domain::normalize_building_register_unit(
            foundation_normalization_domain::RawBuildingRegisterUnit {
                dong_name: "A",
                unit_name: name,
                floor: foundation_normalization_domain::RawBuildingRegisterFloor {
                    floor_type_code: "20",
                    floor_type_name: "지상",
                    floor_number: "1",
                    floor_label: None,
                },
            },
        );
        assert_eq!(row["normalization_reason"], reason);
        assert_eq!(
            row["unit_number"],
            serde_json::json!(normalized.unit_number)
        );
        assert_eq!(
            row["unit_designation_normalized"],
            serde_json::json!(normalized.unit_designation_normalized)
        );
        assert_eq!(row["building_link_method"], "parent_key");
        assert_eq!(row["building_mgm_bldrgst_pk"], "building-b");
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn swapped_source_families_fail_before_output_is_created() -> anyhow::Result<()> {
    for (target, source) in [
        (DEFAULT_BASIS_SOURCE_SLUG, DEFAULT_TITLE_SOURCE_SLUG),
        (DEFAULT_BASIS_SOURCE_SLUG, DEFAULT_SOURCE_SLUG),
        (DEFAULT_TITLE_SOURCE_SLUG, DEFAULT_BASIS_SOURCE_SLUG),
        (DEFAULT_SOURCE_SLUG, DEFAULT_TITLE_SOURCE_SLUG),
    ] {
        let config = fixture("foundation-parent-key-swapped-input", valid_basis())?;
        let path = |slug| {
            config
                .bronze_local_object_root
                .join(format!("bronze/source={slug}/OPN209912310000000003.zip"))
        };
        fs::copy(path(source), path(target))?;
        let error = export_handoff(&config).expect_err("wrong source family must fail");
        assert!(error.to_string().contains("must contain"), "{error}");
        assert!(!config.output_path.exists());
        fs::remove_dir_all(config.bronze_local_object_root)?;
    }
    Ok(())
}

#[test]
fn title_kind_is_checked_even_when_basic_outline_parent_has_kind_three() -> anyhow::Result<()> {
    let config = fixture("foundation-parent-key-title-kind", valid_basis())?;
    let title = title_line("building-b", "B");
    let mut fields: Vec<_> = title.split('|').collect();
    fields[3] = "2";
    let path = config.bronze_local_object_root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/OPN209912310000000003.zip"
    ));
    write_zip_file(&path, "mart_djy_03.txt", fields.join("|").as_bytes())?;
    let row = exported_row(&config)?;
    assert!(row["building_mgm_bldrgst_pk"].is_null());
    assert_eq!(row["building_link_reason"], "parent_title_kind_mismatch");
    fs::remove_dir_all(config.bronze_local_object_root)?;
    Ok(())
}

#[test]
fn absent_parent_never_uses_an_unrelated_basic_outline_candidate() -> anyhow::Result<()> {
    for child_present in [false, true] {
        for conflict in [false, true] {
            let mut basis = vec![basis_line("building-a", "", "2")];
            if conflict {
                basis.push(basis_line("building-a", "", "3"));
            }
            if child_present {
                basis.push(basis_line("unit-opaque", "", "4"));
            }
            let config = fixture("foundation-parent-key-fallback-counterevidence", basis)?;
            let row = exported_row(&config)?;
            assert!(row["building_mgm_bldrgst_pk"].is_null());
            assert_eq!(row["building_link_method"], "unresolved");
            assert_eq!(
                row["building_link_reason"],
                if child_present {
                    "basis_parent_key_missing"
                } else {
                    "basis_unit_missing"
                }
            );
            fs::remove_dir_all(config.bronze_local_object_root)?;
        }
    }
    Ok(())
}

#[test]
fn missing_child_basis_cannot_link_a_unit_to_itself_by_name() -> anyhow::Result<()> {
    let config = fixture("foundation-parent-key-fallback-self-reference", vec![])?;
    let path = config.bronze_local_object_root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/OPN209912310000000003.zip"
    ));
    write_zip_file(
        &path,
        "mart_djy_03.txt",
        title_line("unit-opaque", "A").as_bytes(),
    )?;
    let row = exported_row(&config)?;
    assert!(row["building_mgm_bldrgst_pk"].is_null());
    assert_eq!(row["building_link_reason"], "basis_unit_missing");
    fs::remove_dir_all(config.bronze_local_object_root)?;
    Ok(())
}

#[test]
fn an_unmapped_merged_code_is_refused_before_any_output_exists() -> anyhow::Result<()> {
    // 심은 위반: 통합 시도 99 를 다스리는 크로스워크에 99999 의 짝이 없다. 사전 검증이 실물
    // 크로스워크로 돌므로, 거부는 출력 파일이 생기거나 비워지기 전에 난다.
    let config = fixture("foundation-unit-unmapped-merged", valid_basis())?;
    let crosswalk = foundation_shared_kernel::pnu::SigunguCrosswalk::new(
        std::collections::HashMap::new(),
        ["99".to_owned()],
    )?;
    let error = export_handoff_via(&config, &crosswalk)
        .err()
        .context("an unmapped merged code must stop the export")?;
    assert!(format!("{error:#}").contains("99999"), "{error:#}");
    assert!(!config.output_path.exists(), "the refusal left a handoff");
    assert!(
        !config.summary_path.as_ref().context("summary")?.exists(),
        "the refusal left a summary"
    );
    fs::remove_dir_all(config.bronze_local_object_root)?;
    Ok(())
}
