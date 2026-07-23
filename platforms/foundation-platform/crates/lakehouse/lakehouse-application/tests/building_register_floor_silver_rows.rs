//! Contract tests for building-register floor Silver row normalization.

use chrono::{DateTime, Utc};
use lakehouse_application::{
    build_building_register_floor_entity_context_pack_input,
    build_building_register_floor_normalization_proposal_input,
    build_building_register_floor_silver_handoff,
    build_building_register_floor_silver_handoff_from_public_data_bronze_json,
    normalize_building_register_floor_silver_rows,
    parse_building_register_floor_source_row_from_hub_bulk_text_line,
    parse_building_register_floor_source_rows_from_public_data_json,
    BuildingRegisterFloorEntityContextPackInput, BuildingRegisterFloorSilverRow,
    BuildingRegisterFloorSilverRowsInput, BuildingRegisterFloorSourceRow,
    PublicDataBuildingRegisterFloorBronzeJsonInput,
};
use lakehouse_domain::SILVER_BUILDING_REGISTER_FLOORS;
use serde_json::json;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FIXTURE_VALID_FROM_UTC: &str = "2026-06-20T00:00:00Z";
const FIXTURE_INGESTED_AT_UTC: &str = "2026-07-01T10:00:00Z";

#[test]
fn normalizes_building_register_floor_rows_into_silver_shape() -> TestResult {
    let rows =
        normalize_building_register_floor_silver_rows(&BuildingRegisterFloorSilverRowsInput {
            records: &[floor_row("line-42", "10", "지하", "2", Some("지층"))],
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620",
            bronze_object_key:
                "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip",
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        })?;

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.floor_row_id, "building-register-floor:line-42");
    assert_eq!(row.mgm_bldrgst_pk, "SYNTHETIC-BUILDING-0001");
    assert_eq!(row.floor_type_code_raw, "10");
    assert_eq!(row.floor_type_name_raw, "지하");
    assert_eq!(row.floor_number_raw, "2");
    assert_eq!(row.floor_label_raw.as_deref(), Some("지층"));
    assert_eq!(row.floor_kind, "basement");
    assert_eq!(row.floor_number, Some(2));
    assert_eq!(row.floor_index, Some(-2));
    assert_eq!(row.floor_display_ko.as_deref(), Some("지하 2층"));
    assert_eq!(row.normalization_status, "accepted");
    assert_eq!(
        row.normalization_reason,
        "accepted_basement_generic_label_with_number"
    );
    assert_eq!(row.source_record_id, "line-42");
    assert_eq!(
        row.source_snapshot_id,
        "hubgokr-building-register-floor-overview-20260620"
    );
    assert_eq!(
        row.bronze_object_key,
        "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip"
    );
    assert_eq!(row.source_line_number, Some(42));
    assert_eq!(row.valid_from_utc, parse_utc(FIXTURE_VALID_FROM_UTC)?);
    assert_eq!(row.ingested_at_utc, parse_utc(FIXTURE_INGESTED_AT_UTC)?);
    assert_eq!(row.row_checksum_sha256.len(), 64);
    Ok(())
}

#[test]
fn preserves_proposal_required_rows_in_silver_handoff() -> TestResult {
    let rows =
        normalize_building_register_floor_silver_rows(&BuildingRegisterFloorSilverRowsInput {
            records: &[
                floor_row("line-42", "10", "지하", "2", Some("지층")),
                floor_row("line-43", "10", "지하", "1", Some("1층")),
            ],
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620",
            bronze_object_key:
                "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip",
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        })?;

    assert_eq!(rows[0].normalization_status, "accepted");
    assert_eq!(rows[1].normalization_status, "proposal_required");
    assert_eq!(rows[1].normalization_reason, "label_kind_mismatch");
    assert_eq!(rows[1].floor_display_ko, None);

    let handoff = build_building_register_floor_silver_handoff(&rows)?;

    assert_eq!(
        handoff.contract_table_name,
        "silver.building_register_floors"
    );
    assert_eq!(
        handoff.table_columns,
        SILVER_BUILDING_REGISTER_FLOORS
            .columns
            .iter()
            .map(|column| column.name.to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(handoff.quality_metrics["row_count"], 2);
    assert_eq!(handoff.quality_metrics["proposal_required_count"], 1);
    assert_eq!(handoff.source_snapshot_count, 1);
    assert_eq!(
        handoff.source_snapshot_ids,
        vec!["hubgokr-building-register-floor-overview-20260620".to_owned()]
    );

    let lines = handoff.jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let first: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(first["floor_kind"], "basement");
    assert_eq!(first["floor_display_ko"], "지하 2층");
    let second: serde_json::Value = serde_json::from_str(lines[1])?;
    assert_eq!(second["normalization_status"], "proposal_required");
    assert!(second["floor_display_ko"].is_null());
    Ok(())
}

#[test]
fn builds_floor_entity_context_pack_for_proposal_required_rows() -> TestResult {
    let rows = vec![
        silver_row_with_building(
            "line-41",
            "SYNTHETIC-BUILDING-0001",
            "accepted",
            "accepted_exact_label",
        )?,
        silver_row_with_building(
            "line-43",
            "SYNTHETIC-BUILDING-0001",
            "proposal_required",
            "label_kind_mismatch",
        )?,
    ];

    let handoff = build_building_register_floor_normalization_proposal_input(&rows)?;

    assert_eq!(
        handoff.schema_version,
        "foundation-platform.floor_entity_context_pack.v1"
    );
    assert_eq!(handoff.proposal_count, 1);
    let lines = handoff.jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let proposal: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(
        proposal["schema_version"],
        "foundation-platform.floor_entity_context_pack.v1"
    );
    assert_eq!(proposal["target"]["target_kind"], "building_register_floor");
    assert_eq!(proposal["target"]["raw_record_id"], "line-43");
    assert_eq!(
        proposal["building_identity_candidate"]["mgm_bldrgst_pk"],
        "SYNTHETIC-BUILDING-0001"
    );
    assert_eq!(
        proposal["target_raw_floor"]["floor_label_raw"],
        "ambiguous_label"
    );
    assert_eq!(
        proposal["current_deterministic_normalization"]["status"],
        "proposal_required"
    );
    assert_eq!(
        proposal["current_deterministic_normalization"]["reason"],
        "label_kind_mismatch"
    );
    assert_eq!(
        proposal["same_building_floor_sequence"]
            .as_array()
            .ok_or("same_building_floor_sequence must be array")?
            .len(),
        2
    );
    assert_eq!(
        proposal["semantic_contract"]["source_slug"],
        "hubgokr__building_register_floor_overview"
    );
    let field_mappings = proposal["semantic_contract"]["field_mappings"]
        .as_array()
        .ok_or("semantic_contract.field_mappings must be array")?;
    assert!(field_mappings.iter().any(|mapping| {
        mapping["field_path"] == "floor_number_raw" && mapping["concept_id"] == "floor_number"
    }));
    assert!(field_mappings.iter().any(|mapping| {
        mapping["field_path"] == "floor_type_code_raw" && mapping["concept_id"] == "floor_kind_code"
    }));
    assert!(field_mappings.iter().any(|mapping| {
        mapping["field_path"] == "floor_type_name_raw" && mapping["concept_id"] == "floor_kind_name"
    }));
    assert_eq!(proposal["entity_impact"]["entity_type"], "building");
    assert_eq!(
        proposal["entity_impact"]["entity_key"],
        "SYNTHETIC-BUILDING-0001"
    );
    assert_eq!(
        proposal["entity_impact"]["consistency_domains"],
        json!([
            "floor_label_normalization",
            "floor_kind_consistency",
            "building_floor_sequence_consistency",
        ])
    );
    assert_eq!(proposal["policy_context"]["default_locale"], "ko-KR");
    assert_eq!(
        proposal["allowed_output_contract"]["required_locale"],
        "ko-KR"
    );
    assert!(!handoff.jsonl.contains("line-999"));
    Ok(())
}

#[test]
fn floor_context_pack_does_not_mix_different_buildings() -> TestResult {
    let rows = vec![
        silver_row_with_building("line-41", "building-a", "accepted", "accepted_exact_label")?,
        silver_row_with_building("line-42", "building-b", "accepted", "accepted_exact_label")?,
        silver_row_with_building(
            "line-43",
            "building-a",
            "proposal_required",
            "label_kind_mismatch",
        )?,
    ];

    let handoff = build_building_register_floor_normalization_proposal_input(&rows)?;
    let proposal: serde_json::Value = serde_json::from_str(
        handoff
            .jsonl
            .lines()
            .next()
            .ok_or("expected one context pack")?,
    )?;
    let sequence = proposal["same_building_floor_sequence"]
        .as_array()
        .ok_or("same_building_floor_sequence must be array")?;

    assert_eq!(
        proposal["building_identity_candidate"]["mgm_bldrgst_pk"],
        "building-a"
    );
    assert_eq!(sequence.len(), 2);
    assert!(sequence
        .iter()
        .all(|row| row["mgm_bldrgst_pk"] == "building-a"));
    Ok(())
}

#[test]
fn accepted_only_floor_rows_do_not_generate_context_packs() -> TestResult {
    let rows = vec![
        silver_row_with_building("line-41", "building-a", "accepted", "accepted_exact_label")?,
        silver_row_with_building("line-42", "building-a", "accepted", "accepted_exact_label")?,
    ];

    let handoff = build_building_register_floor_normalization_proposal_input(&rows)?;

    assert_eq!(handoff.proposal_count, 0);
    assert!(handoff.jsonl.is_empty());
    Ok(())
}

#[test]
fn public_context_pack_builder_matches_existing_proposal_input() -> TestResult {
    let rows = vec![silver_row_with_building(
        "line-43",
        "SYNTHETIC-BUILDING-0001",
        "proposal_required",
        "label_kind_mismatch",
    )?];

    let direct = build_building_register_floor_entity_context_pack_input(
        &BuildingRegisterFloorEntityContextPackInput { rows: &rows },
    )?;
    let legacy_entrypoint = build_building_register_floor_normalization_proposal_input(&rows)?;

    assert_eq!(direct.schema_version, legacy_entrypoint.schema_version);
    assert_eq!(direct.jsonl, legacy_entrypoint.jsonl);
    Ok(())
}

#[test]
fn rejects_floor_rows_without_stable_lineage() -> TestResult {
    let result =
        normalize_building_register_floor_silver_rows(&BuildingRegisterFloorSilverRowsInput {
            records: &[floor_row("", "10", "지하", "1", Some("지1"))],
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620",
            bronze_object_key:
                "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip",
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        });

    assert!(result
        .err()
        .ok_or("empty source_record_id must be rejected")?
        .to_string()
        .contains("source_record_id must be non-empty"));
    Ok(())
}

#[test]
fn parses_public_data_floor_overview_json_into_silver_source_rows() -> TestResult {
    let payload = json!({
        "response": {
            "body": {
                "items": {
                    "item": [
                        {
                            "mgmBldrgstPk": "11680-10300-1",
                            "flrGbCd": "10",
                            "flrGbCdNm": "지하",
                            "flrNo": "2",
                            "flrNoNm": "지층"
                        },
                        {
                            "mgmBldrgstPk": "11680-10300-2",
                            "flrGbCd": 20,
                            "flrGbCdNm": "지상",
                            "flrNo": 15,
                            "flrNoNm": "지상15층"
                        }
                    ]
                }
            }
        }
    });

    let rows = parse_building_register_floor_source_rows_from_public_data_json(
        &payload,
        "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json",
    )?;

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        floor_source_row(
            "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json#item-000001",
            "11680-10300-1",
            "10",
            "지하",
            "2",
            Some("지층"),
            Some(1),
        )
    );
    assert_eq!(
        rows[1],
        floor_source_row(
            "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json#item-000002",
            "11680-10300-2",
            "20",
            "지상",
            "15",
            Some("지상15층"),
            Some(2),
        )
    );
    Ok(())
}

#[test]
fn parses_hub_bulk_floor_overview_text_line_into_silver_source_row() -> TestResult {
    let line = "SYNTHETIC-FLOOR-0001|SYNTHETIC LOT ADDRESS 0001|SYNTHETIC ROAD ADDRESS 0001||00000|00000|0|0001|0000||||SYNTHETIC-ROAD-CODE-0001|00001|0|1|0||10|지하|1|지하층|11|벽돌구조|합성구조|01001|단독주택|주택|1.00|0|주건축물||20991231";
    let bronze_object_key =
        "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";

    let row = parse_building_register_floor_source_row_from_hub_bulk_text_line(
        line,
        bronze_object_key,
        2,
    )?;

    assert_eq!(
        row,
        floor_source_row(
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip#line-000002",
            "SYNTHETIC-FLOOR-0001",
            "10",
            "지하",
            "1",
            Some("지하층"),
            Some(2),
        )
    );
    Ok(())
}

#[test]
fn preserves_hub_bulk_floor_rows_with_missing_floor_type_code_for_review() -> TestResult {
    let mut fields = vec![""; 35];
    fields[0] = "SYNTHETIC-FLOOR-0001";
    fields[20] = "1";
    fields[21] = "B1";
    let line = fields.join("|");
    let bronze_object_key =
        "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";

    let row = parse_building_register_floor_source_row_from_hub_bulk_text_line(
        &line,
        bronze_object_key,
        35,
    )?;
    let rows =
        normalize_building_register_floor_silver_rows(&BuildingRegisterFloorSilverRowsInput {
            records: &[row],
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620",
            bronze_object_key,
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        })?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].floor_type_code_raw, "");
    assert_eq!(rows[0].normalization_status, "proposal_required");
    assert_eq!(rows[0].normalization_reason, "unknown_floor_type");
    assert_eq!(rows[0].source_line_number, Some(35));
    Ok(())
}

#[test]
fn parses_single_public_data_floor_item_object() -> TestResult {
    let payload = json!({
        "response": {
            "body": {
                "items": {
                    "item": {
                        "mgmBldrgstPk": "11680-10300-1",
                        "flrGbCd": "10",
                        "flrGbCdNm": "지하",
                        "flrNo": "1",
                        "flrNoNm": "지1층"
                    }
                }
            }
        }
    });

    let rows = parse_building_register_floor_source_rows_from_public_data_json(
        &payload,
        "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json",
    )?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].floor_label_raw.as_deref(), Some("지1층"));
    assert_eq!(rows[0].source_line_number, Some(1));
    Ok(())
}

#[test]
fn builds_silver_handoff_from_public_data_bronze_json_bytes() -> TestResult {
    let payload = json!({
        "response": {
            "body": {
                "items": {
                    "item": [
                        {
                            "mgmBldrgstPk": "11680-10300-1",
                            "flrGbCd": "10",
                            "flrGbCdNm": "지하",
                            "flrNo": "1",
                            "flrNoNm": "지1층"
                        }
                    ]
                }
            }
        }
    });
    let bronze_object_key =
        "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json";

    let handoff = build_building_register_floor_silver_handoff_from_public_data_bronze_json(
        &PublicDataBuildingRegisterFloorBronzeJsonInput {
            raw_payload: &serde_json::to_vec(&payload)?,
            source_snapshot_id: "datagokr-building-register-floor-overview-11680-10300-000001",
            bronze_object_key,
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        },
    )?;

    assert_eq!(
        handoff.contract_table_name,
        "silver.building_register_floors"
    );
    assert_eq!(handoff.quality_metrics["row_count"], 1);
    assert_eq!(handoff.quality_metrics["proposal_required_count"], 0);
    let row: serde_json::Value = serde_json::from_str(
        handoff
            .jsonl
            .lines()
            .next()
            .ok_or("expected one Silver JSONL row")?,
    )?;
    assert_eq!(row["bronze_object_key"], bronze_object_key);
    assert_eq!(
        row["source_record_id"],
        format!("{bronze_object_key}#item-000001")
    );
    assert_eq!(row["floor_kind"], "basement");
    assert_eq!(row["floor_display_ko"], "지하 1층");
    Ok(())
}

fn floor_row(
    source_record_id: &str,
    floor_type_code_raw: &str,
    floor_type_name_raw: &str,
    floor_number_raw: &str,
    floor_label_raw: Option<&str>,
) -> BuildingRegisterFloorSourceRow {
    BuildingRegisterFloorSourceRow {
        source_record_id: source_record_id.to_owned(),
        mgm_bldrgst_pk: "SYNTHETIC-BUILDING-0001".to_owned(),
        floor_type_code_raw: floor_type_code_raw.to_owned(),
        floor_type_name_raw: floor_type_name_raw.to_owned(),
        floor_number_raw: floor_number_raw.to_owned(),
        floor_label_raw: floor_label_raw.map(str::to_owned),
        source_line_number: Some(
            source_record_id
                .strip_prefix("line-")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
        ),
    }
}

fn floor_source_row(
    source_record_id: &str,
    mgm_bldrgst_pk: &str,
    floor_type_code_raw: &str,
    floor_type_name_raw: &str,
    floor_number_raw: &str,
    floor_label_raw: Option<&str>,
    source_line_number: Option<u64>,
) -> BuildingRegisterFloorSourceRow {
    BuildingRegisterFloorSourceRow {
        source_record_id: source_record_id.to_owned(),
        mgm_bldrgst_pk: mgm_bldrgst_pk.to_owned(),
        floor_type_code_raw: floor_type_code_raw.to_owned(),
        floor_type_name_raw: floor_type_name_raw.to_owned(),
        floor_number_raw: floor_number_raw.to_owned(),
        floor_label_raw: floor_label_raw.map(str::to_owned),
        source_line_number,
    }
}

fn silver_row(
    source_record_id: &str,
    normalization_status: &str,
    normalization_reason: &str,
) -> TestResult<BuildingRegisterFloorSilverRow> {
    Ok(BuildingRegisterFloorSilverRow {
        floor_row_id: format!("building-register-floor:{source_record_id}"),
        mgm_bldrgst_pk: "SYNTHETIC-BUILDING-0001".to_owned(),
        floor_type_code_raw: "10".to_owned(),
        floor_type_name_raw: "basement_raw".to_owned(),
        floor_number_raw: "1".to_owned(),
        floor_label_raw: Some("ambiguous_label".to_owned()),
        floor_kind: "basement".to_owned(),
        floor_number: None,
        floor_index: None,
        floor_display_ko: None,
        normalization_status: normalization_status.to_owned(),
        normalization_reason: normalization_reason.to_owned(),
        source_record_id: source_record_id.to_owned(),
        source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
        bronze_object_key:
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip"
                .to_owned(),
        source_line_number: Some(43),
        valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
        valid_to_utc: None,
        ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        row_checksum_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
    })
}

fn silver_row_with_building(
    source_record_id: &str,
    mgm_bldrgst_pk: &str,
    normalization_status: &str,
    normalization_reason: &str,
) -> TestResult<BuildingRegisterFloorSilverRow> {
    let mut row = silver_row(source_record_id, normalization_status, normalization_reason)?;
    mgm_bldrgst_pk.clone_into(&mut row.mgm_bldrgst_pk);
    Ok(row)
}

fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}
