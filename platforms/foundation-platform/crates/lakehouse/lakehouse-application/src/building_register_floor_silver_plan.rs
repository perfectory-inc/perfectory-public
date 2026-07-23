//! Silver handoff helpers for official building-register floor rows.

use std::collections::{BTreeMap, HashMap};

mod columns;
mod hub_bulk;
mod public_data;

use chrono::{DateTime, Utc};
use foundation_normalization_domain::{
    building_register_floor_evidence_numbers, building_register_floor_label_is_attic,
    entity_impact_mappings_for_source, field_semantic_mappings_for_source,
    normalize_building_register_floor, resolve_building_floors, BuildingFloorCounts,
    FloorRowEvidence, NormalizedBuildingRegisterFloor, RawBuildingRegisterFloor,
};
use lakehouse_domain::{LakehouseTableContract, SILVER_BUILDING_REGISTER_FLOORS};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

use columns::{building_register_floor_transport_columns, column_names};
use foundation_normalization_domain::detect_entity_impacts;
pub use hub_bulk::parse_building_register_floor_source_row_from_hub_bulk_text_line;
pub use public_data::parse_building_register_floor_source_rows_from_public_data_json;

/// Bronze JSON object input for data.go.kr building-register floor normalization.
pub struct PublicDataBuildingRegisterFloorBronzeJsonInput<'a> {
    /// Raw Bronze JSON bytes exactly as fetched from data.go.kr.
    pub raw_payload: &'a [u8],
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// Bronze object key that carried these source rows.
    pub bronze_object_key: &'a str,
    /// UTC timestamp from which these source facts are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Input required to normalize building-register floor source rows into Silver rows.
pub struct BuildingRegisterFloorSilverRowsInput<'a> {
    /// Parsed provider source rows ordered by the caller.
    pub records: &'a [BuildingRegisterFloorSourceRow],
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// Bronze object key that carried these source rows.
    pub bronze_object_key: &'a str,
    /// UTC timestamp from which these source facts are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Parsed source-side floor fields before deterministic normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterFloorSourceRow {
    /// Stable row-level source lineage id.
    pub source_record_id: String,
    /// Provider building-register management primary key.
    pub mgm_bldrgst_pk: String,
    /// Raw provider floor-kind code.
    pub floor_type_code_raw: String,
    /// Raw provider floor-kind name.
    pub floor_type_name_raw: String,
    /// Raw provider floor-number field.
    pub floor_number_raw: String,
    /// Raw provider floor label when present in the source dataset.
    pub floor_label_raw: Option<String>,
    /// 1-based source line number inside the Bronze object when available.
    pub source_line_number: Option<u64>,
}

/// Silver `silver.building_register_floors` row prepared from one source row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterFloorSilverRow {
    /// Stable Silver row id.
    pub floor_row_id: String,
    /// Provider building-register management primary key.
    pub mgm_bldrgst_pk: String,
    /// Raw provider floor-kind code.
    pub floor_type_code_raw: String,
    /// Raw provider floor-kind name.
    pub floor_type_name_raw: String,
    /// Raw provider floor-number field.
    pub floor_number_raw: String,
    /// Raw provider floor label when present in the source dataset.
    pub floor_label_raw: Option<String>,
    /// Canonical floor-kind wire value.
    pub floor_kind: String,
    /// Canonical floor number when deterministic normalization accepted one.
    pub floor_number: Option<u16>,
    /// Signed floor position: above-ground positive, basement negative.
    pub floor_index: Option<i16>,
    /// Korean display label derived from deterministic rules.
    pub floor_display_ko: Option<String>,
    /// Normalization status wire value.
    pub normalization_status: String,
    /// Normalization reason wire value.
    pub normalization_reason: String,
    /// Stable row-level source lineage id.
    pub source_record_id: String,
    /// Source-snapshot lineage id.
    pub source_snapshot_id: String,
    /// Bronze object key that carried this source row.
    pub bronze_object_key: String,
    /// 1-based source line number inside the Bronze object when available.
    pub source_line_number: Option<u64>,
    /// UTC timestamp from which this fact is valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp until which this fact is valid.
    pub valid_to_utc: Option<DateTime<Utc>>,
    /// UTC timestamp when this row entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
    /// Lowercase SHA-256 checksum of the row payload excluding this checksum field.
    pub row_checksum_sha256: String,
}

/// Writer-neutral JSONL handoff for `silver.building_register_floors`.
///
/// This is transient transport for writers and tests. The canonical lakehouse table storage remains
/// the `LakehouseTableContract` physical format, currently `Parquet`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterFloorSilverHandoff {
    /// Static lakehouse contract table name.
    pub contract_table_name: &'static str,
    /// Target table columns in static contract order.
    pub table_columns: Vec<String>,
    /// JSONL transport columns in stable writer input order.
    pub transport_columns: Vec<String>,
    /// Newline-delimited JSON records for a downstream writer, not final lakehouse storage.
    pub jsonl: String,
    /// Quality metrics keyed using the same convention as `SparkRunSummary`.
    pub quality_metrics: BTreeMap<String, u64>,
    /// Number of distinct source snapshots represented by the handoff.
    pub source_snapshot_count: u64,
    /// Distinct source snapshot ids represented by the handoff.
    pub source_snapshot_ids: Vec<String>,
    /// Whether `source_snapshot_ids` was truncated by this builder.
    pub source_snapshot_truncated: bool,
}

/// JSONL handoff containing only rows that require AI normalization proposals.
///
/// This is intelligence-platform proposal input. It is not a Silver table and is never applied to
/// canonical storage without the proposal inbox and human review gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterFloorNormalizationProposalInput {
    /// Stable input schema version for intelligence-platform consumers.
    pub schema_version: &'static str,
    /// Newline-delimited proposal-input records, not final lakehouse storage.
    pub jsonl: String,
    /// Number of proposal-input records emitted.
    pub proposal_count: u64,
}

/// Input for building-scoped floor normalization context packs.
pub struct BuildingRegisterFloorEntityContextPackInput<'a> {
    /// Already-normalized Silver handoff rows used as local building context.
    pub rows: &'a [BuildingRegisterFloorSilverRow],
}

/// Silver handoff plus unresolved-row AI proposal input from the same normalized rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterFloorSilverOutputs {
    /// Writer-neutral Silver table handoff.
    pub silver_handoff: BuildingRegisterFloorSilverHandoff,
    /// AI proposal input containing only unresolved rows.
    pub normalization_proposal_input: BuildingRegisterFloorNormalizationProposalInput,
}

/// Error returned while normalizing building-register floors into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BuildingRegisterFloorSilverPlanError {
    /// Input data cannot be represented as a Silver building-register floor row.
    #[error("invalid building-register floor Silver input: {0}")]
    InvalidInput(String),
}

/// Builds a Silver handoff directly from one data.go.kr Bronze JSON payload.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when JSON parsing, source-row parsing,
/// deterministic normalization, or handoff building fails.
pub fn build_building_register_floor_silver_handoff_from_public_data_bronze_json(
    input: &PublicDataBuildingRegisterFloorBronzeJsonInput<'_>,
) -> Result<BuildingRegisterFloorSilverHandoff, BuildingRegisterFloorSilverPlanError> {
    let rows = normalize_building_register_floor_silver_rows_from_public_data_bronze_json(input)?;
    build_building_register_floor_silver_handoff(&rows)
}

/// Builds both Silver handoff and AI proposal input from one Bronze JSON payload.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when JSON parsing, deterministic
/// normalization, Silver handoff building, or proposal input building fails.
pub fn build_building_register_floor_silver_outputs_from_public_data_bronze_json(
    input: &PublicDataBuildingRegisterFloorBronzeJsonInput<'_>,
) -> Result<BuildingRegisterFloorSilverOutputs, BuildingRegisterFloorSilverPlanError> {
    let rows = normalize_building_register_floor_silver_rows_from_public_data_bronze_json(input)?;
    Ok(BuildingRegisterFloorSilverOutputs {
        silver_handoff: build_building_register_floor_silver_handoff(&rows)?,
        normalization_proposal_input: build_building_register_floor_normalization_proposal_input(
            &rows,
        )?,
    })
}

/// Normalizes building-register floor source rows into Silver rows.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when required lineage or source identity is
/// empty, or when row JSON serialization fails while computing checksums.
pub fn normalize_building_register_floor_silver_rows(
    input: &BuildingRegisterFloorSilverRowsInput<'_>,
) -> Result<Vec<BuildingRegisterFloorSilverRow>, BuildingRegisterFloorSilverPlanError> {
    normalize_building_register_floor_silver_rows_with_title_counts(input, &HashMap::new())
}

/// Normalizes building-register floor source rows into Silver rows, using
/// 표제부 (building-title) floor counts as the third witness for building-level
/// contradiction resolution.
///
/// `title_floor_counts` maps `mgm_bldrgst_pk` to the 지상층수 / 지하층수 counts.
/// Buildings absent from the map fall back to the two internal witnesses only
/// (identical to [`normalize_building_register_floor_silver_rows`]).
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when required lineage or source identity is
/// empty, or when row JSON serialization fails while computing checksums.
pub fn normalize_building_register_floor_silver_rows_with_title_counts<
    S: std::hash::BuildHasher,
>(
    input: &BuildingRegisterFloorSilverRowsInput<'_>,
    title_floor_counts: &HashMap<String, BuildingFloorCounts, S>,
) -> Result<Vec<BuildingRegisterFloorSilverRow>, BuildingRegisterFloorSilverPlanError> {
    validate_lineage_part("source_snapshot_id", input.source_snapshot_id)?;
    validate_lineage_part("bronze_object_key", input.bronze_object_key)?;

    // Pass 1: per-row deterministic normalization plus the two witness numbers.
    let mut normalized: Vec<NormalizedBuildingRegisterFloor> =
        Vec::with_capacity(input.records.len());
    let mut evidence: Vec<FloorRowEvidence> = Vec::with_capacity(input.records.len());
    for record in input.records {
        validate_lineage_part("source_record_id", &record.source_record_id)?;
        validate_lineage_part("mgm_bldrgst_pk", &record.mgm_bldrgst_pk)?;
        let raw = raw_floor(record);
        let per_row = normalize_building_register_floor(raw);
        let (provider_number, label_number) = building_register_floor_evidence_numbers(raw);
        evidence.push(FloorRowEvidence {
            provider_number,
            label_number,
            attic_candidate: building_register_floor_label_is_attic(raw.floor_label),
            per_row: per_row.clone(),
        });
        normalized.push(per_row);
    }

    // Pass 2: building-level contradiction resolution, one building (동) in isolation
    // so a witness majority never leaks across buildings. Records may interleave
    // buildings, so group by management key rather than assuming contiguity.
    let mut group_indices: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, record) in input.records.iter().enumerate() {
        group_indices
            .entry(record.mgm_bldrgst_pk.as_str())
            .or_default()
            .push(index);
    }
    for (pk, indices) in &group_indices {
        let counts = title_floor_counts.get(*pk).copied().unwrap_or_default();
        let group: Vec<FloorRowEvidence> = indices.iter().map(|&i| evidence[i].clone()).collect();
        let resolved = resolve_building_floors(&group, counts);
        for (local, &global) in indices.iter().enumerate() {
            normalized[global] = resolved[local].clone();
        }
    }

    // Pass 3: build Silver rows from the (possibly resolved) results.
    input
        .records
        .iter()
        .zip(normalized.iter())
        .map(|(record, per_row)| build_silver_row(record, per_row, input))
        .collect()
}

fn raw_floor(record: &BuildingRegisterFloorSourceRow) -> RawBuildingRegisterFloor<'_> {
    RawBuildingRegisterFloor {
        floor_type_code: &record.floor_type_code_raw,
        floor_type_name: &record.floor_type_name_raw,
        floor_number: &record.floor_number_raw,
        floor_label: record.floor_label_raw.as_deref(),
    }
}

/// Builds a writer-neutral JSONL handoff from Silver building-register floor rows.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when a row has invalid required fields,
/// checksum shape, or JSON serialization fails.
pub fn build_building_register_floor_silver_handoff(
    rows: &[BuildingRegisterFloorSilverRow],
) -> Result<BuildingRegisterFloorSilverHandoff, BuildingRegisterFloorSilverPlanError> {
    let mut quality_metrics = required_quality_metrics(&SILVER_BUILDING_REGISTER_FLOORS);
    quality_metrics.insert("row_count".to_owned(), rows.len() as u64);
    quality_metrics.insert("proposal_required_count".to_owned(), 0);
    quality_metrics.insert("invalid_checksum_count".to_owned(), 0);

    let mut records = Vec::with_capacity(rows.len());
    let mut source_snapshot_ids = Vec::<String>::new();

    for row in rows {
        validate_handoff_row(row, &mut quality_metrics);
        if !source_snapshot_ids.contains(&row.source_snapshot_id) {
            source_snapshot_ids.push(row.source_snapshot_id.clone());
        }
        records.push(row_to_json_value(row));
    }

    source_snapshot_ids.sort();
    let jsonl = records
        .iter()
        .map(compact_json_line)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    let jsonl = if jsonl.is_empty() {
        String::new()
    } else {
        format!("{jsonl}\n")
    };

    Ok(BuildingRegisterFloorSilverHandoff {
        contract_table_name: SILVER_BUILDING_REGISTER_FLOORS.table_name,
        table_columns: column_names(&SILVER_BUILDING_REGISTER_FLOORS),
        transport_columns: building_register_floor_transport_columns(),
        jsonl,
        quality_metrics,
        source_snapshot_count: source_snapshot_ids.len() as u64,
        source_snapshot_ids,
        source_snapshot_truncated: false,
    })
}

/// Builds an AI proposal-input handoff from unresolved Silver rows only.
///
/// Accepted deterministic rows are intentionally excluded. The AI layer receives only the rows
/// that foundation-platform marked as `proposal_required`, and the resulting proposals still need
/// to return through the foundation-platform proposal inbox and human review before canonical
/// writes.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when proposal JSON serialization fails.
pub fn build_building_register_floor_normalization_proposal_input(
    rows: &[BuildingRegisterFloorSilverRow],
) -> Result<BuildingRegisterFloorNormalizationProposalInput, BuildingRegisterFloorSilverPlanError> {
    build_building_register_floor_entity_context_pack_input(
        &BuildingRegisterFloorEntityContextPackInput { rows },
    )
}

/// Builds building-scoped floor entity context packs for intelligence-platform proposal workers.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when context-pack JSON serialization fails.
pub fn build_building_register_floor_entity_context_pack_input(
    input: &BuildingRegisterFloorEntityContextPackInput<'_>,
) -> Result<BuildingRegisterFloorNormalizationProposalInput, BuildingRegisterFloorSilverPlanError> {
    const SCHEMA_VERSION: &str = "foundation-platform.floor_entity_context_pack.v1";

    let mut rows_by_building = BTreeMap::<&str, Vec<&BuildingRegisterFloorSilverRow>>::new();
    for row in input.rows {
        rows_by_building
            .entry(row.mgm_bldrgst_pk.as_str())
            .or_default()
            .push(row);
    }

    let records = input
        .rows
        .iter()
        .filter(|row| row.normalization_status == "proposal_required")
        .map(|target_row| {
            let building_rows = rows_by_building
                .get(target_row.mgm_bldrgst_pk.as_str())
                .ok_or_else(|| {
                    BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
                        "missing floor context rows for mgm_bldrgst_pk={}",
                        target_row.mgm_bldrgst_pk
                    ))
                })?;
            let context_pack =
                floor_entity_context_pack_value(SCHEMA_VERSION, target_row, building_rows)?;
            compact_json_line(&context_pack)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let jsonl = records.join("\n");
    let jsonl = if jsonl.is_empty() {
        String::new()
    } else {
        format!("{jsonl}\n")
    };

    Ok(BuildingRegisterFloorNormalizationProposalInput {
        schema_version: SCHEMA_VERSION,
        proposal_count: records.len() as u64,
        jsonl,
    })
}

fn floor_entity_context_pack_value(
    schema_version: &str,
    target_row: &BuildingRegisterFloorSilverRow,
    building_rows: &[&BuildingRegisterFloorSilverRow],
) -> Result<JsonValue, BuildingRegisterFloorSilverPlanError> {
    let source_slug = source_slug_from_bronze_object_key(&target_row.bronze_object_key)
        .ok_or_else(|| {
            BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
                "building-register floor context pack requires canonical Bronze source key: {}",
                target_row.bronze_object_key
            ))
        })?;
    let entity_impact = entity_impact_value(source_slug, target_row)?;
    let context_pack_seed = format!(
        "{schema_version}:{}:{}",
        target_row.mgm_bldrgst_pk, target_row.row_checksum_sha256
    );
    Ok(serde_json::json!({
        "schema_version": schema_version,
        "context_pack_id": format!(
            "floor-context-pack:{}",
            sha256_hex(context_pack_seed.as_bytes())
        ),
        "source_system": "foundation-platform.silver.building_register_floors",
        "target": {
            "target_kind": "building_register_floor",
            "raw_record_id": target_row.source_record_id,
            "silver_row_id": target_row.floor_row_id,
            "bronze_object_key": target_row.bronze_object_key,
            "row_checksum_sha256": target_row.row_checksum_sha256,
            "source_snapshot_id": target_row.source_snapshot_id,
            "source_line_number": target_row.source_line_number,
        },
        "building_identity_candidate": {
            "mgm_bldrgst_pk": target_row.mgm_bldrgst_pk,
            "source_confidence": "provider_primary_key",
        },
        "entity_impact": entity_impact,
        "semantic_contract": semantic_contract_value(&target_row.bronze_object_key),
        "target_raw_floor": {
            "floor_type_code_raw": target_row.floor_type_code_raw,
            "floor_type_name_raw": target_row.floor_type_name_raw,
            "floor_number_raw": target_row.floor_number_raw,
            "floor_label_raw": target_row.floor_label_raw,
        },
        "current_deterministic_normalization": {
            "floor_kind": target_row.floor_kind,
            "floor_number": target_row.floor_number,
            "floor_index": target_row.floor_index,
            "floor_display_ko": target_row.floor_display_ko,
            "status": target_row.normalization_status,
            "reason": target_row.normalization_reason,
        },
        "same_building_floor_sequence": building_rows
            .iter()
            .map(|row| floor_sequence_context_value(row))
            .collect::<Vec<_>>(),
        "building_title_context": {
            "status": "not_available_in_current_handoff",
        },
        "unit_context_summary": {
            "status": "not_available_in_current_handoff",
        },
        "policy_context": {
            "policy_id": "foundation-platform.floor-normalization",
            "policy_version": "v1",
            "default_locale": "ko-KR",
            "machine_values_language": "en-US",
        },
        "allowed_output_contract": {
            "required_locale": "ko-KR",
            "machine_fields": [
                "floor_kind",
                "floor_number",
                "floor_index",
                "normalization_status",
                "normalization_reason"
            ],
            "localized_fields": [
                "floor_display_ko",
                "review_message_ko"
            ],
        },
        "trace": {
            "valid_from_utc": timestamp_json(target_row.valid_from_utc),
            "ingested_at_utc": timestamp_json(target_row.ingested_at_utc),
        }
    }))
}

fn entity_impact_value(
    source_slug: &str,
    target_row: &BuildingRegisterFloorSilverRow,
) -> Result<JsonValue, BuildingRegisterFloorSilverPlanError> {
    let fields = BTreeMap::from([
        (
            "mgm_bldrgst_pk".to_owned(),
            target_row.mgm_bldrgst_pk.clone(),
        ),
        (
            "floor_type_code_raw".to_owned(),
            target_row.floor_type_code_raw.clone(),
        ),
        (
            "floor_type_name_raw".to_owned(),
            target_row.floor_type_name_raw.clone(),
        ),
        (
            "floor_number_raw".to_owned(),
            target_row.floor_number_raw.clone(),
        ),
        (
            "floor_label_raw".to_owned(),
            target_row.floor_label_raw.clone().unwrap_or_default(),
        ),
    ]);
    let impact = detect_entity_impacts(source_slug, &fields)
        .into_iter()
        .next()
        .ok_or_else(|| {
            BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
                "building-register floor context pack could not detect entity impact for source={source_slug}"
            ))
        })?;
    Ok(serde_json::json!({
        "entity_type": impact.entity_type,
        "entity_key": impact.entity_key,
        "consistency_domains": impact.consistency_domains,
    }))
}

fn semantic_contract_value(bronze_object_key: &str) -> JsonValue {
    let source_slug = source_slug_from_bronze_object_key(bronze_object_key).unwrap_or("");
    serde_json::json!({
        "source_slug": source_slug,
        "field_mappings": field_semantic_mappings_for_source(source_slug)
            .iter()
            .map(|mapping| {
                serde_json::json!({
                    "field_path": mapping.field.field_path,
                    "concept_id": mapping.concept_id.as_str(),
                    "required_for_entity_context": mapping.required_for_entity_context,
                })
            })
            .collect::<Vec<_>>(),
        "entity_impacts": entity_impact_mappings_for_source(source_slug)
            .iter()
            .map(|impact| {
                serde_json::json!({
                    "entity_type": impact.entity_type.as_str(),
                    "entity_key_fields": impact.entity_key_fields,
                    "consistency_domains": impact.consistency_domains
                        .iter()
                        .map(|domain| domain.as_str())
                        .collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn source_slug_from_bronze_object_key(bronze_object_key: &str) -> Option<&str> {
    bronze_object_key
        .strip_prefix("bronze/source=")?
        .split('/')
        .next()
        .filter(|source_slug| !source_slug.is_empty())
}

fn floor_sequence_context_value(row: &BuildingRegisterFloorSilverRow) -> JsonValue {
    serde_json::json!({
        "mgm_bldrgst_pk": row.mgm_bldrgst_pk,
        "source_record_id": row.source_record_id,
        "silver_row_id": row.floor_row_id,
        "floor_type_code_raw": row.floor_type_code_raw,
        "floor_type_name_raw": row.floor_type_name_raw,
        "floor_number_raw": row.floor_number_raw,
        "floor_label_raw": row.floor_label_raw,
        "floor_kind": row.floor_kind,
        "floor_number": row.floor_number,
        "floor_index": row.floor_index,
        "floor_display_ko": row.floor_display_ko,
        "normalization_status": row.normalization_status,
        "normalization_reason": row.normalization_reason,
        "source_line_number": row.source_line_number,
    })
}

fn build_silver_row(
    record: &BuildingRegisterFloorSourceRow,
    normalized: &NormalizedBuildingRegisterFloor,
    input: &BuildingRegisterFloorSilverRowsInput<'_>,
) -> Result<BuildingRegisterFloorSilverRow, BuildingRegisterFloorSilverPlanError> {
    let mut row = BuildingRegisterFloorSilverRow {
        floor_row_id: format!("building-register-floor:{}", record.source_record_id),
        mgm_bldrgst_pk: record.mgm_bldrgst_pk.clone(),
        floor_type_code_raw: record.floor_type_code_raw.clone(),
        floor_type_name_raw: record.floor_type_name_raw.clone(),
        floor_number_raw: record.floor_number_raw.clone(),
        floor_label_raw: record.floor_label_raw.clone(),
        floor_kind: normalized.kind.wire_name().to_owned(),
        floor_number: normalized.floor_number,
        floor_index: normalized.floor_index,
        floor_display_ko: normalized.display_ko.clone(),
        normalization_status: normalized.status.wire_name().to_owned(),
        normalization_reason: normalized.reason.wire_name().to_owned(),
        source_record_id: record.source_record_id.clone(),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        bronze_object_key: input.bronze_object_key.to_owned(),
        source_line_number: record.source_line_number,
        valid_from_utc: input.valid_from_utc,
        valid_to_utc: None,
        ingested_at_utc: input.ingested_at_utc,
        row_checksum_sha256: String::new(),
    };
    row.row_checksum_sha256 = row_checksum(&row)?;
    Ok(row)
}

/// Normalizes one data.go.kr building-register floor Bronze JSON payload into Silver rows.
///
/// This keeps the deterministic floor normalization SSOT in Rust while allowing multiple
/// storage writers, including JSONL diagnostics and Parquet lakehouse handoff, to share the same
/// row construction path.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when JSON parsing or source-row parsing fails.
pub fn normalize_building_register_floor_silver_rows_from_public_data_bronze_json(
    input: &PublicDataBuildingRegisterFloorBronzeJsonInput<'_>,
) -> Result<Vec<BuildingRegisterFloorSilverRow>, BuildingRegisterFloorSilverPlanError> {
    let payload = serde_json::from_slice::<JsonValue>(input.raw_payload).map_err(|error| {
        BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
            "building-register floor Bronze JSON parse failed: {error}"
        ))
    })?;
    let records = parse_building_register_floor_source_rows_from_public_data_json(
        &payload,
        input.bronze_object_key,
    )?;
    normalize_building_register_floor_silver_rows(&BuildingRegisterFloorSilverRowsInput {
        records: &records,
        source_snapshot_id: input.source_snapshot_id,
        bronze_object_key: input.bronze_object_key,
        valid_from_utc: input.valid_from_utc,
        ingested_at_utc: input.ingested_at_utc,
    })
}

fn validate_handoff_row(
    row: &BuildingRegisterFloorSilverRow,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    record_required_string_quality("floor_row_id", &row.floor_row_id, quality_metrics);
    record_required_string_quality("mgm_bldrgst_pk", &row.mgm_bldrgst_pk, quality_metrics);
    record_required_string_quality("floor_kind", &row.floor_kind, quality_metrics);
    record_required_string_quality(
        "normalization_status",
        &row.normalization_status,
        quality_metrics,
    );
    record_required_string_quality(
        "normalization_reason",
        &row.normalization_reason,
        quality_metrics,
    );
    record_required_string_quality("source_record_id", &row.source_record_id, quality_metrics);
    record_required_string_quality(
        "source_snapshot_id",
        &row.source_snapshot_id,
        quality_metrics,
    );
    record_required_string_quality("bronze_object_key", &row.bronze_object_key, quality_metrics);
    record_required_string_quality(
        "row_checksum_sha256",
        &row.row_checksum_sha256,
        quality_metrics,
    );

    if row.normalization_status == "proposal_required" {
        increment_metric(quality_metrics, "proposal_required_count");
    }
    if !is_lowercase_sha256(&row.row_checksum_sha256) {
        increment_metric(quality_metrics, "invalid_checksum_count");
    }
}

fn required_quality_metrics(contract: &LakehouseTableContract) -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::from([("row_count".to_owned(), 0)]);
    for column in contract.columns.iter().filter(|column| column.required) {
        metrics.insert(format!("{}__null_count", column.name), 0);
        if column.logical_type == "string" {
            metrics.insert(format!("{}__empty_count", column.name), 0);
        }
    }
    metrics
}

fn record_required_string_quality(
    name: &'static str,
    value: &str,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    if value.is_empty() {
        increment_metric(quality_metrics, &format!("{name}__empty_count"));
    }
}

fn increment_metric(metrics: &mut BTreeMap<String, u64>, name: &str) {
    *metrics.entry(name.to_owned()).or_insert(0) += 1;
}

fn row_to_json_value(row: &BuildingRegisterFloorSilverRow) -> JsonValue {
    let mut record = JsonMap::new();
    record.insert(
        "floor_row_id".to_owned(),
        JsonValue::String(row.floor_row_id.clone()),
    );
    record.insert(
        "mgm_bldrgst_pk".to_owned(),
        JsonValue::String(row.mgm_bldrgst_pk.clone()),
    );
    record.insert(
        "floor_type_code_raw".to_owned(),
        JsonValue::String(row.floor_type_code_raw.clone()),
    );
    record.insert(
        "floor_type_name_raw".to_owned(),
        JsonValue::String(row.floor_type_name_raw.clone()),
    );
    record.insert(
        "floor_number_raw".to_owned(),
        JsonValue::String(row.floor_number_raw.clone()),
    );
    record.insert(
        "floor_label_raw".to_owned(),
        optional_string_json(row.floor_label_raw.as_ref()),
    );
    record.insert(
        "floor_kind".to_owned(),
        JsonValue::String(row.floor_kind.clone()),
    );
    record.insert(
        "floor_number".to_owned(),
        optional_u16_json(row.floor_number),
    );
    record.insert("floor_index".to_owned(), optional_i16_json(row.floor_index));
    record.insert(
        "floor_display_ko".to_owned(),
        optional_string_json(row.floor_display_ko.as_ref()),
    );
    record.insert(
        "normalization_status".to_owned(),
        JsonValue::String(row.normalization_status.clone()),
    );
    record.insert(
        "normalization_reason".to_owned(),
        JsonValue::String(row.normalization_reason.clone()),
    );
    record.insert(
        "source_record_id".to_owned(),
        JsonValue::String(row.source_record_id.clone()),
    );
    record.insert(
        "source_snapshot_id".to_owned(),
        JsonValue::String(row.source_snapshot_id.clone()),
    );
    record.insert(
        "bronze_object_key".to_owned(),
        JsonValue::String(row.bronze_object_key.clone()),
    );
    record.insert(
        "source_line_number".to_owned(),
        row.source_line_number
            .map_or(JsonValue::Null, JsonValue::from),
    );
    record.insert(
        "valid_from_utc".to_owned(),
        JsonValue::String(timestamp_json(row.valid_from_utc)),
    );
    record.insert("valid_to_utc".to_owned(), JsonValue::Null);
    record.insert(
        "ingested_at_utc".to_owned(),
        JsonValue::String(timestamp_json(row.ingested_at_utc)),
    );
    record.insert(
        "row_checksum_sha256".to_owned(),
        JsonValue::String(row.row_checksum_sha256.clone()),
    );
    JsonValue::Object(record)
}

fn row_checksum(
    row: &BuildingRegisterFloorSilverRow,
) -> Result<String, BuildingRegisterFloorSilverPlanError> {
    let mut payload = row_to_json_value(row);
    if let JsonValue::Object(record) = &mut payload {
        record.remove("row_checksum_sha256");
    }
    Ok(sha256_hex(compact_json_line(&payload)?.as_bytes()))
}

fn optional_string_json(value: Option<&String>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| JsonValue::String(value.clone()))
}

fn optional_u16_json(value: Option<u16>) -> JsonValue {
    value.map_or(JsonValue::Null, JsonValue::from)
}

fn optional_i16_json(value: Option<i16>) -> JsonValue {
    value.map_or(JsonValue::Null, JsonValue::from)
}

fn timestamp_json(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn compact_json_line(value: &JsonValue) -> Result<String, BuildingRegisterFloorSilverPlanError> {
    serde_json::to_string(value)
        .map_err(|error| BuildingRegisterFloorSilverPlanError::InvalidInput(error.to_string()))
}

pub(crate) fn validate_lineage_part(
    label: &'static str,
    value: &str,
) -> Result<(), BuildingRegisterFloorSilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(());
    }
    Err(BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}
