//! Scope JSONL reading and validation.
//!
//! Owns the `ScopeRow` legal-dong record, parses the scope JSONL produced upstream, and checks
//! each row against the scope evidence manifest (counts, uniqueness, provider-source policy).

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

use super::support::*;
use super::{SCOPE_ROW_SCHEMA_VERSION, SCOPE_SCHEMA_VERSION};

#[derive(Clone, Debug)]
pub(crate) struct ScopeRow {
    pub(crate) scope_unit_id: String,
    pub(crate) sigungu_cd: String,
    pub(crate) bjdong_cd: String,
    pub(crate) bjdong_code: String,
    pub(crate) source_row_count: u64,
}

pub(super) fn validate_scope_evidence(
    config: &super::config::WriterConfig,
    scope_evidence: &JsonValue,
) -> anyhow::Result<()> {
    if string_property(scope_evidence, "schema_version") != SCOPE_SCHEMA_VERSION {
        bail!("national scope evidence schema mismatch");
    }
    if string_property(scope_evidence, "status") != "ready" {
        bail!("national scope evidence status must be ready");
    }
    if string_property(scope_evidence, "output_kind") != "jsonl" {
        bail!("national scope evidence output_kind must be jsonl");
    }
    if string_property(scope_evidence, "source_kind") != "administrative_spatial_scope_registry" {
        bail!("national scope evidence source_kind must be administrative_spatial_scope_registry");
    }
    if string_property(scope_evidence, "registry_path").is_empty() {
        bail!("national scope evidence registry_path is required");
    }
    if !is_sha256(&string_property(scope_evidence, "registry_sha256")) {
        bail!("national scope evidence registry_sha256 is required");
    }
    if string_property(scope_evidence, "scope_row_schema_version") != SCOPE_ROW_SCHEMA_VERSION {
        bail!("national scope row schema mismatch");
    }
    if string_property(scope_evidence, "output_path")
        != crate::public_data_control_support::repo_relative_path(
            &config.root,
            &config.scope_jsonl_path,
        )
    {
        bail!("national scope evidence output_path must match scope JSONL");
    }
    if bool_property(scope_evidence, "completion_claim_allowed").unwrap_or(true) {
        bail!("national scope evidence must not allow completion claims");
    }
    if bool_property(scope_evidence, "production_cutover_allowed").unwrap_or(true) {
        bail!("national scope evidence must not allow production cutover");
    }
    Ok(())
}

pub(super) fn read_scope_jsonl(path: &Path) -> anyhow::Result<Vec<ScopeRow>> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read scope JSONL {}", path.display()))?;
    let raw = String::from_utf8_lossy(&bytes);
    let mut rows = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        let line_number = index + 1;
        let line = if line_number == 1 {
            line.trim_start_matches('\u{feff}')
        } else {
            line
        };
        if line.trim().is_empty() {
            bail!("scope JSONL line {line_number} must not be blank");
        }
        let value = serde_json::from_str::<JsonValue>(line)
            .with_context(|| format!("scope JSONL line {line_number} is not valid JSON"))?;
        rows.push(scope_row_from_value(value, line_number)?);
    }
    Ok(rows)
}

fn scope_row_from_value(value: JsonValue, index: usize) -> anyhow::Result<ScopeRow> {
    for field in [
        "schema_version",
        "scope_unit_id",
        "scope_kind",
        "canonical_code",
        "scope_key",
        "bjdong_code",
        "sigungu_cd",
        "bjdong_cd",
        "geometry_srid",
        "bbox",
        "source_provider",
        "source_snapshot_id",
        "source_row_count",
    ] {
        if value.get(field).is_none() || value_to_required_string(value.get(field)).is_empty() {
            bail!("scope row {index} missing {field}");
        }
    }
    if string_property(&value, "schema_version") != SCOPE_ROW_SCHEMA_VERSION {
        bail!("scope row {index} schema mismatch");
    }
    if string_property(&value, "scope_kind") != "legal_dong" {
        bail!("scope row {index} scope_kind must be legal_dong");
    }
    let scope_unit_id = string_property(&value, "scope_unit_id");
    let sigungu_cd = string_property(&value, "sigungu_cd");
    let bjdong_cd = string_property(&value, "bjdong_cd");
    if !is_digits(&sigungu_cd, 5) || !is_digits(&bjdong_cd, 5) {
        bail!("scope row {index} must use five-digit sigungu_cd and bjdong_cd");
    }
    if !bjdong_cd.ends_with("00") {
        bail!("scope row {index} must be EMD-level legal_dong for VWorld emdCd collection");
    }
    let bjdong_code = format!("{sigungu_cd}{bjdong_cd}");
    if string_property(&value, "bjdong_code") != bjdong_code {
        bail!("scope row {index} bjdong_code must equal sigungu_cd plus bjdong_cd");
    }
    if string_property(&value, "canonical_code") != bjdong_code {
        bail!("scope row {index} canonical_code must equal bjdong_code");
    }
    if string_property(&value, "scope_key") != format!("{sigungu_cd}:{bjdong_cd}") {
        bail!("scope row {index} scope_key must equal sigungu_cd:bjdong_cd");
    }
    let source_row_count = u64_property(&value, "source_row_count").unwrap_or(0);
    if source_row_count < 1 {
        bail!("scope row {index} source_row_count must be positive");
    }
    if u64_property(&value, "geometry_srid").unwrap_or(0) != 4326 {
        bail!("scope row {index} geometry_srid must be EPSG 4326");
    }
    if matches!(
        string_property(&value, "source_provider").as_str(),
        "VWorld"
            | "data.go.kr"
            | "provider-parcel"
            | "vworld_parcel_boundaries_silver_handoff_jsonl"
    ) {
        bail!("scope row {index} source_provider must not be provider parcel data");
    }
    validate_bbox(value.get("bbox"), index)?;

    Ok(ScopeRow {
        scope_unit_id,
        sigungu_cd,
        bjdong_cd,
        bjdong_code,
        source_row_count,
    })
}

pub(super) fn validate_scope_rows(
    rows: &[ScopeRow],
    scope_evidence: &JsonValue,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        bail!("scope JSONL must contain at least one row");
    }
    if u64_property(scope_evidence, "scope_row_count").unwrap_or(0)
        != u64::try_from(rows.len()).unwrap_or(u64::MAX)
    {
        bail!("national scope evidence row count must match scope JSONL");
    }
    let source_row_sum = rows.iter().map(|row| row.source_row_count).sum::<u64>();
    if u64_property(scope_evidence, "source_row_count").unwrap_or(0) != source_row_sum {
        bail!("national scope evidence source row count must match scope JSONL");
    }
    let mut scope_keys = BTreeSet::new();
    for row in rows {
        let scope_key = format!("{}:{}", row.sigungu_cd, row.bjdong_cd);
        if !scope_keys.insert(scope_key.clone()) {
            bail!("duplicate scope row: {scope_key}");
        }
    }
    Ok(())
}
