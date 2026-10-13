//! Silver `parcel_boundaries` R2 handoff: the execution evidence that names the shards and the
//! row parser both the PostGIS mirror and the R2 marker-anchor rebuilds read (ADR-0194 §2-1).

use std::{fs, path::Path};

use anyhow::{bail, Context};
use serde::Deserialize;
use serde_json::json;

/// Source geometry SRID of the Silver handoff rows.
pub const SOURCE_SRID: i32 = 4326;
const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.silver_gold_national_promotion_execution.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffObject {
    /// Silver handoff shard id from the execution evidence.
    pub shard_id: String,
    /// R2 object key containing the shard JSONL handoff.
    pub object_key: String,
    /// Expected JSONL row count for this object, when the evidence names one.
    ///
    /// The API-lane execution evidence carries a per-shard count; the ADR-0067 contract names
    /// objects without row counts, so its loads are judged on the national total instead
    /// (root ADR-0082).
    pub row_count: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionEvidence {
    /// Number of selected handoff objects.
    pub object_count: u64,
    /// Expected total row count across all handoff objects.
    pub expected_row_count: u64,
    /// Ordered R2 handoff objects to process.
    pub objects: Vec<HandoffObject>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawExecutionEvidence {
    schema_version: String,
    status: String,
    output_storage_driver: String,
    summary: RawExecutionSummary,
    shard_results: Vec<RawShardResult>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawExecutionSummary {
    #[serde(rename = "selected_shard_count")]
    selected_shards: u64,
    #[serde(rename = "succeeded_shard_count")]
    succeeded_shards: u64,
    #[serde(rename = "failed_shard_count")]
    failed_shards: u64,
    #[serde(rename = "output_row_count")]
    output_rows: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct RawShardResult {
    shard_id: String,
    status: String,
    output_storage_driver: String,
    output_object_key: String,
    output_row_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct SilverHandoffRow {
    boundary_id: String,
    pnu: String,
    jibun: Option<String>,
    bonbun: Option<String>,
    bubun: Option<String>,
    geometry_wkb_hex: String,
    geometry_wkb_encoding: String,
    geometry_srid: i32,
    bbox_min_x: f64,
    bbox_min_y: f64,
    bbox_max_x: f64,
    bbox_max_y: f64,
    geometry_checksum_sha256: String,
    source_record_id: String,
    source_snapshot_id: String,
    valid_from_utc: String,
    valid_to_utc: Option<String>,
    ingested_at_utc: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageRow {
    /// Canonical 19-digit parcel number.
    pub pnu: String,
    /// Source row identity for the parcel boundary.
    pub boundary_id: String,
    /// Lowercase hex-encoded WKB geometry in EPSG:4326.
    pub geometry_wkb_hex: String,
    /// Lowercase SHA-256 checksum for the source geometry WKB.
    pub geometry_checksum_sha256: String,
    /// Traceable JSON properties copied into serving projections.
    pub properties_json: String,
}

/// Reads and validates the national Silver handoff execution evidence.
pub fn read_execution_evidence(path: &Path) -> anyhow::Result<ExecutionEvidence> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read execution evidence {}", path.display()))?;
    let raw: RawExecutionEvidence = serde_json::from_slice(strip_utf8_bom(&bytes))
        .with_context(|| format!("execution evidence is not valid JSON: {}", path.display()))?;
    execution_evidence_from_raw(raw)
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes)
}

fn execution_evidence_from_raw(raw: RawExecutionEvidence) -> anyhow::Result<ExecutionEvidence> {
    if raw.schema_version != EXECUTION_SCHEMA_VERSION {
        bail!("execution evidence schema_version mismatch");
    }
    if raw.status != "ready" {
        bail!("execution evidence status must be ready");
    }
    if raw.output_storage_driver != "r2" {
        bail!("execution evidence output_storage_driver must be r2");
    }
    if raw.summary.failed_shards != 0 {
        bail!("execution evidence contains failed shards");
    }
    if raw.summary.selected_shards != raw.summary.succeeded_shards {
        bail!("execution evidence selected/succeeded shard count mismatch");
    }

    let mut objects = Vec::with_capacity(raw.shard_results.len());
    let mut row_sum = 0_u64;
    for shard in raw.shard_results {
        if shard.status != "succeeded" {
            bail!("shard {} did not succeed", shard.shard_id);
        }
        if shard.output_storage_driver != "r2" {
            bail!("shard {} output_storage_driver must be r2", shard.shard_id);
        }
        validate_object_key(shard.output_object_key.as_str())?;
        row_sum = row_sum
            .checked_add(shard.output_row_count)
            .context("execution evidence output row count overflow")?;
        objects.push(HandoffObject {
            shard_id: shard.shard_id,
            object_key: shard.output_object_key,
            row_count: Some(shard.output_row_count),
        });
    }

    if u64::try_from(objects.len()).context("object count overflow")? != raw.summary.selected_shards
    {
        bail!("execution evidence shard_results count mismatch");
    }
    if row_sum != raw.summary.output_rows {
        bail!("execution evidence shard row sum mismatch");
    }
    if row_sum == 0 {
        bail!("execution evidence output_row_count must be positive");
    }

    Ok(ExecutionEvidence {
        object_count: raw.summary.selected_shards,
        expected_row_count: row_sum,
        objects,
    })
}

/// Parses one Silver handoff JSONL row into the COPY stage representation.
pub fn parse_stage_row(
    line: &[u8],
    source_object_key: &str,
    line_number: u64,
) -> anyhow::Result<StageRow> {
    let row: SilverHandoffRow = serde_json::from_slice(line)
        .with_context(|| format!("handoff line {line_number} is not valid JSON"))?;
    validate_silver_handoff_row(&row, line_number)?;
    let properties = json!({
        "boundary_id": row.boundary_id,
        "handoff_source_snapshot_id": row.source_snapshot_id,
        "handoff_source_record_id": row.source_record_id,
        "source_object_key": source_object_key,
        "jibun": row.jibun,
        "bonbun": row.bonbun,
        "bubun": row.bubun,
        "bbox": {
            "min_x": row.bbox_min_x,
            "min_y": row.bbox_min_y,
            "max_x": row.bbox_max_x,
            "max_y": row.bbox_max_y
        },
        "valid_from_utc": row.valid_from_utc,
        "valid_to_utc": row.valid_to_utc,
        "ingested_at_utc": row.ingested_at_utc
    });

    Ok(StageRow {
        pnu: row.pnu,
        boundary_id: row.boundary_id,
        geometry_wkb_hex: row.geometry_wkb_hex,
        geometry_checksum_sha256: row.geometry_checksum_sha256,
        properties_json: serde_json::to_string(&properties)
            .context("failed to serialize PostGIS mirror row properties")?,
    })
}

fn validate_silver_handoff_row(row: &SilverHandoffRow, line_number: u64) -> anyhow::Result<()> {
    if !is_pnu(&row.pnu) {
        bail!("handoff line {line_number} pnu must be 19 digits");
    }
    if row.boundary_id.trim().is_empty() {
        bail!("handoff line {line_number} boundary_id must not be empty");
    }
    if row.geometry_wkb_encoding != "hex" {
        bail!("handoff line {line_number} geometry_wkb_encoding must be hex");
    }
    if row.geometry_srid != SOURCE_SRID {
        bail!("handoff line {line_number} geometry_srid must be {SOURCE_SRID}");
    }
    if !is_lowercase_even_hex(&row.geometry_wkb_hex) {
        bail!("handoff line {line_number} geometry_wkb_hex must be lowercase even-length hex");
    }
    if !is_lowercase_sha256(&row.geometry_checksum_sha256) {
        bail!("handoff line {line_number} geometry_checksum_sha256 must be lowercase sha256");
    }
    for (name, value) in [
        ("bbox_min_x", row.bbox_min_x),
        ("bbox_min_y", row.bbox_min_y),
        ("bbox_max_x", row.bbox_max_x),
        ("bbox_max_y", row.bbox_max_y),
    ] {
        if !value.is_finite() {
            bail!("handoff line {line_number} {name} must be finite");
        }
    }
    if row.bbox_max_x < row.bbox_min_x || row.bbox_max_y < row.bbox_min_y {
        bail!("handoff line {line_number} bbox must be ordered");
    }
    Ok(())
}

/// Appends one tab-delimited CSV row accepted by `PostgreSQL` `COPY`.
pub fn push_copy_csv_row(buffer: &mut Vec<u8>, row: &StageRow, source_object_key: &str) {
    push_copy_csv_field(buffer, Some(row.pnu.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.boundary_id.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(source_object_key));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.geometry_wkb_hex.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.geometry_checksum_sha256.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.properties_json.as_str()));
    buffer.push(b'\n');
}

fn push_copy_csv_field(buffer: &mut Vec<u8>, value: Option<&str>) {
    let Some(value) = value else {
        buffer.extend_from_slice(br"\N");
        return;
    };
    buffer.push(b'"');
    for byte in value.bytes() {
        if byte == b'"' {
            buffer.extend_from_slice(b"\"\"");
        } else {
            buffer.push(byte);
        }
    }
    buffer.push(b'"');
}

/// Validates the namespaced source snapshot id lineage shape.
pub fn validate_source_snapshot_id(value: &str) -> anyhow::Result<()> {
    if value.trim() != value || value.len() < 3 || value.len() > 256 {
        bail!("source snapshot id length or padding is invalid");
    }
    let Some((namespace, body)) = value.split_once(':') else {
        bail!("source snapshot id must use <namespace>:<id> format");
    };
    if namespace.len() < 2 || body.len() < 3 {
        bail!("source snapshot id namespace or body length is invalid");
    }
    if value.contains('/') || value.contains('\\') || value.contains("..") {
        bail!("source snapshot id must not contain path separators or traversal markers");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        bail!("source snapshot id contains invalid characters");
    }
    Ok(())
}

pub fn validate_object_key(value: &str) -> anyhow::Result<()> {
    if value.trim() != value || value.is_empty() {
        bail!("object key must not be empty or padded");
    }
    if value.starts_with('/') || value.contains('\\') || value.contains("//") {
        bail!("object key must be provider-relative and normalized");
    }
    if value
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        bail!("object key must not contain empty, '.', or '..' segments");
    }
    Ok(())
}

fn is_pnu(value: &str) -> bool {
    value.len() == 19 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_lowercase_even_hex(value: &str) -> bool {
    !value.is_empty()
        && value.len().is_multiple_of(2)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;
    use uuid::Uuid;

    #[test]
    fn execution_evidence_selects_r2_succeeded_handoff_objects() -> anyhow::Result<()> {
        let evidence = execution_evidence_from_raw(RawExecutionEvidence {
            schema_version: EXECUTION_SCHEMA_VERSION.to_owned(),
            status: "ready".to_owned(),
            output_storage_driver: "r2".to_owned(),
            summary: RawExecutionSummary {
                selected_shards: 2,
                succeeded_shards: 2,
                failed_shards: 0,
                output_rows: 3,
            },
            shard_results: vec![
                RawShardResult {
                    shard_id: "silver-parcel-boundaries-vworld-0001".to_owned(),
                    status: "succeeded".to_owned(),
                    output_storage_driver: "r2".to_owned(),
                    output_object_key: "silver-handoff/a/part-0001.jsonl".to_owned(),
                    output_row_count: 1,
                },
                RawShardResult {
                    shard_id: "silver-parcel-boundaries-vworld-0002".to_owned(),
                    status: "succeeded".to_owned(),
                    output_storage_driver: "r2".to_owned(),
                    output_object_key: "silver-handoff/a/part-0002.jsonl".to_owned(),
                    output_row_count: 2,
                },
            ],
        })?;

        assert_eq!(evidence.object_count, 2);
        assert_eq!(evidence.expected_row_count, 3);
        assert_eq!(
            evidence.objects[1].object_key,
            "silver-handoff/a/part-0002.jsonl"
        );
        Ok(())
    }

    #[test]
    fn execution_evidence_rejects_incomplete_shards() {
        let error = execution_evidence_from_raw(RawExecutionEvidence {
            schema_version: EXECUTION_SCHEMA_VERSION.to_owned(),
            status: "ready".to_owned(),
            output_storage_driver: "r2".to_owned(),
            summary: RawExecutionSummary {
                selected_shards: 1,
                succeeded_shards: 0,
                failed_shards: 1,
                output_rows: 1,
            },
            shard_results: Vec::new(),
        })
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

        assert!(error.contains("failed shards"));
    }

    #[test]
    fn execution_evidence_reader_accepts_utf8_bom() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "foundation-platform-postgis-evidence-bom-{}.json",
            Uuid::now_v7()
        ));
        let payload = format!(
            "\u{feff}{}",
            serde_json::to_string(&json!({
                "schema_version": EXECUTION_SCHEMA_VERSION,
                "status": "ready",
                "output_storage_driver": "r2",
                "summary": {
                    "selected_shard_count": 1,
                    "succeeded_shard_count": 1,
                    "failed_shard_count": 0,
                    "output_row_count": 1
                },
                "shard_results": [{
                    "shard_id": "silver-parcel-boundaries-vworld-0001",
                    "status": "succeeded",
                    "output_storage_driver": "r2",
                    "output_object_key": "silver-handoff/a/part-0001.jsonl",
                    "output_row_count": 1
                }]
            }))?
        );
        fs::write(&path, payload)?;

        let evidence = read_execution_evidence(&path)?;

        assert_eq!(evidence.expected_row_count, 1);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn silver_handoff_line_becomes_stage_row_with_trace_properties() -> anyhow::Result<()> {
        let line = br#"{"boundary_id":"vworld-cadastral:parcel-boundary:pnu:9999900101100010001","pnu":"9999900101100010001","jibun":"1-1","bonbun":"0001","bubun":"0001","geometry_wkb_hex":"010600000000000000","geometry_wkb_encoding":"hex","geometry_srid":4326,"bbox_min_x":127.12347023440,"bbox_min_y":36.123450,"bbox_max_x":127.12347023441,"bbox_max_y":36.123451,"geometry_checksum_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_record_id":"national-promotion:vworld-shard-0001","source_snapshot_id":"national-promotion:vworld-shard-0001","valid_from_utc":"2026-05-24T00:00:00Z","valid_to_utc":null,"ingested_at_utc":"2026-05-24T00:00:01Z"}"#;

        let row = parse_stage_row(line, "silver-handoff/a/part-0001.jsonl", 1)?;
        let properties: JsonValue = serde_json::from_str(&row.properties_json)?;

        assert_eq!(row.pnu, "9999900101100010001");
        assert_eq!(
            properties["handoff_source_record_id"],
            "national-promotion:vworld-shard-0001"
        );
        assert_eq!(
            properties["source_object_key"],
            "silver-handoff/a/part-0001.jsonl"
        );
        assert_eq!(properties["bbox"]["min_x"], 127.123_470_234_40);
        Ok(())
    }

    #[test]
    fn copy_csv_row_escapes_quotes_without_losing_object_key() {
        let row = StageRow {
            pnu: "9999900101100010001".to_owned(),
            boundary_id: "boundary\"id".to_owned(),
            geometry_wkb_hex: "010600000000000000".to_owned(),
            geometry_checksum_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            properties_json: "{\"jibun\":\"1-1\"}".to_owned(),
        };
        let mut buffer = Vec::new();

        push_copy_csv_row(&mut buffer, &row, "silver-handoff/a/part-0001.jsonl");
        let rendered = String::from_utf8(buffer).unwrap_or_default();

        assert!(rendered.contains("\"boundary\"\"id\""));
        assert!(rendered.contains("\"silver-handoff/a/part-0001.jsonl\""));
    }

    #[test]
    fn source_snapshot_id_accepts_pipeline_lineage_not_only_iceberg() -> anyhow::Result<()> {
        validate_source_snapshot_id("iceberg:parcel-boundaries-snapshot-001")?;
        validate_source_snapshot_id("national-promotion:silver-parcel-boundaries-vworld-0002")?;
        assert!(validate_source_snapshot_id("../silver-parcel-boundaries").is_err());
        assert!(validate_source_snapshot_id(" national-promotion:bad").is_err());
        Ok(())
    }
}
