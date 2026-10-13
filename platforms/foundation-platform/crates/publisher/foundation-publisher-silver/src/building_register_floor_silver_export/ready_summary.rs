//! Rendering and bounded publication payload for the single ready summary.
use super::{
    blocking::Cancellation, completed_handoff, input_evidence::FileEvidence, ExportConfig,
    ExportReport,
};
use chrono::Utc;
use std::path::PathBuf;

pub(super) fn prepare(
    config: &ExportConfig,
    selected: &[FileEvidence],
    report: &ExportReport,
    source_snapshot_ids: &[String],
    cancellation: &Cancellation,
) -> anyhow::Result<Option<(PathBuf, Vec<u8>)>> {
    let summary = if let Some(summary_path) = &config.summary_path {
        let floor_entity_context_pack_input = config.proposal_input_path.as_ref().map(|path| {
            serde_json::json!({
                "path": path.display().to_string(),
                "proposal_count": report.normalization_proposal_count
            })
        });
        let summary = serde_json::json!({
            "schema_version": "foundation-platform.building_register_floor_silver_handoff_export.v1",
            "generated_at_utc": Utc::now().to_rfc3339(),
            "status": "ready",
            "completion_claim_allowed": false,
            "production_cutover_allowed": false,
            "national_rollout_allowed": false,
            "source": {
                "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
                "selector": config.source_selector.source_summary(),
                "input_object_count": report.input_object_count,
                "source_snapshot_id": config.source_snapshot_id.as_str(),
                "max_rows": config.max_rows,
                "chunk_rows": config.chunk_rows,
                "output_format": config.output_format.wire_name()
            },
            "selected_input_evidence": selected,
            "reuse_evidence": completed_handoff::evidence(config, selected, report, cancellation)?,
            "committed_inputs": config.committed_inputs,
            "ingested_at_utc": config.ingested_at_utc,
            "valid_from_utc": config.valid_from_utc,
            "output": {
                "path": config.output_path.display().to_string(),
                "format": config.output_format.wire_name(),
                "contract": "silver.building_register_floors",
                "row_count": report.row_count,
                "proposal_required_count": report.proposal_required_count,
                "floor_entity_context_pack_input": floor_entity_context_pack_input,
                "source_snapshot_ids": source_snapshot_ids
            },
            "evidence_limitations": [
                "local_bronze_to_silver_handoff_only",
                "does_not_write_iceberg_table",
                "does_not_apply_ai_or_human_review",
                "does_not_approve_production_cutover"
            ]
        });
        let payload = completed_handoff::serialize_summary(&summary)?;
        Some((summary_path.clone(), payload))
    } else {
        None
    };

    cancellation.check()?;
    Ok(summary)
}
