//! Shared typed contracts for sealing and consuming parcel-publication evidence.

use anyhow::bail;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use uuid::Uuid;

pub const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_publication_execution_evidence.v1";
pub const QUALITY_SCHEMA_VERSION: &str = "foundation-platform.parcel_publication_quality.v1";
pub const PARCEL_LOGICAL_TABLE: &str = "silver.parcel_boundaries";
pub const GEOMETRY_REPAIR_STRATEGY: &str = "postgis-make-valid-v1";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParcelPublicationExecutionEvidence {
    pub schema_version: String,
    pub status: String,
    pub mirror_rebuild_run_id: Uuid,
    pub source_record_id: Uuid,
    pub source_file_asset_id: Uuid,
    pub scope: PublicationScope,
    pub limits: PublicationLimits,
    pub iceberg_commit: IcebergCommit,
    pub production_cutover_allowed: bool,
    pub national_rollout_allowed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationScope {
    pub kind: String,
    pub complete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationLimits {
    pub object_limit: JsonValue,
    pub row_limit: JsonValue,
    pub shard_limit: JsonValue,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergCommit {
    pub committed: bool,
    pub logical_table: String,
    pub table_uuid: Uuid,
    pub snapshot_id: i64,
}

impl ParcelPublicationExecutionEvidence {
    pub fn validate_publication_claims(&self) -> anyhow::Result<()> {
        if self.schema_version != EXECUTION_SCHEMA_VERSION {
            bail!("execution evidence schema_version must be {EXECUTION_SCHEMA_VERSION}");
        }
        if self.status != "succeeded" {
            bail!("execution evidence status must be succeeded");
        }
        if self.scope.kind != "national" || !self.scope.complete {
            bail!("execution evidence must cover the complete national scope");
        }
        if !self.limits.object_limit.is_null()
            || !self.limits.row_limit.is_null()
            || !self.limits.shard_limit.is_null()
        {
            bail!("execution evidence must not carry object, row, or shard limits");
        }
        if !self.iceberg_commit.committed {
            bail!("execution evidence must prove a completed Iceberg commit");
        }
        if self.iceberg_commit.logical_table != PARCEL_LOGICAL_TABLE {
            bail!("execution evidence must name {PARCEL_LOGICAL_TABLE}");
        }
        if self.iceberg_commit.snapshot_id <= 0 {
            bail!("execution evidence Iceberg snapshot id must be positive");
        }
        if !self.production_cutover_allowed {
            bail!("execution evidence must allow production cutover");
        }
        if !self.national_rollout_allowed {
            bail!("execution evidence must allow national rollout");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct ParcelPublicationQuality {
    pub schema_version: String,
    pub object_count: u64,
    pub expected_row_count: u64,
    pub loaded_row_count: u64,
    pub invalid_srid_count: u64,
    pub invalid_geometry_count: u64,
    pub empty_geometry_count: u64,
    pub nonpositive_area_count: u64,
    pub source_srid: String,
    pub target_srid: String,
    pub geometry_repair_strategy: String,
}

pub fn verify_quality_report(
    evidence_schema_version: &str,
    source_row_count: i64,
    quality: &ParcelPublicationQuality,
) -> anyhow::Result<()> {
    let expected_rows = u64::try_from(source_row_count)
        .map_err(|_| anyhow::anyhow!("sealed parcel source row count must be positive"))?;
    if evidence_schema_version != QUALITY_SCHEMA_VERSION
        || quality.schema_version != QUALITY_SCHEMA_VERSION
        || quality.object_count == 0
        || quality.expected_row_count != expected_rows
        || quality.loaded_row_count != expected_rows
        || quality.invalid_srid_count != 0
        || quality.invalid_geometry_count != 0
        || quality.empty_geometry_count != 0
        || quality.nonpositive_area_count != 0
        || quality.source_srid != "EPSG:4326"
        || quality.target_srid != "EPSG:5179"
        || quality.geometry_repair_strategy != GEOMETRY_REPAIR_STRATEGY
    {
        bail!(
            "parcel source evidence does not carry the complete zero-defect {QUALITY_SCHEMA_VERSION} quality report"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::ParcelPublicationExecutionEvidence;

    fn valid_claim() -> Value {
        json!({
            "schema_version": "foundation-platform.parcel_publication_execution_evidence.v1",
            "status": "succeeded",
            "mirror_rebuild_run_id": "b56cc1d0-f59b-4511-93ed-932dfd1504fb",
            "source_record_id": "29b0a7f4-7af4-446c-aa65-56aa04f87c17",
            "source_file_asset_id": "74dc8433-bda3-4dde-9a33-a9b178565907",
            "scope": {"kind": "national", "complete": true},
            "limits": {"object_limit": null, "row_limit": null, "shard_limit": null},
            "iceberg_commit": {
                "committed": true,
                "logical_table": "silver.parcel_boundaries",
                "table_uuid": "2f7bf2d1-3e08-4d1a-936e-556d8ebfd055",
                "snapshot_id": 841361364657368626_i64
            },
            "production_cutover_allowed": true,
            "national_rollout_allowed": true
        })
    }

    #[test]
    fn every_publication_claim_is_fail_closed() -> anyhow::Result<()> {
        let violations = [
            ("/status", json!("running"), "status"),
            ("/scope/kind", json!("regional"), "national scope"),
            ("/scope/complete", json!(false), "national scope"),
            ("/limits/row_limit", json!(1), "must not carry"),
            ("/iceberg_commit/committed", json!(false), "Iceberg commit"),
            (
                "/iceberg_commit/logical_table",
                json!("silver.other"),
                "silver.parcel_boundaries",
            ),
            ("/iceberg_commit/snapshot_id", json!(0), "positive"),
            (
                "/production_cutover_allowed",
                json!(false),
                "production cutover",
            ),
            (
                "/national_rollout_allowed",
                json!(false),
                "national rollout",
            ),
        ];
        for (pointer, violating_value, expected_message) in violations {
            let mut value = valid_claim();
            *value.pointer_mut(pointer).expect("fixture pointer exists") = violating_value;
            let claim: ParcelPublicationExecutionEvidence = serde_json::from_value(value)?;
            let error = claim
                .validate_publication_claims()
                .expect_err("each injected eligibility violation must be rejected");
            assert!(
                error.to_string().contains(expected_message),
                "{pointer} rejection was {error:#}"
            );
        }
        Ok(())
    }

    #[test]
    fn unknown_or_missing_binding_fields_are_rejected() {
        let mut unknown = valid_claim();
        unknown["operator_override"] = json!(true);
        assert!(serde_json::from_value::<ParcelPublicationExecutionEvidence>(unknown).is_err());

        let mut missing = valid_claim();
        missing
            .as_object_mut()
            .expect("object")
            .remove("source_record_id");
        assert!(serde_json::from_value::<ParcelPublicationExecutionEvidence>(missing).is_err());
    }
}
