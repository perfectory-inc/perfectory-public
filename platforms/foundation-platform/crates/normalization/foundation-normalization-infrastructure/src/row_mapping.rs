//! `PostgreSQL` row mapping for proposal and application records.

use foundation_normalization_application::NormalizationProposalRecord;
use foundation_normalization_domain::{
    NormalizationError, NormalizationProposalStatus, NormalizationTargetKind,
};
use foundation_shared_kernel::ids::ComplexId;
use serde_json::Value as JsonValue;
use sqlx::Row;
use uuid::Uuid;

use crate::postgres_error::map_sqlx;

pub struct ProposalForApply {
    pub status: NormalizationProposalStatus,
    pub target_kind: NormalizationTargetKind,
    pub target_identity: JsonValue,
    pub target_schema_version: String,
    pub proposal_schema_version: String,
    pub proposed_record: JsonValue,
}

pub struct ApplicationForRollback {
    pub proposal_id: Uuid,
    pub target_kind: NormalizationTargetKind,
    pub target_id: Option<ComplexId>,
    pub before_snapshot: JsonValue,
    pub after_snapshot: JsonValue,
}

pub fn row_to_proposal_record(
    row: &sqlx::postgres::PgRow,
    created: bool,
) -> Result<NormalizationProposalRecord, NormalizationError> {
    let status: String = row.try_get("status").map_err(map_sqlx)?;
    Ok(NormalizationProposalRecord {
        id: row.try_get("id").map_err(map_sqlx)?,
        proposal_key: row.try_get("proposal_key").map_err(map_sqlx)?,
        status: parse_status(status.as_str())?,
        created,
    })
}

pub fn parse_status(raw: &str) -> Result<NormalizationProposalStatus, NormalizationError> {
    match raw {
        "pending_review" => Ok(NormalizationProposalStatus::PendingReview),
        "approved" => Ok(NormalizationProposalStatus::Approved),
        "rejected" => Ok(NormalizationProposalStatus::Rejected),
        "superseded" => Ok(NormalizationProposalStatus::Superseded),
        "applied" => Ok(NormalizationProposalStatus::Applied),
        "apply_failed" => Ok(NormalizationProposalStatus::ApplyFailed),
        "rolled_back" => Ok(NormalizationProposalStatus::RolledBack),
        other => Err(NormalizationError::Persistence(format!(
            "unknown normalization proposal status: {other}"
        ))),
    }
}

pub fn parse_target_kind(raw: &str) -> Result<NormalizationTargetKind, NormalizationError> {
    match raw {
        "industrial_complex" => Ok(NormalizationTargetKind::IndustrialComplex),
        "building_register_floor" => Ok(NormalizationTargetKind::BuildingRegisterFloor),
        "building_register_unit" => Ok(NormalizationTargetKind::BuildingRegisterUnit),
        other => Err(NormalizationError::InvalidInput(format!(
            "unsupported normalization target_kind: {other}"
        ))),
    }
}
