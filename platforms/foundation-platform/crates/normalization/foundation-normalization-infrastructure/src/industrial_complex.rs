//! Industrial-complex normalization ledger validation.

use foundation_normalization_domain::NormalizationError;
use foundation_shared_kernel::ids::ComplexId;
use serde_json::Value as JsonValue;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

pub const APPLY_COMMAND_TYPE: &str = "industrial_complex.normalization.apply.v1";
pub const ROLLBACK_COMMAND_TYPE: &str = "industrial_complex.normalization.rollback.v1";

struct LedgerEntry {
    id: Uuid,
    kind: LedgerEntryKind,
    before_snapshot: JsonValue,
    after_snapshot: JsonValue,
    before_version: i64,
    after_version: i64,
}

enum LedgerEntryKind {
    Apply,
    Rollback { application_id: Uuid },
}

pub async fn load_rollback_head(
    tx: &mut Transaction<'_, Postgres>,
    target_id: ComplexId,
    selected_application_id: Uuid,
) -> Result<JsonValue, NormalizationError> {
    let rows = sqlx::query(
        "SELECT id, command_type, rollback_of, before_snapshot, after_snapshot
         FROM catalog.normalization_application
         WHERE target_kind = 'industrial_complex'
           AND target_id = $1",
    )
    .bind(target_id.as_uuid())
    .fetch_all(&mut **tx)
    .await
    .map_err(crate::postgres_error::map_sqlx)?;

    let mut entries = rows
        .into_iter()
        .map(|row| map_ledger_entry(&row, target_id))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.before_version);
    validate_ledger_order(&entries)?;

    let mut active_stack = Vec::new();
    let mut selected_index = None;
    for (index, entry) in entries.iter().enumerate() {
        match entry.kind {
            LedgerEntryKind::Apply => {
                active_stack.push(entry.id);
                if entry.id == selected_application_id {
                    selected_index = Some(index);
                }
            }
            LedgerEntryKind::Rollback { application_id } => {
                if active_stack.last().copied() != Some(application_id) {
                    return Err(invalid_ledger(
                        "industrial-complex rollback ledger is not LIFO",
                    ));
                }
                active_stack.pop();
            }
        }
    }

    let selected_index = selected_index.ok_or_else(|| {
        invalid_ledger("industrial-complex application is missing from its target ledger")
    })?;
    if active_stack.last().copied() != Some(selected_application_id) {
        return Err(NormalizationError::TargetStateConflict(
            target_id.to_string(),
        ));
    }

    let mut expected_next_version = entries[selected_index].after_version;
    for entry in entries.iter().skip(selected_index + 1) {
        if entry.before_version != expected_next_version {
            return Err(NormalizationError::TargetStateConflict(
                target_id.to_string(),
            ));
        }
        expected_next_version = entry.after_version;
    }

    entries
        .last()
        .map(|entry| entry.after_snapshot.clone())
        .ok_or_else(|| invalid_ledger("industrial-complex target ledger is empty"))
}

fn map_ledger_entry(
    row: &sqlx::postgres::PgRow,
    target_id: ComplexId,
) -> Result<LedgerEntry, NormalizationError> {
    let id: Uuid = row.try_get("id").map_err(crate::postgres_error::map_sqlx)?;
    let command_type: String = row
        .try_get("command_type")
        .map_err(crate::postgres_error::map_sqlx)?;
    let rollback_of: Option<Uuid> = row
        .try_get("rollback_of")
        .map_err(crate::postgres_error::map_sqlx)?;
    let before_snapshot: Option<JsonValue> = row
        .try_get("before_snapshot")
        .map_err(crate::postgres_error::map_sqlx)?;
    let after_snapshot: Option<JsonValue> = row
        .try_get("after_snapshot")
        .map_err(crate::postgres_error::map_sqlx)?;
    let before_snapshot = before_snapshot
        .ok_or_else(|| invalid_ledger("industrial-complex ledger before_snapshot is missing"))?;
    let after_snapshot = after_snapshot
        .ok_or_else(|| invalid_ledger("industrial-complex ledger after_snapshot is missing"))?;
    let before_version = snapshot_version(&before_snapshot, target_id, "before_snapshot")?;
    let after_version = snapshot_version(&after_snapshot, target_id, "after_snapshot")?;
    let kind = match (command_type.as_str(), rollback_of) {
        (APPLY_COMMAND_TYPE, None) => LedgerEntryKind::Apply,
        (ROLLBACK_COMMAND_TYPE, Some(application_id)) => {
            LedgerEntryKind::Rollback { application_id }
        }
        _ => {
            return Err(invalid_ledger(
                "industrial-complex ledger command and rollback relationship disagree",
            ));
        }
    };
    Ok(LedgerEntry {
        id,
        kind,
        before_snapshot,
        after_snapshot,
        before_version,
        after_version,
    })
}

fn snapshot_version(
    snapshot: &JsonValue,
    target_id: ComplexId,
    field_name: &str,
) -> Result<i64, NormalizationError> {
    let object = snapshot.as_object().ok_or_else(|| {
        invalid_ledger(format!(
            "industrial-complex ledger {field_name} must be an object"
        ))
    })?;
    let snapshot_id = object
        .get("id")
        .and_then(JsonValue::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .map(ComplexId::new)
        .ok_or_else(|| {
            invalid_ledger(format!(
                "industrial-complex ledger {field_name}.id must be a UUID string"
            ))
        })?;
    if snapshot_id != target_id {
        return Err(invalid_ledger(format!(
            "industrial-complex ledger {field_name}.id does not match target_id"
        )));
    }
    object
        .get("version")
        .and_then(JsonValue::as_i64)
        .filter(|version| *version > 0)
        .ok_or_else(|| {
            invalid_ledger(format!(
                "industrial-complex ledger {field_name}.version must be positive"
            ))
        })
}

fn validate_ledger_order(entries: &[LedgerEntry]) -> Result<(), NormalizationError> {
    for entry in entries {
        if entry.after_version != entry.before_version + 1 {
            return Err(invalid_ledger(
                "industrial-complex ledger mutation must increment version by one",
            ));
        }
    }
    for pair in entries.windows(2) {
        if pair[0].before_version >= pair[1].before_version {
            return Err(invalid_ledger(
                "industrial-complex ledger contains duplicate or reversed versions",
            ));
        }
        if pair[0].after_version == pair[1].before_version
            && pair[0].after_snapshot != pair[1].before_snapshot
        {
            return Err(invalid_ledger(
                "industrial-complex ledger snapshots do not form a continuous handoff",
            ));
        }
    }
    Ok(())
}

fn invalid_ledger(message: impl Into<String>) -> NormalizationError {
    NormalizationError::Persistence(message.into())
}
