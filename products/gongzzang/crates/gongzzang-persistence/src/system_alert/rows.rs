use chrono::{DateTime, Utc};
use shared_kernel::id::{Id, SystemAlertMarker, UserMarker};
use sqlx::postgres::PgRow;
use sqlx::Row;
use system_alert_domain::repository::RepoError;
use system_alert_domain::{SystemAlert, SystemAlertSeverity};

pub(super) const COLUMNS: &str = "id, severity, source, title, detail, metadata, \
    acknowledged_at, acknowledged_by, resolved_at, created_at";

fn parse_severity(value: &str) -> Result<SystemAlertSeverity, RepoError> {
    SystemAlertSeverity::from_db_str(value)
        .ok_or_else(|| RepoError::Database(format!("unexpected severity: {value}")))
}

pub(super) fn row_to_alert(row: &PgRow) -> Result<SystemAlert, RepoError> {
    let id: String = row
        .try_get("id")
        .map_err(|error| RepoError::Database(error.to_string()))?;
    let severity: String = row
        .try_get("severity")
        .map_err(|error| RepoError::Database(error.to_string()))?;
    let acknowledged_by: Option<String> = row
        .try_get("acknowledged_by")
        .map_err(|error| RepoError::Database(error.to_string()))?;

    Ok(SystemAlert {
        id: Id::<SystemAlertMarker>::try_from_str(id.trim())
            .map_err(|error| RepoError::Database(format!("malformed system_alert id: {error}")))?,
        severity: parse_severity(&severity)?,
        source: row
            .try_get("source")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        title: row
            .try_get("title")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        detail: row
            .try_get("detail")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        metadata: row
            .try_get("metadata")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        acknowledged_at: row
            .try_get::<Option<DateTime<Utc>>, _>("acknowledged_at")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        acknowledged_by: acknowledged_by
            .map(|value| {
                Id::<UserMarker>::try_from_str(value.trim()).map_err(|error| {
                    RepoError::Database(format!("malformed acknowledged_by: {error}"))
                })
            })
            .transpose()?,
        resolved_at: row
            .try_get::<Option<DateTime<Utc>>, _>("resolved_at")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|error| RepoError::Database(error.to_string()))?,
    })
}
