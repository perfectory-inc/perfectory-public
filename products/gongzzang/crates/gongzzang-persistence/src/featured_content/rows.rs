use chrono::{DateTime, Utc};
use featured_content_domain::repository::RepoError;
use featured_content_domain::{
    FeaturedContent, FeaturedContentFeatureKind, FeaturedContentTargetKind,
};
use shared_kernel::id::{FeaturedContentMarker, Id, UserMarker};
use sqlx::postgres::PgRow;
use sqlx::Row;

pub(super) const COLUMNS: &str = "id, target_kind, target_id, feature_kind, weight, \
    starts_at, ends_at, purchased_by, impression_count, click_count, created_at";

fn parse_target_kind(value: &str) -> Result<FeaturedContentTargetKind, RepoError> {
    FeaturedContentTargetKind::from_db_str(value)
        .ok_or_else(|| RepoError::Database(format!("unexpected target_kind: {value}")))
}

fn parse_feature_kind(value: &str) -> Result<FeaturedContentFeatureKind, RepoError> {
    FeaturedContentFeatureKind::from_db_str(value)
        .ok_or_else(|| RepoError::Database(format!("unexpected feature_kind: {value}")))
}

pub(super) fn row_to_featured(row: &PgRow) -> Result<FeaturedContent, RepoError> {
    let id: String = row
        .try_get("id")
        .map_err(|error| RepoError::Database(error.to_string()))?;
    let target_kind: String = row
        .try_get("target_kind")
        .map_err(|error| RepoError::Database(error.to_string()))?;
    let feature_kind: String = row
        .try_get("feature_kind")
        .map_err(|error| RepoError::Database(error.to_string()))?;
    let purchased_by: Option<String> = row
        .try_get("purchased_by")
        .map_err(|error| RepoError::Database(error.to_string()))?;

    Ok(FeaturedContent {
        id: Id::<FeaturedContentMarker>::try_from_str(id.trim()).map_err(|error| {
            RepoError::Database(format!("malformed featured_content id: {error}"))
        })?,
        target_kind: parse_target_kind(&target_kind)?,
        target_id: row
            .try_get("target_id")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        feature_kind: parse_feature_kind(&feature_kind)?,
        weight: row
            .try_get("weight")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        starts_at: row
            .try_get::<DateTime<Utc>, _>("starts_at")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        ends_at: row
            .try_get::<DateTime<Utc>, _>("ends_at")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        purchased_by: purchased_by
            .map(|value| {
                Id::<UserMarker>::try_from_str(value.trim()).map_err(|error| {
                    RepoError::Database(format!("malformed purchased_by: {error}"))
                })
            })
            .transpose()?,
        impression_count: row
            .try_get("impression_count")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        click_count: row
            .try_get("click_count")
            .map_err(|error| RepoError::Database(error.to_string()))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|error| RepoError::Database(error.to_string()))?,
    })
}
