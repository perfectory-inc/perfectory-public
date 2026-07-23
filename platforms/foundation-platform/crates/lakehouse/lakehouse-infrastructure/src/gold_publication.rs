//! `PostgreSQL` industrial-complex Gold publication adapter.

mod row_mapping;
mod transaction;

use async_trait::async_trait;
use foundation_shared_kernel::events::catalog_v1::{
    CatalogEvent, IndustrialComplexGoldPointerPublishedV1,
};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_application::ports::{
    IndustrialComplexGoldPointerReader, LakehousePublicationUnitOfWork,
};
use lakehouse_application::PublishIndustrialComplexGoldPointerCommand;
use lakehouse_domain::{
    IndustrialComplexGoldPointer, IndustrialComplexGoldPointerPublished, LakehouseError,
};
use sqlx::PgPool;

use crate::postgres_error::map_sqlx;
use row_mapping::{row_to_gold_pointer, GOLD_POINTER_COLUMNS};

/// `PostgreSQL` reader for published industrial-complex Gold pointers.
pub struct PgIndustrialComplexGoldPointerReader {
    pool: PgPool,
}

impl PgIndustrialComplexGoldPointerReader {
    /// Creates a reader backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `PostgreSQL` transaction boundary for industrial-complex Gold publication.
pub struct PgLakehousePublicationUnitOfWork {
    pool: PgPool,
}

impl PgLakehousePublicationUnitOfWork {
    /// Creates a unit of work backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IndustrialComplexGoldPointerReader for PgIndustrialComplexGoldPointerReader {
    async fn list_industrial_complex_gold_pointers(
        &self,
        complex_ids: &[ComplexId],
    ) -> Result<Vec<IndustrialComplexGoldPointer>, LakehouseError> {
        if complex_ids.is_empty() {
            return Ok(Vec::new());
        }
        let complex_uuids = complex_ids
            .iter()
            .map(ComplexId::as_uuid)
            .collect::<Vec<_>>();
        let query = format!(
            "SELECT {GOLD_POINTER_COLUMNS}
             FROM catalog.industrial_complex_gold_pointer gp
             JOIN catalog.file_asset profile_file ON profile_file.id = gp.profile_file_asset_id
             LEFT JOIN catalog.file_asset spatial_file
                    ON spatial_file.id = gp.spatial_locator_file_asset_id
             WHERE gp.complex_id = ANY($1)
             ORDER BY gp.complex_id"
        );
        let rows = sqlx::query(&query)
            .bind(&complex_uuids)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;

        rows.iter().map(row_to_gold_pointer).collect()
    }

    async fn find_industrial_complex_gold_pointer(
        &self,
        complex_id: ComplexId,
    ) -> Result<Option<IndustrialComplexGoldPointer>, LakehouseError> {
        let query = format!(
            "SELECT {GOLD_POINTER_COLUMNS}
             FROM catalog.industrial_complex_gold_pointer gp
             JOIN catalog.file_asset profile_file ON profile_file.id = gp.profile_file_asset_id
             LEFT JOIN catalog.file_asset spatial_file
                    ON spatial_file.id = gp.spatial_locator_file_asset_id
             WHERE gp.complex_id = $1"
        );
        let row = sqlx::query(&query)
            .bind(complex_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;

        row.as_ref().map(row_to_gold_pointer).transpose()
    }
}

#[async_trait]
impl LakehousePublicationUnitOfWork for PgLakehousePublicationUnitOfWork {
    async fn publish_industrial_complex_gold_pointer(
        &self,
        command: PublishIndustrialComplexGoldPointerCommand,
    ) -> Result<IndustrialComplexGoldPointer, LakehouseError> {
        transaction::publish_industrial_complex_gold_pointer(&self.pool, command).await
    }
}

fn gold_pointer_published_envelope(event: IndustrialComplexGoldPointerPublished) -> CatalogEvent {
    CatalogEvent::IndustrialComplexGoldPointerPublished(IndustrialComplexGoldPointerPublishedV1 {
        schema_version: 1,
        complex_id: event.complex_id,
        current_version: event.current_version,
        previous_version: event.previous_version,
        profile_object_key: event.profile_object_key,
        spatial_locator_object_key: event.spatial_locator_object_key,
        source_record_id: event.source_record_id.as_uuid(),
        source_snapshot_id: event.source_snapshot_id,
        iceberg_snapshot_id: event.iceberg_snapshot_id,
        profile_row_count: event.profile_row_count,
        profile_checksum_sha256: event.profile_checksum_sha256,
        published_at: event.published_at,
    })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use foundation_shared_kernel::ids::{ComplexId, SourceRecordId};
    use lakehouse_domain::IndustrialComplexGoldPointerPublished;
    use uuid::Uuid;

    use super::gold_pointer_published_envelope;

    #[test]
    fn gold_pointer_event_preserves_exact_legacy_json_bytes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let published_at =
            DateTime::parse_from_rfc3339("2026-05-18T00:00:00Z")?.with_timezone(&Utc);
        let event = IndustrialComplexGoldPointerPublished {
            complex_id: ComplexId::new(Uuid::nil()),
            current_version: "gold-v2".to_owned(),
            previous_version: Some("gold-v1".to_owned()),
            profile_object_key: "gold/industrial-complex/profiles/gold-v2.json".to_owned(),
            spatial_locator_object_key: Some(
                "gold/industrial-complex/spatial-locators/gold-v2.parquet".to_owned(),
            ),
            source_record_id: SourceRecordId::new(Uuid::nil()),
            source_snapshot_id: "source-snapshot-42".to_owned(),
            iceberg_snapshot_id: "iceberg-snapshot-84".to_owned(),
            profile_row_count: 7,
            profile_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            published_at,
        };

        let json = serde_json::to_string(&gold_pointer_published_envelope(event))?;

        assert_eq!(
            json,
            r#"{"type":"catalog.industrial_complex.gold_pointer.published.v1","schema_version":1,"complex_id":"00000000-0000-0000-0000-000000000000","current_version":"gold-v2","previous_version":"gold-v1","profile_object_key":"gold/industrial-complex/profiles/gold-v2.json","spatial_locator_object_key":"gold/industrial-complex/spatial-locators/gold-v2.parquet","source_record_id":"00000000-0000-0000-0000-000000000000","source_snapshot_id":"source-snapshot-42","iceberg_snapshot_id":"iceberg-snapshot-84","profile_row_count":7,"profile_checksum_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","published_at":"2026-05-18T00:00:00Z"}"#
        );
        Ok(())
    }
}
