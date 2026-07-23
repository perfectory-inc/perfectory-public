//! PostgreSQL reader for active building-register-unit overrides.

use async_trait::async_trait;
use foundation_normalization_application::{
    ActiveBuildingRegisterUnitOverride, ActiveBuildingRegisterUnitOverrideReader,
};
use foundation_normalization_domain::NormalizationError;
use sqlx::PgPool;

use crate::building_register_unit;

/// `PostgreSQL` implementation of active building-register-unit override reads.
pub struct PgActiveBuildingRegisterUnitOverrideReader {
    pool: PgPool,
}

impl PgActiveBuildingRegisterUnitOverrideReader {
    /// Creates an active override reader backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ActiveBuildingRegisterUnitOverrideReader for PgActiveBuildingRegisterUnitOverrideReader {
    async fn list_active_building_register_unit_overrides(
        &self,
    ) -> Result<Vec<ActiveBuildingRegisterUnitOverride>, NormalizationError> {
        building_register_unit::list_active_overrides(&self.pool)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| ActiveBuildingRegisterUnitOverride {
                        application_id: row.application_id,
                        snapshot: row.snapshot,
                    })
                    .collect()
            })
    }
}
