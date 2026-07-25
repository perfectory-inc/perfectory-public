//! Provider-neutral spatial WAP adapter boundary.
//!
//! The adapter intentionally accepts a runner supplied by the deployment environment. The Rust
//! control plane validates the evidence and sequencing; it does not embed Cloudflare APIs or
//! grant a publisher a Docker socket.

use async_trait::async_trait;
use lakehouse_application::ports::{
    SpatialTileWap, SpatialTileWapCandidate, SpatialTileWapValidation,
};
use lakehouse_domain::LakehouseError;
use uuid::Uuid;

/// Evidence-producing Spark/WAP runner supplied by the deployment adapter.
#[async_trait]
pub trait SpatialTileWapRunner: Send + Sync {
    /// Creates a candidate branch from the selected base.
    async fn prepare(
        &self,
        base: &str,
        change_set: &str,
        release_id: Uuid,
    ) -> Result<SpatialTileWapCandidate, LakehouseError>;
    /// Validates a candidate and returns secret-free evidence.
    async fn validate(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<SpatialTileWapValidation, LakehouseError>;
    /// Retains a selected candidate.
    async fn retain(&self, candidate: &SpatialTileWapCandidate) -> Result<(), LakehouseError>;
    /// Expires an unselected candidate.
    async fn expire(&self, candidate: &SpatialTileWapCandidate) -> Result<(), LakehouseError>;
    /// Fast-forwards main along selected ancestry.
    async fn fast_forward(&self, selected_snapshot_id: &str) -> Result<(), LakehouseError>;
}

/// Thin application-port adapter around the pinned Spark WAP job.
pub struct SparkSpatialTileWap<R> {
    runner: R,
}

impl<R> SparkSpatialTileWap<R> {
    /// Wraps a deployment-provided WAP runner.
    #[must_use]
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }
}

#[async_trait]
impl<R> SpatialTileWap for SparkSpatialTileWap<R>
where
    R: SpatialTileWapRunner,
{
    async fn prepare_candidate(
        &self,
        base_snapshot_id: &str,
        change_set: &str,
        release_id: Uuid,
    ) -> Result<SpatialTileWapCandidate, LakehouseError> {
        if base_snapshot_id.trim().is_empty() || change_set.trim().is_empty() {
            return Err(LakehouseError::InvalidContract(
                "WAP base snapshot and change set are required".to_owned(),
            ));
        }
        self.runner
            .prepare(base_snapshot_id, change_set, release_id)
            .await
    }
    async fn validate_candidate(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<SpatialTileWapValidation, LakehouseError> {
        self.runner.validate(candidate).await
    }
    async fn retain_selected(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<(), LakehouseError> {
        self.runner.retain(candidate).await
    }
    async fn expire_unselected(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<(), LakehouseError> {
        self.runner.expire(candidate).await
    }
    async fn fast_forward_main(&self, selected_snapshot_id: &str) -> Result<(), LakehouseError> {
        if selected_snapshot_id.trim().is_empty() {
            return Err(LakehouseError::InvalidContract(
                "selected snapshot is required".to_owned(),
            ));
        }
        self.runner.fast_forward(selected_snapshot_id).await
    }
}
