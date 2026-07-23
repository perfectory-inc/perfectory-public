//! Industrial-complex Gold pointer publication use case.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use foundation_shared_kernel::ObjectKey;
use lakehouse_domain::{IndustrialComplexGoldPointer, LakehouseError};

use crate::ports::LakehousePublicationUnitOfWork;

/// Input accepted by the industrial-complex Gold publication use case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishIndustrialComplexGoldPointerInput {
    /// Industrial complex whose Gold pointer should be published.
    pub complex_id: ComplexId,
    /// Newly active Gold artifact version.
    pub current_version: String,
    /// Active Gold artifact version observed before publication.
    pub expected_current_version: Option<String>,
    /// Gold profile object key.
    pub profile_object_key: String,
    /// Optional Gold spatial locator object key.
    pub spatial_locator_object_key: Option<String>,
    /// Source system or pipeline name.
    pub source: String,
    /// Optional source URL.
    pub source_url: Option<String>,
    /// Optional source-side publication id.
    pub source_external_id: Option<String>,
    /// Source snapshot represented by the artifact.
    pub source_snapshot_id: String,
    /// Iceberg snapshot represented by the artifact.
    pub iceberg_snapshot_id: String,
    /// Profile row count represented by the artifact.
    pub profile_row_count: u64,
    /// Gold profile object size in bytes.
    pub profile_size_bytes: u64,
    /// Optional Gold spatial locator object size in bytes.
    pub spatial_locator_size_bytes: Option<u64>,
    /// SHA-256 checksum of the profile artifact.
    pub profile_checksum_sha256: String,
    /// UTC publication time.
    pub published_at: DateTime<Utc>,
}

/// Validated command committed by `LakehousePublicationUnitOfWork`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishIndustrialComplexGoldPointerCommand {
    /// Industrial complex whose Gold pointer should be published.
    pub complex_id: ComplexId,
    /// Newly active Gold artifact version.
    pub current_version: String,
    /// Active Gold artifact version observed before publication.
    pub expected_current_version: Option<String>,
    /// Gold profile object key.
    pub profile_object_key: String,
    /// Optional Gold spatial locator object key.
    pub spatial_locator_object_key: Option<String>,
    /// Source system or pipeline name.
    pub source: String,
    /// Optional source URL.
    pub source_url: Option<String>,
    /// Optional source-side publication id.
    pub source_external_id: Option<String>,
    /// Source snapshot represented by the artifact.
    pub source_snapshot_id: String,
    /// Iceberg snapshot represented by the artifact.
    pub iceberg_snapshot_id: String,
    /// Profile row count represented by the artifact.
    pub profile_row_count: u64,
    /// Gold profile object size in bytes.
    pub profile_size_bytes: u64,
    /// Optional Gold spatial locator object size in bytes.
    pub spatial_locator_size_bytes: Option<u64>,
    /// SHA-256 checksum of the profile artifact.
    pub profile_checksum_sha256: String,
    /// UTC publication time.
    pub published_at: DateTime<Utc>,
}

/// Publishes a validated industrial-complex Gold pointer.
pub struct PublishIndustrialComplexGoldPointer {
    unit_of_work: Arc<dyn LakehousePublicationUnitOfWork>,
}

impl PublishIndustrialComplexGoldPointer {
    /// Builds the use case with its Lakehouse transaction boundary.
    #[must_use]
    pub fn new(unit_of_work: Arc<dyn LakehousePublicationUnitOfWork>) -> Self {
        Self { unit_of_work }
    }

    /// Validates and publishes one Gold pointer.
    ///
    /// # Errors
    /// Returns `LakehouseError` when validation or persistence fails.
    pub async fn execute(
        &self,
        input: PublishIndustrialComplexGoldPointerInput,
    ) -> Result<IndustrialComplexGoldPointer, LakehouseError> {
        let command = PublishIndustrialComplexGoldPointerCommand::from(input);
        command.validate()?;
        self.unit_of_work
            .publish_industrial_complex_gold_pointer(command)
            .await
    }
}

impl PublishIndustrialComplexGoldPointerCommand {
    /// Validates the complete publication contract.
    ///
    /// # Errors
    /// Returns `LakehouseError::InvalidContract` when any field violates the contract.
    pub fn validate(&self) -> Result<(), LakehouseError> {
        validate_clean_required("current_version", self.current_version.as_str())?;
        if let Some(expected_current_version) = &self.expected_current_version {
            validate_clean_required("expected_current_version", expected_current_version)?;
            if expected_current_version == &self.current_version {
                return invalid("expected_current_version must differ from current_version");
            }
        }
        ObjectKey::parse(self.profile_object_key.as_str())
            .map_err(|error| LakehouseError::InvalidContract(error.to_string()))?;
        if let Some(key) = &self.spatial_locator_object_key {
            ObjectKey::parse(key)
                .map_err(|error| LakehouseError::InvalidContract(error.to_string()))?;
        }
        match (
            self.spatial_locator_object_key.as_ref(),
            self.spatial_locator_size_bytes,
        ) {
            (Some(_), Some(size_bytes)) if size_bytes > 0 => {}
            (Some(_), _) => {
                return invalid(
                    "spatial_locator_size_bytes must be positive when spatial_locator_object_key is set",
                );
            }
            (None, Some(_)) => {
                return invalid("spatial_locator_size_bytes requires spatial_locator_object_key");
            }
            (None, None) => {}
        }
        validate_clean_required("source", self.source.as_str())?;
        if let Some(source_external_id) = &self.source_external_id {
            validate_clean_required("source_external_id", source_external_id)?;
        }
        validate_clean_required("source_snapshot_id", self.source_snapshot_id.as_str())?;
        validate_clean_required("iceberg_snapshot_id", self.iceberg_snapshot_id.as_str())?;
        if self.profile_row_count == 0 {
            return invalid("profile_row_count must be positive");
        }
        if self.profile_size_bytes == 0 {
            return invalid("profile_size_bytes must be positive");
        }
        if !is_lowercase_sha256(self.profile_checksum_sha256.as_str()) {
            return invalid("profile_checksum_sha256 must be 64 lowercase hex characters");
        }
        Ok(())
    }
}

impl From<PublishIndustrialComplexGoldPointerInput> for PublishIndustrialComplexGoldPointerCommand {
    fn from(input: PublishIndustrialComplexGoldPointerInput) -> Self {
        Self {
            complex_id: input.complex_id,
            current_version: input.current_version,
            expected_current_version: input.expected_current_version,
            profile_object_key: input.profile_object_key,
            spatial_locator_object_key: input.spatial_locator_object_key,
            source: input.source,
            source_url: input.source_url,
            source_external_id: input.source_external_id,
            source_snapshot_id: input.source_snapshot_id,
            iceberg_snapshot_id: input.iceberg_snapshot_id,
            profile_row_count: input.profile_row_count,
            profile_size_bytes: input.profile_size_bytes,
            spatial_locator_size_bytes: input.spatial_locator_size_bytes,
            profile_checksum_sha256: input.profile_checksum_sha256,
            published_at: input.published_at,
        }
    }
}

fn validate_clean_required(field: &str, value: &str) -> Result<(), LakehouseError> {
    if value.is_empty() {
        return invalid(format!("{field} must not be empty"));
    }
    if value.trim() != value {
        return invalid(format!("{field} must not have surrounding whitespace"));
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, LakehouseError> {
    Err(LakehouseError::InvalidContract(message.into()))
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
