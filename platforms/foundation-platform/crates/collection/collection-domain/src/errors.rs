//! Collection domain and persistence boundary errors.

use thiserror::Error;

/// Error returned by Collection application ports and infrastructure adapters.
#[derive(Debug, Error)]
pub enum CollectionError {
    /// An ingestion run was not found.
    #[error("ingestion run not found (id={0})")]
    IngestionRunNotFound(String),

    /// An ingestion run completion command failed validation.
    #[error("invalid ingestion run completion: {0}")]
    InvalidIngestionRunCompletion(String),

    /// A provider cannot expose the source through the automated acquisition lane.
    #[error("provider acquisition blocked: {0}")]
    ProviderAcquisitionBlocked(String),

    /// Infrastructure failed behind the Collection boundary.
    #[error("collection infrastructure error: {0}")]
    Infrastructure(String),
}
