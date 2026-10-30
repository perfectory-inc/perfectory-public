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

    /// The provider did not serve after the retries its policy allows (root ADR-0363): its side,
    /// cleared by time, not by a release.
    #[error("provider unavailable: {0}")]
    ProviderUnavailable(String),

    /// The provider refused our account until its owner acts on the provider's site (root
    /// ADR-0363): 2026-10-30 `VWorld` answered the login `useChk`, new terms to accept.
    #[error("provider account action required: {provider} answered {reason}: {message}")]
    ProviderAccountActionRequired {
        /// Endpoint catalog provider, the key of `config/provider-accounts.contract.json`.
        provider: String,
        /// The provider's result code.
        reason: String,
        /// What the provider said, credentials redacted.
        message: String,
    },

    /// Infrastructure failed behind the Collection boundary.
    #[error("collection infrastructure error: {0}")]
    Infrastructure(String),
}
