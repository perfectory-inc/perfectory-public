use async_trait::async_trait;
use intelligence_normalization_domain::normalization::{
    NormalizationProposal, NormalizationRequest,
};
use thiserror::Error;

use crate::workflow_state::{FoundationSubmissionResult, NormalizationProposalSubmission};

#[derive(Debug, Error)]
pub enum NormalizationProposalError {
    #[error("normalization proposal generator is not configured")]
    NotConfigured,
    #[error("normalization proposal generation failed")]
    GenerationFailed { message: String },
    #[error("normalization proposal response was invalid")]
    InvalidResponse { message: String },
}

impl NormalizationProposalError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::NotConfigured => "normalization proposal generator is not configured",
            Self::GenerationFailed { .. } => "normalization proposal generation failed",
            Self::InvalidResponse { .. } => "normalization proposal response was invalid",
        }
    }
}

#[async_trait]
pub trait NormalizationProposalGenerator: Send + Sync {
    async fn propose(
        &self,
        request: &NormalizationRequest,
    ) -> Result<NormalizationProposal, NormalizationProposalError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FoundationSubmissionFailureClass {
    Retryable,
    Terminal,
    ReconcileRequired,
}

#[derive(Debug, Error)]
pub enum FoundationSubmissionError {
    #[error("foundation-platform rejected normalization submission with status {status}")]
    Rejected {
        status: u16,
        body: String,
        retryable: bool,
    },
    #[error("foundation-platform submission failed before send")]
    PreSendFailure { message: String },
    #[error("foundation-platform submission outcome is ambiguous")]
    AmbiguousOutcome { message: String },
    #[error("foundation-platform returned invalid response: {message}")]
    InvalidResponse { message: String },
}

impl FoundationSubmissionError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::Rejected { .. } => "foundation-platform rejected submission",
            Self::PreSendFailure { .. } => "foundation-platform submission failed before send",
            Self::AmbiguousOutcome { .. } => "foundation-platform submission outcome is ambiguous",
            Self::InvalidResponse { .. } => "foundation-platform returned invalid response",
        }
    }

    pub fn failure_class(&self) -> FoundationSubmissionFailureClass {
        match self {
            Self::PreSendFailure { .. } => FoundationSubmissionFailureClass::Retryable,
            Self::Rejected {
                retryable: true, ..
            } => FoundationSubmissionFailureClass::Retryable,
            Self::Rejected {
                retryable: false, ..
            } => FoundationSubmissionFailureClass::Terminal,
            Self::AmbiguousOutcome { .. } | Self::InvalidResponse { .. } => {
                FoundationSubmissionFailureClass::ReconcileRequired
            }
        }
    }
}

#[async_trait]
pub trait FoundationNormalizationSubmitter: Send + Sync {
    async fn submit(
        &self,
        submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError>;
}
