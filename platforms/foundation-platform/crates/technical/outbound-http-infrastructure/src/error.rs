use thiserror::Error;

/// Provider-neutral failure produced by outbound HTTP resilience infrastructure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct OutboundHttpError {
    message: String,
    class: OutboundFailureClass,
}

/// Whose side a failure is on (root ADR-0363).
///
/// In the resilience executors' vocabulary: what they stop retrying (exhausted retryable,
/// `ProviderUnavailable`) and what no retry or release can clear because the provider wants the
/// account owner (`ProviderAccountActionRequired`). Every
/// other failure is `Fatal` — a request, response or parse this code got wrong — and stays the
/// caller's to fix. The AWS SDKs' retry classifiers and gRPC's `UNAVAILABLE` vs
/// `FAILED_PRECONDITION` draw the same line.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum OutboundFailureClass {
    /// Our request or our reading of the answer failed; a new release is what fixes it.
    #[default]
    Fatal,
    /// The provider did not serve (transport failure, 408/429/5xx, maintenance page) after the
    /// policy's retries; it clears on its own.
    ProviderUnavailable,
    /// The provider refused the account until its owner acts on the provider's site (new terms
    /// to accept, a locked or expired account). `provider` is the endpoint catalog's provider
    /// (`vworld.kr`) and `reason` the provider's own code (`useChk`).
    ProviderAccountActionRequired {
        /// Endpoint catalog provider, the key of `config/provider-accounts.contract.json`.
        provider: String,
        /// The provider's result code, never a credential.
        reason: String,
    },
}

impl OutboundHttpError {
    /// Creates an outbound HTTP infrastructure error without attaching a business domain.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            class: OutboundFailureClass::Fatal,
        }
    }

    /// A provider that did not serve after the retries its policy allows.
    pub fn provider_unavailable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            class: OutboundFailureClass::ProviderUnavailable,
        }
    }

    /// A provider that refused the account until its owner acts.
    pub fn provider_account_action_required(
        provider: impl Into<String>,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            message: message.into(),
            class: OutboundFailureClass::ProviderAccountActionRequired {
                provider: provider.into(),
                reason: reason.into(),
            },
        }
    }

    /// Whose side the failure is on.
    #[must_use]
    pub const fn class(&self) -> &OutboundFailureClass {
        &self.class
    }

    /// Returns the owned provider-neutral error message.
    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }

    /// Returns the message and its class.
    #[must_use]
    pub fn into_parts(self) -> (String, OutboundFailureClass) {
        (self.message, self.class)
    }
}
