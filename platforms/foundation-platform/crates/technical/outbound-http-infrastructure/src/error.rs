use thiserror::Error;

/// Provider-neutral failure produced by outbound HTTP resilience infrastructure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct OutboundHttpError {
    message: String,
}

impl OutboundHttpError {
    /// Creates an outbound HTTP infrastructure error without attaching a business domain.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns the owned provider-neutral error message.
    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }
}
