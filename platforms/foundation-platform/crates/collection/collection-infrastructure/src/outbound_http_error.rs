use collection_domain::CollectionError;
use outbound_http_infrastructure::{AttemptError, OutboundFailureClass, OutboundHttpError};

/// Translates provider-neutral HTTP failures at the Collection adapter boundary, keeping whose
/// side they are on (root ADR-0363).
pub fn into_collection_error(error: OutboundHttpError) -> CollectionError {
    match error.into_parts() {
        (message, OutboundFailureClass::Fatal) => CollectionError::Infrastructure(message),
        (message, OutboundFailureClass::ProviderUnavailable) => {
            CollectionError::ProviderUnavailable(message)
        }
        (message, OutboundFailureClass::ProviderAccountActionRequired { provider, reason }) => {
            CollectionError::ProviderAccountActionRequired {
                provider,
                reason,
                message,
            }
        }
    }
}

/// Converts a Collection-local parser/URL failure into a fatal provider attempt failure.
pub fn into_fatal_attempt(error: CollectionError) -> AttemptError {
    let message = match error {
        CollectionError::Infrastructure(message) => message,
        other => other.to_string(),
    };
    AttemptError::Fatal(OutboundHttpError::new(message))
}
