use collection_domain::CollectionError;
use outbound_http_infrastructure::{AttemptError, OutboundHttpError};

/// Translates provider-neutral HTTP failures at the Collection adapter boundary.
pub fn into_collection_error(error: OutboundHttpError) -> CollectionError {
    CollectionError::Infrastructure(error.into_message())
}

/// Converts a Collection-local parser/URL failure into a fatal provider attempt failure.
pub fn into_fatal_attempt(error: CollectionError) -> AttemptError {
    let message = match error {
        CollectionError::Infrastructure(message) => message,
        other => other.to_string(),
    };
    AttemptError::Fatal(OutboundHttpError::new(message))
}
