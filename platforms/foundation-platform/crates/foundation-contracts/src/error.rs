//! Shared Foundation Platform HTTP error envelopes.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Safe error body returned by Foundation API handlers.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ApiErrorResponse {
    /// Safe client-facing diagnostic.
    pub error: String,
}

/// Opaque internal-error body returned by Foundation API handlers.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct InternalApiErrorResponse {
    /// Stable opaque diagnostic.
    pub error: String,
    /// Opaque identifier operators can use to locate an internal failure.
    pub correlation_id: Uuid,
}
