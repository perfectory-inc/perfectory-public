//! Pure validated service principal and service-call policy model.

#![deny(missing_docs)]

/// Service identity errors.
pub mod error;
/// Service call metadata and pure capability evaluation.
pub mod policy;
/// Validated service principal model.
pub mod principal;

pub use error::ServiceIdentityError;
pub use policy::{evaluate_service_call, ServiceCallMetadata};
pub use principal::ValidatedServicePrincipal;
