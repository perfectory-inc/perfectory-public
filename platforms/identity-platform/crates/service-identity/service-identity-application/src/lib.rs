//! Service identity application use cases and focused outbound ports.

#![deny(missing_docs)]

/// Validated service-call authorization use case.
pub mod authorize_call;
/// Credential verification and audit ports.
pub mod ports;

pub use authorize_call::{
    AuthorizeServiceCall, AuthorizeServiceCallInput, AuthorizeServiceCallOutput,
};
