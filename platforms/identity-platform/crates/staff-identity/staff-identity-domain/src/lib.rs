//! Pure staff identity domain model.
//!
//! This crate owns staff account and verified-session state without infrastructure concerns.

#![deny(missing_docs)]

/// Staff identity errors.
pub mod error;
/// Verified staff session model.
pub mod session;
/// Staff account aggregate.
pub mod staff;

pub use error::StaffIdentityError;
pub use session::StaffSession;
pub use staff::Staff;
