//! Staff identity application use cases and focused outbound ports.

#![deny(missing_docs)]

/// Outbound ports used to verify staff sessions.
pub mod ports;
/// Staff-session revocation use case.
pub mod revoke_session;
/// Staff bearer verification and session persistence use case.
pub mod verify_session;

pub use revoke_session::RevokeStaffSession;
pub use verify_session::{
    VerifiedStaffContext, VerifyStaffSession, VerifyStaffSessionInput, VerifyStaffSessionOutput,
};
