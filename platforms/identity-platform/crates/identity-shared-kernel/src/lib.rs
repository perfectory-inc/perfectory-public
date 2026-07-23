//! Internal shared value types for Identity Platform capabilities.
//!
//! This crate intentionally contains only identifiers shared by Identity implementation crates.
//! Published consumers use `identity-contracts::PrincipalId` instead.

#![deny(missing_docs)]

/// Identity implementation identifiers.
pub mod ids;

pub use ids::{SessionId, StaffId};
