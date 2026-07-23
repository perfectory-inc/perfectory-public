//! Pure authorization domain model.
//!
//! This crate evaluates staff role capabilities without HTTP, storage, or service dependencies.

#![deny(missing_docs)]

/// Permission value object.
pub mod permission;
/// Pure policy evaluation.
pub mod policy;
/// Role code and staff role grant model.
pub mod role;

pub use permission::Permission;
pub use policy::{evaluate_policy, PolicyDecision, PolicyInput};
pub use role::{RoleCode, RoleGrant};
