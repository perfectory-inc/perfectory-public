//! Storage-agnostic `CourtAuction` read model and reader ports.
//!
//! This crate defines read-only domain types and interfaces. Collection, transformation,
//! persistence, and reader implementations belong to adapters outside this crate.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod auction_kind;
pub mod auction_status;
pub mod entity;
pub mod errors;
pub mod reader;
