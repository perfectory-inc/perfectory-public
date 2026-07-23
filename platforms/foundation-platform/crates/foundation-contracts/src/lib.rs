//! Foundation Platform HTTP request and response DTOs.
//!
//! This crate is the Rust source for wire contracts shared by Foundation Platform services,
//! `OpenAPI` documents, and generated consumers. Domain validation remains in the owning
//! capability; this crate owns only stable transport shapes.

#![deny(missing_docs)]

/// Catalog context DTOs for industrial complex, file, spatial, and vector-tile APIs.
pub mod catalog;

/// Shared Foundation Platform HTTP error envelopes.
pub mod error;

/// Normalization proposal, review, application, and rollback DTOs.
pub mod normalization;
