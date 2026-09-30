//! Dawneer staff console server (root ADR-0114, ADR-0116).
//!
//! The BFF between a staff member's browser and the platforms: it signs the staff member in with
//! Zitadel, keeps their tokens, and relays an allow-list of platform routes. It owns no data and
//! no business rules.

/// Routes.
pub mod app;
/// Settings.
pub mod config;
/// Zitadel sign-in.
pub mod oidc;
/// The routes the console may reach.
pub mod relay;
/// Session storage.
pub mod session;
