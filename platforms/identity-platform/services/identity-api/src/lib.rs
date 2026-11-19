//! Identity Platform HTTP deployable composition.

mod error;
mod openapi;
mod routes;
/// Production and test composition state.
pub mod state;

use std::sync::Arc;

use axum::Router;

pub use openapi::openapi_document;
pub use state::{AppState, ProductionConfig, StaffAdministration};

/// Builds the Identity HTTP router from an explicit application state.
pub fn router(state: Arc<AppState>) -> Router {
    routes::router(state)
}
