//! Gongzzang HTTP API service entrypoint.

#![forbid(unsafe_code)]

use std::process::ExitCode;

mod app;
mod http {
    pub mod mutation_ctx;
    pub mod problem;
    pub mod request_id;
}

pub mod foundation_anchor_import;
mod foundation_parcel_lookup;
mod observability;
mod photo_upload;

mod backend_authorization;
mod backend_rate_limit;
mod building_reader;
mod listing_marker_policy;
mod listing_marker_serving;
mod startup;
mod traffic_auth_policy;

mod routes {
    pub mod admin_listings;
    pub mod auth_event;
    pub mod bookmarks;
    pub mod buildings;
    pub mod floors;
    pub mod foundation_events;
    pub mod health;
    pub mod listing_marker_common;
    pub mod listing_marker_counts;
    pub mod listing_marker_deltas;
    pub mod listing_marker_filters;
    pub mod listing_marker_masks;
    pub mod listing_marker_tiles;
    pub mod listing_marker_tombstones;
    pub mod listings;
    pub mod metrics;
    pub mod notifications;
    pub mod parcels;
    pub mod users;
}

#[tokio::main]
async fn main() -> ExitCode {
    let _sentry_guard = observability::init_sentry();
    startup::init_tracing();
    app::run().await
}
