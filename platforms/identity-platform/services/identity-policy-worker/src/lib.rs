//! Leased, at-least-once publisher for Identity Platform outbox events.

mod config;
mod http_publisher;
mod postgres_outbox;
mod shutdown;
mod worker;

pub use config::{ConfigError, WorkerConfig};
pub use http_publisher::{
    HttpEventPublisher, HttpPublisherBuildError, PublisherEndpoint, PublisherEndpointError,
    IDEMPOTENCY_KEY_HEADER,
};
pub use postgres_outbox::{
    PgOutboxRepository, CLAIM_DUE_SQL, MARK_PUBLISHED_SQL, RECORD_FAILURE_SQL,
};
pub use shutdown::run_until_shutdown;
pub use worker::{
    ClaimRequest, DeliveryWorker, EventPublisher, LeasedOutboxEvent, OutboxRepository,
    PublishError, RepositoryError, TickStats, ValidatedOutboxEvent, ValidationError, WorkerError,
    WorkerOptions, MAX_ATTEMPTS,
};
