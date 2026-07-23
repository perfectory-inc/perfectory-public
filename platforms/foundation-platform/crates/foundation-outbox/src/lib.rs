//! Outbox publishing primitives for foundation-platform.
//!
//! The crate polls the Catalog outbox table and publishes events through
//! pluggable broadcaster adapters, and writes Catalog-owned runtime artifacts
//! such as the vector tile manifest pointer to object storage.

#![deny(missing_docs)]

/// Event envelope and broadcaster abstractions.
pub mod broadcaster;
/// Runtime configuration for outbox polling.
pub mod config;
/// Error types returned by publisher adapters and workers.
pub mod errors;
/// Collection-job dispatch port (`JobBus`) and in-memory reference implementation.
pub mod jobbus;
/// Lakehouse lineage event HTTP publisher.
pub mod lineage;
/// Object storage adapters used by Catalog runtime artifact publishing.
pub mod object_storage;
/// Production `RawWrittenSink` that records `collection.raw_written` into the Postgres outbox.
pub mod outbox_raw_written_sink;
/// Static vector tile manifest pointer publishing.
pub mod vector_tile_manifest;
/// HTTP webhook broadcaster for outbox fan-out.
pub mod webhook;
/// Database outbox polling worker.
pub mod worker;

pub use broadcaster::{EventBroadcaster, LoggingBroadcaster};
pub use config::PublisherConfig;
pub use errors::PublishError;
pub use jobbus::{
    CollectionJob, CollectionSuccess, FailureDisposition, InMemoryJobBus, JobBus, JobBusError,
    JobFailure, JobLease, LeasedJob, NackOutcome, RawWrittenSink, RecordingRawWrittenSink,
};
pub use lineage::LakehouseLineagePublisher;
pub use object_storage::{
    FileObjectStorage, LoggingObjectStorage, ObjectStorageService, ObjectStorageStreamingService,
    R2ObjectStorage,
};
pub use outbox_raw_written_sink::OutboxRawWrittenSink;
pub use vector_tile_manifest::{CatalogEventBroadcaster, PgVectorTileManifestReader};
pub use webhook::WebhookBroadcaster;
pub use worker::{OutboxScope, OutboxWorker};
