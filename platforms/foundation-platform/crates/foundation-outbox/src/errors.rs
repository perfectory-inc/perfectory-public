use thiserror::Error;

#[derive(Debug, Error)]
/// Error returned while publishing or polling outbox events.
pub enum PublishError {
    /// Adapter-level publication failure.
    #[error("broadcaster error: {0}")]
    Broadcaster(String),
    /// Infrastructure failure such as DB, configuration, or object storage setup.
    #[error("infrastructure error: {0}")]
    Infrastructure(String),
    /// A `CreateOnly` write was rejected because the object key already exists.
    ///
    /// Surfaced by the storage port (R2 `412 Precondition Failed` under
    /// `If-None-Match: *`, or a local filesystem `AlreadyExists`) so the caller can
    /// run the write-once reconcile/recover protocol instead of treating it as a
    /// plain failure.
    // Task: 412 -> checksum reconcile/recover/quarantine is wired in the NEXT task; for
    // now this variant only carries the colliding key.
    #[error("object already exists: {key}")]
    ObjectAlreadyExists {
        /// Provider-relative object key that already existed.
        key: String,
    },
}
