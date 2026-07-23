use std::time::Duration;

#[derive(Clone, Debug)]
/// Runtime settings for polling outbox tables.
pub struct PublisherConfig {
    /// Delay between polling cycles when the worker runs continuously.
    pub poll_interval: Duration,
    /// Maximum number of outbox rows selected per tick.
    pub batch_size: i64,
    /// Maximum retry count before an event is treated as exhausted.
    pub max_retries: i32,
    /// Base duration reserved for retry backoff strategies.
    pub backoff_base: Duration,
    /// How long a worker owns a row while its external delivery is in flight.
    pub lease_duration: Duration,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            batch_size: 50,
            max_retries: 5,
            backoff_base: Duration::from_secs(1),
            lease_duration: Duration::from_mins(1),
        }
    }
}
