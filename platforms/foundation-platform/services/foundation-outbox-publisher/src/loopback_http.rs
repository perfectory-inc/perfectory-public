//! Loopback-aware HTTP send helper for cutover tooling.

use std::time::{Duration, Instant};

/// Adds a send variant that retries connect-class failures against loopback hosts.
///
/// Guardrail tests start their fake servers as PowerShell background jobs without a
/// readiness signal, so the first connect can race the listener bind — a race the slower
/// job spin-up on the Linux CI runner loses. Only loopback hosts are retried (bounded at
/// 10s), so behavior toward real endpoints is unchanged.
pub(crate) trait LoopbackRetrySend: Sized {
    /// Sends the request, retrying loopback connect failures until the deadline.
    async fn send_with_loopback_connect_retry(self) -> Result<reqwest::Response, reqwest::Error>;
}

impl LoopbackRetrySend for reqwest::RequestBuilder {
    async fn send_with_loopback_connect_retry(self) -> Result<reqwest::Response, reqwest::Error> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let Some(attempt) = self.try_clone() else {
                // Non-cloneable bodies (streams) cannot be retried; send as-is.
                return self.send().await;
            };
            match attempt.send().await {
                Ok(response) => return Ok(response),
                Err(error)
                    if error.is_connect()
                        && error_targets_loopback(&error)
                        && Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn error_targets_loopback(error: &reqwest::Error) -> bool {
    error
        .url()
        .is_some_and(|url| matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1")))
}
