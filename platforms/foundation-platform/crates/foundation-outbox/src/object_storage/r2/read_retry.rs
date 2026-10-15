//! The one way this crate reads object bytes from R2 (root ADR-0102).
//!
//! The SDK's own retry ends when the response headers arrive. A body that breaks after them (a
//! reset connection, `streaming error`) and a request that never got an answer are asked for again
//! with a new request, after an equal-jitter exponential backoff: half the ceiling fixed, half
//! random, the ceiling doubling from `min_wait` up to `max_wait`. That is the strategy the Spark
//! side's `S3FileIO` settings name (`lakehouse_engine._assemble_catalog_settings`), with the same
//! numbers, so one R2 hiccup costs the same everywhere.
//!
//! A real answer that will not change on asking again (`404`, `412` from an `If-Match`, `403`) is
//! returned at once; `5xx` and `429` that outlived the SDK's adaptive retry are asked again.
//!
//! Every byte read of `R2ObjectStorage` goes through [`read`] (async) or [`read_blocking`] (the
//! seekable reader's blocking thread): whole objects, ranges, versioned reads.

use std::time::Duration;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use tokio::runtime::Handle;

use crate::errors::PublishError;

/// How hard one read tries (root ADR-0102). Tests use a fast copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct R2ReadPolicy {
    pub max_attempts: usize,
    pub min_wait: Duration,
    pub max_wait: Duration,
}

impl R2ReadPolicy {
    /// `s3.retry.num-retries=10`, `min-wait-ms=200`, `max-wait-ms=30000`: the Spark side's numbers.
    pub const ADR_0102: Self = Self {
        max_attempts: 11,
        min_wait: Duration::from_millis(200),
        max_wait: Duration::from_secs(30),
    };

    /// The wait after failed attempt `attempt` (1-based), with `unit` in `[0, 1)` as the random part.
    pub fn delay(&self, attempt: usize, unit: f64) -> Duration {
        let doublings = u32::try_from(attempt.saturating_sub(1))
            .unwrap_or(u32::MAX)
            .min(30);
        let ceiling = self
            .min_wait
            .saturating_mul(1_u32 << doublings)
            .min(self.max_wait);
        let half = ceiling / 2;
        half + half.mul_f64(unit.clamp(0.0, 1.0))
    }
}

/// What one read asks for.
#[derive(Clone, Copy, Debug)]
pub struct ReadRequest<'a> {
    pub key: &'a str,
    pub range: Option<&'a str>,
    pub if_match: Option<&'a str>,
}

/// One whole answer: its bytes and the version they belong to.
#[derive(Debug)]
pub struct ReadAnswer {
    pub bytes: Vec<u8>,
    pub e_tag: Option<String>,
}

enum Attempt {
    Done(ReadAnswer),
    Again(String),
    Final(String),
}

fn target(request: ReadRequest<'_>) -> String {
    request.range.map_or_else(
        || request.key.to_owned(),
        |range| format!("range {} {range}", request.key),
    )
}

/// Whether a request that got no usable answer may be asked again. A request that never reached R2
/// or never heard back may; a real answer may only when it is a server error or a throttle.
fn request_error_is_transient(error: &SdkError<GetObjectError, HttpResponse>) -> bool {
    match error {
        SdkError::DispatchFailure(_) | SdkError::TimeoutError(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(_) => error
            .raw_response()
            .map(|response| response.status().as_u16())
            .is_some_and(|status| status >= 500 || status == 429),
        _ => false,
    }
}

async fn attempt(client: &aws_sdk_s3::Client, bucket: &str, request: ReadRequest<'_>) -> Attempt {
    let mut get = client.get_object().bucket(bucket).key(request.key);
    if let Some(range) = request.range {
        get = get.range(range);
    }
    if let Some(e_tag) = request.if_match {
        get = get.if_match(e_tag);
    }
    let output = match get.send().await {
        Ok(output) => output,
        Err(error) => {
            let message = format!("failed to read R2 object {}: {error}", target(request));
            return if request_error_is_transient(&error) {
                Attempt::Again(message)
            } else {
                Attempt::Final(message)
            };
        }
    };
    let e_tag = output.e_tag().map(ToOwned::to_owned);
    match output.body.collect().await {
        Ok(body) => Attempt::Done(ReadAnswer {
            bytes: body.into_bytes().to_vec(),
            e_tag,
        }),
        Err(error) => Attempt::Again(format!(
            "failed to read R2 object body {}: {error}",
            target(request)
        )),
    }
}

/// A number in `[0, 1)` that differs per call, from the standard library's per-instance random keys.
fn jitter_unit() -> f64 {
    use std::hash::{BuildHasher as _, Hasher as _};
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
        >> 11;
    #[allow(clippy::cast_precision_loss)]
    let unit = bits as f64 / (1_u64 << 53) as f64;
    unit
}

fn outcome(
    policy: &R2ReadPolicy,
    request: ReadRequest<'_>,
    number: usize,
    result: Attempt,
) -> Result<Option<ReadAnswer>, PublishError> {
    match result {
        Attempt::Done(answer) => Ok(Some(answer)),
        Attempt::Again(message) if number < policy.max_attempts => {
            tracing::warn!(
                key = request.key,
                range = request.range,
                attempt = number,
                max_attempts = policy.max_attempts,
                error = %message,
                "asking R2 again after a transient read failure"
            );
            Ok(None)
        }
        Attempt::Again(message) | Attempt::Final(message) => {
            Err(PublishError::Broadcaster(message))
        }
    }
}

/// Reads one object or range through the policy.
pub async fn read(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    policy: &R2ReadPolicy,
    request: ReadRequest<'_>,
) -> Result<ReadAnswer, PublishError> {
    for number in 1..=policy.max_attempts {
        let result = attempt(client, bucket, request).await;
        if let Some(answer) = outcome(policy, request, number, result)? {
            return Ok(answer);
        }
        tokio::time::sleep(policy.delay(number, jitter_unit())).await;
    }
    Err(PublishError::Broadcaster(format!(
        "R2 read policy allows no attempt for {}",
        target(request)
    )))
}

/// The same, from a blocking thread that owns a runtime handle (the seekable reader). `on_attempt`
/// runs before every request, for the caller's request count.
pub fn read_blocking(
    handle: &Handle,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    policy: &R2ReadPolicy,
    request: ReadRequest<'_>,
    on_attempt: &dyn Fn(),
) -> Result<ReadAnswer, PublishError> {
    for number in 1..=policy.max_attempts {
        on_attempt();
        let result = handle.block_on(attempt(client, bucket, request));
        if let Some(answer) = outcome(policy, request, number, result)? {
            return Ok(answer);
        }
        std::thread::sleep(policy.delay(number, jitter_unit()));
    }
    Err(PublishError::Broadcaster(format!(
        "R2 read policy allows no attempt for {}",
        target(request)
    )))
}
