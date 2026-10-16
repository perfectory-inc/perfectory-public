//! 공짱이 R2 클라이언트를 만들고 객체를 읽는 한 곳 (루트 ADR-0261).
//!
//! 영역 사이에는 Cargo 경로 의존이 없으므로(루트 ADR-0001) foundation-platform 의 R2 읽기 경로를 쓰지 않고, 같은
//! 정책을 여기 한 번 적는다: 응답을 못 받은 요청과 헤더 뒤에 끊긴 본문은 **새 요청**으로, equal-jitter 지수
//! 백오프(11회, 200ms 에서 30s 까지 배증, 절반 고정·절반 무작위) 뒤에 다시 묻는다(루트 ADR-0102). 바뀌지 않을
//! 응답(`NoSuchKey`, `403`)은 바로 돌려준다.
//!
//! `scripts/guard/r2-reads-have-one-path.sh` 가 공짱 안에서 이 크레이트 밖의 S3 클라이언트 생성을 거부한다.

use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::{
    BehaviorVersion, Builder as S3ConfigBuilder, Region, RequestChecksumCalculation,
    ResponseChecksumValidation,
};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;

/// 공짱의 R2 계정 하나.
#[derive(Clone, Debug)]
pub struct R2Account {
    /// Cloudflare 계정 id: 엔드포인트 주소가 된다.
    pub account_id: String,
    /// R2 접근 키 id.
    pub access_key: String,
    /// R2 비밀 키.
    pub secret_key: String,
}

impl R2Account {
    /// 이 계정의 R2 S3 엔드포인트.
    #[must_use]
    pub fn endpoint_url(&self) -> String {
        format!("https://{}.r2.cloudflarestorage.com", self.account_id)
    }
}

/// 클라이언트를 쓰는 쪽마다 다른 것: 이름, SDK 가 한 요청을 몇 번 보내는지, 한 번의 상한.
#[derive(Clone, Copy, Debug)]
pub struct ClientProfile {
    /// 자격 증명에 붙는 쓰는 쪽의 이름.
    pub app_name: &'static str,
    /// SDK 가 응답 머리까지 한 요청을 보내는 횟수.
    pub sdk_attempts: u32,
    /// 한 번 보낸 요청의 상한.
    pub attempt_timeout: Duration,
}

/// 공짱의 S3 클라이언트는 여기서만 만든다.
#[must_use]
pub fn client(account: &R2Account, profile: ClientProfile) -> aws_sdk_s3::Client {
    client_at(account, &account.endpoint_url(), profile)
}

fn client_at(account: &R2Account, endpoint: &str, profile: ClientProfile) -> aws_sdk_s3::Client {
    let credentials = Credentials::new(
        &account.access_key,
        &account.secret_key,
        None,
        None,
        profile.app_name,
    );
    let config = S3ConfigBuilder::default()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("auto"))
        .endpoint_url(endpoint)
        .credentials_provider(credentials)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .retry_config(
            aws_config::retry::RetryConfig::standard().with_max_attempts(profile.sdk_attempts),
        )
        .timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .operation_attempt_timeout(profile.attempt_timeout)
                .build(),
        )
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// 한 읽기가 얼마나 다시 묻는지 (루트 ADR-0102). 시험은 빠른 사본을 쓴다.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadPolicy {
    /// 한 읽기가 묻는 최대 횟수.
    pub max_attempts: usize,
    /// 첫 실패 뒤 대기 상한의 시작값.
    pub min_wait: Duration,
    /// 대기 상한의 최댓값.
    pub max_wait: Duration,
}

impl ReadPolicy {
    /// foundation-platform 의 R2 읽기 정책과 Spark 엔진 계약 `r2_read` 와 같은 숫자.
    pub const ADR_0102: Self = Self {
        max_attempts: 11,
        min_wait: Duration::from_millis(200),
        max_wait: Duration::from_secs(30),
    };

    /// 실패한 `attempt`(1부터) 뒤의 대기. `unit` 은 `[0, 1)` 의 무작위 몫.
    #[must_use]
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

/// 읽은 객체 하나: 본문 전부와 그 응답의 머리.
#[derive(Debug)]
pub struct ReadObject {
    /// 본문 전부.
    pub bytes: Vec<u8>,
    /// 응답의 `Content-Type`.
    pub content_type: Option<String>,
    /// 응답의 `Content-Length`.
    pub content_length: Option<i64>,
}

/// 읽기가 끝내 실패한 이유.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadError {
    /// 그 키에 객체가 없다(`NoSuchKey`). 다시 물어도 같다.
    NotFound,
    /// 바뀌지 않을 거절이거나, 정책의 마지막 시도까지 실패했다.
    Failed(String),
}

enum Attempt {
    Done(ReadObject),
    Again(String),
    Final(ReadError),
}

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

async fn attempt(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Attempt {
    let output = match client.get_object().bucket(bucket).key(key).send().await {
        Ok(output) => output,
        Err(error) => {
            if error
                .as_service_error()
                .is_some_and(GetObjectError::is_no_such_key)
            {
                return Attempt::Final(ReadError::NotFound);
            }
            let message = format!("{}", aws_sdk_s3::error::DisplayErrorContext(&error));
            return if request_error_is_transient(&error) {
                Attempt::Again(message)
            } else {
                Attempt::Final(ReadError::Failed(message))
            };
        }
    };
    let content_type = output.content_type().map(ToOwned::to_owned);
    let content_length = output.content_length();
    match output.body.collect().await {
        Ok(body) => Attempt::Done(ReadObject {
            bytes: body.into_bytes().to_vec(),
            content_type,
            content_length,
        }),
        Err(error) => Attempt::Again(format!("R2 object body {key}: {error}")),
    }
}

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

/// 객체 하나를 정책대로 읽는다.
///
/// # Errors
///
/// 객체가 없으면 `ReadError::NotFound`, 바뀌지 않을 거절이거나 모든 시도가 실패하면 `ReadError::Failed`.
pub async fn read_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    policy: &ReadPolicy,
) -> Result<ReadObject, ReadError> {
    for number in 1..=policy.max_attempts {
        match attempt(client, bucket, key).await {
            Attempt::Done(object) => return Ok(object),
            Attempt::Final(error) => return Err(error),
            Attempt::Again(message) if number < policy.max_attempts => {
                tracing::warn!(key, attempt = number, max_attempts = policy.max_attempts, error = %message,
                    "asking R2 again after a transient read failure");
                tokio::time::sleep(policy.delay(number, jitter_unit())).await;
            }
            Attempt::Again(message) => return Err(ReadError::Failed(message)),
        }
    }
    Err(ReadError::Failed(format!(
        "the read policy allows no attempt for {key}"
    )))
}

#[cfg(test)]
mod tests;
