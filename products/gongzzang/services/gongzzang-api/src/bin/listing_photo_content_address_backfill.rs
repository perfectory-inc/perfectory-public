//! Copies id-key listing photos into the content-addressed media store (root ADR-0427 §3 ③).
//!
//! Before ADR-0427 a listing photo was put at an id key (`media/listing-photo/listings/<lst>/photos/
//! <lph>.<ext>`). This job reads each such photo that has no hash yet, puts the same bytes at
//! `media/listing-photo/sha256/<hex>.<ext>` (write-once: `If-None-Match: *`, the store checks the
//! sha256) and appends `(old key -> hash)` to `listing_photo_content_address`. It never deletes or
//! rewrites anything: the old object stays and keeps being served through `r2_key`.
//!
//! Partial: one run takes at most `LISTING_PHOTO_BACKFILL_BATCH_LIMIT` (default 200, at most 5,000)
//! unmapped, confirmed, non-deleted photos; a mapped photo is never read again. A photo whose bytes
//! are not the format its row declares is counted and left unmapped (it keeps its old key).
//!
//! Run by the person who operates gongzzang, after the code that reads both layouts is deployed
//! (root ADR-0427 배포 순서 ②→③). No deploy runs it. Env: `DATABASE_URL`,
//! `LISTING_PHOTO_R2_ACCOUNT_ID`, `LISTING_PHOTO_R2_ACCESS_KEY`, `LISTING_PHOTO_R2_SECRET_KEY`,
//! `LISTING_PHOTO_R2_BUCKET`.

#![forbid(unsafe_code)]

use std::process::ExitCode;
use std::time::Duration;

use sha2::{Digest as _, Sha256};
use shared_kernel::media::{
    content_addressed_key, MediaFormat, Sha256Hex, LISTING_PHOTO_MEDIA_PREFIX,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row as _;

const BATCH_LIMIT_ENV: &str = "LISTING_PHOTO_BACKFILL_BATCH_LIMIT";
const DEFAULT_BATCH_LIMIT: i64 = 200;
const MAX_BATCH_LIMIT: i64 = 5_000;

#[derive(Debug, Default)]
struct Report {
    mapped: usize,
    already_stored: usize,
    format_mismatched: usize,
    missing: usize,
    failed: usize,
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt::try_init();
    match run().await {
        Ok(report) => {
            tracing::info!(
                mapped = report.mapped,
                already_stored = report.already_stored,
                format_mismatched = report.format_mismatched,
                missing = report.missing,
                failed = report.failed,
                "listing photo content-address backfill completed"
            );
            if report.failed == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "listing photo content-address backfill failed");
            ExitCode::FAILURE
        }
    }
}

fn env(name: &str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("env {name} not set"))
}

fn batch_limit(raw: Option<&str>) -> Result<i64, String> {
    raw.map_or(Ok(DEFAULT_BATCH_LIMIT), |raw| {
        raw.parse::<i64>()
            .ok()
            .filter(|limit| (1..=MAX_BATCH_LIMIT).contains(limit))
            .ok_or_else(|| format!("{BATCH_LIMIT_ENV} must be 1..={MAX_BATCH_LIMIT}"))
    })
}

/// The content-addressed key and hash of an id-key photo's bytes, or `None` when the bytes are not
/// the format the row declares.
fn content_address(bytes: &[u8], declared_content_type: &str) -> Option<(Sha256Hex, String)> {
    let format = MediaFormat::sniff(bytes)?;
    if format.content_type() != declared_content_type {
        return None;
    }
    let hash = Sha256Hex::parse(&format!("{:x}", Sha256::digest(bytes))).ok()?;
    let key = content_addressed_key(LISTING_PHOTO_MEDIA_PREFIX, &hash, format);
    Some((hash, key))
}

#[allow(clippy::too_many_lines)]
async fn run() -> Result<Report, String> {
    let limit = batch_limit(std::env::var(BATCH_LIMIT_ENV).ok().as_deref())?;
    let bucket = env("LISTING_PHOTO_R2_BUCKET")?;
    let client = gongzzang_r2_object_storage::client(
        &gongzzang_r2_object_storage::R2Account {
            account_id: env("LISTING_PHOTO_R2_ACCOUNT_ID")?,
            access_key: env("LISTING_PHOTO_R2_ACCESS_KEY")?,
            secret_key: env("LISTING_PHOTO_R2_SECRET_KEY")?,
        },
        gongzzang_r2_object_storage::ClientProfile {
            app_name: "listing-photo-content-address-backfill",
            sdk_attempts: 2,
            attempt_timeout: Duration::from_secs(60),
        },
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&env("DATABASE_URL")?)
        .await
        .map_err(|error| error.to_string())?;

    let rows = sqlx::query(
        "select p.r2_key, p.content_type from listing_photo p \
         left join listing_photo_content_address m on m.legacy_r2_key = p.r2_key \
         where p.content_sha256 is null and m.legacy_r2_key is null \
           and p.deleted_at is null and p.file_size_bytes is not null \
         order by p.r2_key limit $1",
    )
    .bind(limit)
    .fetch_all(&pool)
    .await
    .map_err(|error| error.to_string())?;

    let mut report = Report::default();
    for row in rows {
        let legacy_key: String = row.try_get("r2_key").map_err(|e| e.to_string())?;
        let content_type: String = row.try_get("content_type").map_err(|e| e.to_string())?;
        let object = match gongzzang_r2_object_storage::read_object(
            &client,
            &bucket,
            &legacy_key,
            &gongzzang_r2_object_storage::ReadPolicy::ADR_0102,
        )
        .await
        {
            Ok(object) => object,
            Err(gongzzang_r2_object_storage::ReadError::NotFound) => {
                report.missing += 1;
                tracing::warn!(legacy_key, "listing photo object is missing; left unmapped");
                continue;
            }
            Err(gongzzang_r2_object_storage::ReadError::Failed(error)) => {
                report.failed += 1;
                tracing::warn!(legacy_key, error, "listing photo read failed");
                continue;
            }
        };
        let Some((hash, content_key)) = content_address(&object.bytes, &content_type) else {
            report.format_mismatched += 1;
            tracing::warn!(
                legacy_key,
                content_type,
                "bytes are not the declared format"
            );
            continue;
        };
        let size_bytes = i64::try_from(object.bytes.len()).map_err(|e| e.to_string())?;
        let put = client
            .put_object()
            .bucket(&bucket)
            .key(&content_key)
            .content_type(content_type)
            .checksum_sha256(aws_smithy_types::base64::encode(Sha256::digest(
                &object.bytes,
            )))
            .if_none_match("*")
            .body(object.bytes.into())
            .send()
            .await;
        match put {
            Ok(_) => {}
            // The key is the hash: an object already there holds these bytes.
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 412) =>
            {
                report.already_stored += 1;
            }
            Err(error) => {
                report.failed += 1;
                tracing::warn!(
                    legacy_key,
                    error = %aws_sdk_s3::error::DisplayErrorContext(&error),
                    "content-addressed put failed"
                );
                continue;
            }
        }
        sqlx::query(
            "insert into listing_photo_content_address \
             (legacy_r2_key, content_sha256, content_key, size_bytes) values ($1, $2, $3, $4) \
             on conflict (legacy_r2_key) do nothing",
        )
        .bind(&legacy_key)
        .bind(hash.as_str())
        .bind(&content_key)
        .bind(size_bytes)
        .execute(&pool)
        .await
        .map_err(|error| error.to_string())?;
        report.mapped += 1;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::{batch_limit, content_address, DEFAULT_BATCH_LIMIT};

    #[test]
    fn the_batch_is_bounded() {
        assert_eq!(batch_limit(None), Ok(DEFAULT_BATCH_LIMIT));
        assert_eq!(batch_limit(Some("10")), Ok(10));
        assert!(batch_limit(Some("0")).is_err());
        assert!(batch_limit(Some("5001")).is_err());
    }

    #[test]
    fn a_photo_is_addressed_by_its_bytes_only_when_they_are_the_declared_format() {
        let jpeg = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        let addressed = content_address(&jpeg, "image/jpeg");
        assert!(addressed.is_some());
        if let Some((hash, key)) = addressed {
            assert_eq!(
                key,
                format!("media/listing-photo/sha256/{}.jpg", hash.as_str())
            );
            assert_eq!(
                content_address(&jpeg, "image/jpeg").map(|(h, _)| h),
                Some(hash)
            );
        }
        assert!(content_address(&jpeg, "image/png").is_none());
        assert!(content_address(b"<html>gone</html>", "image/jpeg").is_none());
    }
}
