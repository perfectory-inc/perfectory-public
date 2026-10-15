use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use collection_application::bronze_catalog_recovery::{
    BronzeCatalogRecoveryMode, BronzeCatalogRecoveryObjectReader,
    BronzeCatalogRecoveryStorageError, ExistingBronzeObject,
};

use super::{
    parse_verification_concurrency, validate_manifest_uri_policy,
    BoundedBronzeCatalogRecoveryReader, RecoveryExecutionPolicy,
};

#[test]
fn dry_run_is_the_default_and_does_not_require_database_access() {
    let policy = RecoveryExecutionPolicy::from_values(None, None, None, Some("2"), None)
        .expect("bounded dry-run policy should be valid");

    assert_eq!(policy.mode, BronzeCatalogRecoveryMode::DryRun);
    assert_eq!(policy.selected_source_slug, None);
    assert_eq!(policy.max_candidates, Some(2));
    assert_eq!(policy.database_url, None);
}

#[test]
fn apply_requires_explicit_confirmation_database_and_single_source() {
    for (confirmation, source, database_url) in [
        (None, Some("vworldkr__parcel"), Some("postgres://db")),
        (Some("APPLY"), None, Some("postgres://db")),
        (Some("APPLY"), Some("vworldkr__parcel"), None),
    ] {
        assert!(RecoveryExecutionPolicy::from_values(
            Some("apply"),
            confirmation,
            source,
            None,
            database_url,
        )
        .is_err());
    }
}

#[test]
fn apply_forbids_partial_candidate_limit() {
    let error = RecoveryExecutionPolicy::from_values(
        Some("apply"),
        Some("APPLY"),
        Some("vworldkr__parcel"),
        Some("1"),
        Some("postgres://db"),
    )
    .expect_err("apply must never silently recover a partial candidate slice");

    assert!(error.to_string().contains("MAX_CANDIDATES"));
}

#[test]
fn explicitly_confirmed_single_source_apply_is_accepted() {
    let policy = RecoveryExecutionPolicy::from_values(
        Some("apply"),
        Some("APPLY"),
        Some("vworldkr__parcel"),
        None,
        Some("postgres://db"),
    )
    .expect("fully explicit apply policy should be valid");

    assert_eq!(policy.mode, BronzeCatalogRecoveryMode::Apply);
    assert_eq!(
        policy.selected_source_slug.as_deref(),
        Some("vworldkr__parcel")
    );
    assert_eq!(policy.database_url.as_deref(), Some("postgres://db"));
}

#[test]
fn apply_forbids_manual_manifest_uri_override() {
    let error = validate_manifest_uri_policy(
        BronzeCatalogRecoveryMode::Apply,
        Some("r2://somewhere/unsealed.json"),
    )
    .expect_err("apply must seal and select its own immutable evidence URI");

    assert!(error.to_string().contains("forbids MANIFEST_URI override"));
}

#[test]
fn verification_concurrency_is_bounded_and_defaults_to_32() {
    assert_eq!(parse_verification_concurrency(None).unwrap(), 32);
    assert_eq!(parse_verification_concurrency(Some("1")).unwrap(), 1);
    assert_eq!(parse_verification_concurrency(Some("64")).unwrap(), 64);

    for invalid in ["0", "65", "many"] {
        assert!(parse_verification_concurrency(Some(invalid)).is_err());
    }
}

#[tokio::test]
async fn bounded_reader_overlaps_reads_without_exceeding_limit_and_restores_order() {
    let inner = TrackingReader::default();
    let reader = BoundedBronzeCatalogRecoveryReader::new(&inner, 2);
    let keys = (1..=6)
        .map(|index| format!("key-{index}"))
        .collect::<Vec<_>>();

    let results = reader.read_existing_objects(&keys).await;

    assert!(inner.max_in_flight.load(Ordering::SeqCst) > 1);
    assert!(inner.max_in_flight.load(Ordering::SeqCst) <= 2);
    let observed_order = results
        .into_iter()
        .map(|result| result.unwrap().unwrap().observed_r2_etag)
        .collect::<Vec<_>>();
    assert_eq!(observed_order, keys);
}

#[derive(Default)]
struct TrackingReader {
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

#[async_trait]
impl BronzeCatalogRecoveryObjectReader for TrackingReader {
    async fn read_existing_object(
        &self,
        key: &str,
    ) -> Result<Option<ExistingBronzeObject>, BronzeCatalogRecoveryStorageError> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(current, Ordering::SeqCst);
        let index = key
            .strip_prefix("key-")
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| BronzeCatalogRecoveryStorageError("invalid test key".to_owned()))?;
        tokio::time::sleep(Duration::from_millis((7 - index) * 5)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(Some(ExistingBronzeObject {
            checksum_sha256: "a".repeat(64),
            size_bytes: index,
            observed_r2_etag: key.to_owned(),
            observed_r2_last_modified: Utc::now(),
        }))
    }
}
