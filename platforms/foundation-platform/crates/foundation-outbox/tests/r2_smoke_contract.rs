//! Contract tests for the R2 smoke-test object key and optional live round trip.

use std::{error::Error, io};

use foundation_outbox::{
    object_storage::{
        normalize_r2_inventory_prefix, validate_r2_smoke_object_key, ObjectStorageSmokeReport,
        R2InventoryRequest, R2ObjectStorage, DEFAULT_R2_INVENTORY_MAX_KEYS,
        DEFAULT_R2_SMOKE_OBJECT_KEY, MAX_R2_INVENTORY_MAX_KEYS,
    },
    vector_tile_manifest::MANIFEST_POINTER_OBJECT_KEY,
};
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_R2_SMOKE_ENV: &str = "FOUNDATION_PLATFORM_R2_LIVE_SMOKE";
const LIVE_R2_INVENTORY_ENV: &str = "FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE";

#[test]
fn default_r2_smoke_key_is_dedicated_and_not_the_runtime_pointer() -> TestResult {
    assert_eq!(
        DEFAULT_R2_SMOKE_OBJECT_KEY,
        "gold/_smoke/foundation-platform-r2-smoke.json"
    );
    assert_ne!(DEFAULT_R2_SMOKE_OBJECT_KEY, MANIFEST_POINTER_OBJECT_KEY);
    validate_r2_smoke_object_key(DEFAULT_R2_SMOKE_OBJECT_KEY)?;
    Ok(())
}

#[test]
fn r2_smoke_key_rejects_empty_absolute_and_canonical_pointer_keys() -> TestResult {
    for key in [
        "",
        "   ",
        "/gold/_smoke/test.json",
        MANIFEST_POINTER_OBJECT_KEY,
    ] {
        let error = match validate_r2_smoke_object_key(key) {
            Ok(()) => {
                return Err(io::Error::other(format!("key {key:?} should be rejected")).into());
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("R2 smoke object key"),
            "unexpected error for {key:?}: {error}"
        );
    }
    Ok(())
}

#[test]
fn live_r2_smoke_requires_explicit_opt_in() {
    assert!(!live_r2_smoke_enabled(None));
    assert!(!live_r2_smoke_enabled(Some("")));
    assert!(!live_r2_smoke_enabled(Some(" 1 ")));
    assert!(!live_r2_smoke_enabled(Some("true")));
    assert!(live_r2_smoke_enabled(Some("1")));
}

#[test]
fn r2_smoke_report_exports_operation_metrics_without_object_key() {
    let report = ObjectStorageSmokeReport {
        key: "gold/_smoke/foundation-platform-r2-smoke-fixture.json".to_owned(),
        bytes_verified: 42,
        put_request_count: 1,
        get_request_count: 1,
        delete_request_count: 1,
    };

    let metrics = report.to_prometheus_metrics("live_r2_smoke");

    assert!(metrics.contains("# HELP foundation_platform_r2_smoke_request_total"));
    assert!(metrics.contains(
        "foundation_platform_r2_smoke_request_total{source=\"live_r2_smoke\",operation=\"put\"} 1"
    ));
    assert!(metrics.contains(
        "foundation_platform_r2_smoke_request_total{source=\"live_r2_smoke\",operation=\"get\"} 1"
    ));
    assert!(metrics.contains(
        "foundation_platform_r2_smoke_request_total{source=\"live_r2_smoke\",operation=\"delete\"} 1"
    ));
    assert!(metrics
        .contains("foundation_platform_r2_smoke_bytes_verified{source=\"live_r2_smoke\"} 42"));
    assert!(!metrics.contains("foundation-platform-r2-smoke-fixture.json"));
}

#[test]
fn r2_inventory_request_defaults_to_root_prefix_and_bounded_page_size() -> TestResult {
    let request = R2InventoryRequest::new(None, None)?;

    assert_eq!(request.prefix(), None);
    assert_eq!(request.max_keys(), DEFAULT_R2_INVENTORY_MAX_KEYS);
    Ok(())
}

#[test]
fn r2_inventory_prefix_accepts_provider_relative_prefixes() -> TestResult {
    assert_eq!(normalize_r2_inventory_prefix(None)?, None);
    assert_eq!(normalize_r2_inventory_prefix(Some(""))?, None);
    assert_eq!(
        normalize_r2_inventory_prefix(Some("bronze/source=molit-building-register/"))?,
        Some("bronze/source=molit-building-register/".to_owned())
    );
    Ok(())
}

#[test]
fn r2_inventory_prefix_rejects_ambiguous_or_absolute_prefixes() -> TestResult {
    for prefix in [
        " bronze/",
        "/bronze/",
        "bronze\\source",
        "bronze//source",
        "bronze/../gold/",
        "bronze/source=../gold/",
        "bronze/./source/",
    ] {
        let error = match normalize_r2_inventory_prefix(Some(prefix)) {
            Ok(value) => {
                return Err(io::Error::other(format!(
                    "prefix {prefix:?} should be rejected, got {value:?}"
                ))
                .into());
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("R2 inventory prefix"),
            "unexpected error for {prefix:?}: {error}"
        );
    }
    Ok(())
}

#[test]
fn r2_inventory_request_rejects_unbounded_page_sizes() -> TestResult {
    for max_keys in [0, -1, MAX_R2_INVENTORY_MAX_KEYS + 1] {
        let error = match R2InventoryRequest::new(None, Some(max_keys)) {
            Ok(request) => {
                return Err(io::Error::other(format!(
                    "max_keys {max_keys} should be rejected, got {request:?}"
                ))
                .into());
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("R2 inventory max_keys"),
            "unexpected error for {max_keys}: {error}"
        );
    }
    Ok(())
}

fn live_r2_smoke_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

#[tokio::test]
#[ignore = "requires Cloudflare R2 credentials and writes a temporary smoke object"]
async fn r2_smoke_round_trip_writes_reads_and_deletes_a_dedicated_object() -> TestResult {
    if !live_r2_smoke_enabled(std::env::var(LIVE_R2_SMOKE_ENV).ok().as_deref()) {
        return Ok(());
    }

    let storage = R2ObjectStorage::from_env()?;
    let key = format!(
        "gold/_smoke/foundation-platform-r2-smoke-{}.json",
        Uuid::new_v4()
    );
    let body = serde_json::to_vec(&json!({
        "service": "foundation-platform",
        "purpose": "r2-smoke",
        "key": key,
    }))?;

    let report = storage.round_trip_smoke(key.clone(), body.clone()).await?;

    assert_eq!(report.key, key);
    assert_eq!(report.bytes_verified, body.len());
    Ok(())
}

#[tokio::test]
#[ignore = "requires Cloudflare R2 credentials and performs a read-only ListObjectsV2 call"]
async fn r2_inventory_lists_root_prefix_without_mutating_objects() -> TestResult {
    if !live_r2_smoke_enabled(std::env::var(LIVE_R2_INVENTORY_ENV).ok().as_deref()) {
        return Ok(());
    }

    let storage = R2ObjectStorage::from_env()?;
    let request = R2InventoryRequest::new(None, Some(20))?;
    let report = storage.inventory(request).await?;

    assert_eq!(report.prefix(), None);
    assert_eq!(report.max_keys(), 20);
    assert!(report.key_count() <= 20);
    Ok(())
}
