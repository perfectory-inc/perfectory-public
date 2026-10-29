//! Contract tests for the Iceberg REST catalog adapter.

use std::error::Error;

use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_domain::SILVER_INDUSTRIAL_COMPLEXES;
use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogConfig, LakehouseCatalogProvider,
};
use uuid::Uuid;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> LakehouseCatalogConfig {
    LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        catalog_uri: server.uri(),
        warehouse: "foundation-platform".to_owned(),
        catalog_token: Some("secret-token".to_owned()),
    }
}

fn config_with_uri(catalog_uri: String) -> LakehouseCatalogConfig {
    LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        catalog_uri,
        warehouse: "foundation-platform".to_owned(),
        catalog_token: Some("secret-token".to_owned()),
    }
}

async fn mount_catalog_config(server: &MockServer, prefix: &str) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(query_param("warehouse", "foundation-platform"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header("x-iceberg-access-delegation", "vended-credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "overrides": {
                "prefix": prefix
            },
            "defaults": {}
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn loads_current_snapshot_through_catalog_config_prefix() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path("/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00001.json",
            "metadata": {
                "current-snapshot-id": 123_456_789
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;

    assert_eq!(snapshot.table_name, "silver.industrial_complexes");
    assert_eq!(snapshot.snapshot_id, "123456789");
    assert_eq!(
        snapshot.metadata_location,
        "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00001.json"
    );
    Ok(())
}

#[tokio::test]
async fn loads_table_uuid_and_complete_snapshot_identity_set() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/parcel_boundaries",
        ))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/parcel_boundaries/metadata/00003.json",
            "metadata": {
                "table-uuid": "2f7bf2d1-3e08-4d1a-936e-556d8ebfd055",
                "current-snapshot-id": 841_361_364_657_368_626_i64,
                "snapshots": [
                    {"snapshot-id": 841_361_364_657_368_624_i64},
                    {"snapshot-id": 841_361_364_657_368_626_i64}
                ]
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let metadata = catalog
        .get_table_metadata("silver.parcel_boundaries")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;

    assert_eq!(
        metadata.table_uuid,
        Uuid::parse_str("2f7bf2d1-3e08-4d1a-936e-556d8ebfd055")?
    );
    assert_eq!(
        metadata.snapshot_ids,
        vec![841_361_364_657_368_624, 841_361_364_657_368_626]
    );
    assert_eq!(metadata.current_snapshot_id, Some(841_361_364_657_368_626));
    assert_eq!(metadata.table_name, "silver.parcel_boundaries");
    Ok(())
}

#[tokio::test]
async fn current_manifest_list_carries_the_snapshot_update_timestamp() -> Result<(), Box<dyn Error>>
{
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/parcel_boundaries",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "s3://foundation-platform-lakehouse-prod/silver/parcel_boundaries/metadata/00003.json",
            "metadata": {
                "current-snapshot-id": 841_361_364_657_368_626_i64,
                "snapshots": [{
                    "snapshot-id": 841_361_364_657_368_626_i64,
                    "timestamp-ms": 1_777_777_777_000_i64,
                    "manifest-list": "s3://foundation-platform-lakehouse-prod/silver/parcel_boundaries/metadata/snap-3.avro"
                }]
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list("silver.parcel_boundaries")
        .await?
        .ok_or_else(|| std::io::Error::other("current snapshot should exist"))?;

    assert_eq!(snapshot.snapshot_id, 841_361_364_657_368_626);
    assert_eq!(snapshot.snapshot_timestamp_ms, 1_777_777_777_000);
    Ok(())
}

#[tokio::test]
async fn accepts_catalog_uri_that_already_ends_with_v1() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00001.json",
            "metadata": {
                "current-snapshot-id": 123_456_789
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config_with_uri(format!("{}/v1", server.uri())))?;
    let snapshot = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await?;

    assert!(snapshot.is_some());
    Ok(())
}

#[tokio::test]
async fn missing_table_returns_none_instead_of_infrastructure_error() -> Result<(), Box<dyn Error>>
{
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .and(header("authorization", "Bearer secret-token"))
        .and(header("x-iceberg-access-delegation", "vended-credentials"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .get_current_snapshot(SILVER_INDUSTRIAL_COMPLEXES.table_name)
        .await?;

    assert!(snapshot.is_none());
    Ok(())
}

#[tokio::test]
async fn load_table_retries_transient_failures_until_success() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // One transient 503, then the real table: the adapter must retry past the failure.
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00003.json",
            "metadata": {
                "current-snapshot-id": 42
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist after retry"))?;

    assert_eq!(snapshot.snapshot_id, "42");
    Ok(())
}

#[tokio::test]
async fn load_table_does_not_retry_malformed_json_payloads() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // expect(1): a decode failure is fatal — only transport-level body failures retry.
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("<html>not json</html>"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let error = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected JSON decode failure"))?;

    assert!(
        !error.to_string().contains("attempts"),
        "decode failures must not be retried: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn load_table_fails_fast_on_non_retryable_status() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // expect(1): a 401 must not be retried even though the policy allows multiple attempts.
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let error = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected non-retryable load table failure"))?;

    assert!(
        error
            .to_string()
            .contains("Iceberg REST load table failed with status 401"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn ensure_table_returns_existing_snapshot_without_cloudflare_business_api(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path("/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00002.json",
            "metadata": {
                "current-snapshot-id": "987654321"
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog.ensure_table(&SILVER_INDUSTRIAL_COMPLEXES).await?;

    assert_eq!(snapshot.table_name, "silver.industrial_complexes");
    assert_eq!(snapshot.snapshot_id, "987654321");
    Ok(())
}

// ---- Tags that pin a served snapshot (root ADR-0146) ------------------------------------------
//
// Snapshot ids sit in the repository's synthetic namespace (`scripts/guard/public-fixture-safety.py`).

const TAGGED: i64 = 999_990_000_000_000_001;
const OTHER: i64 = 999_990_000_000_000_002;
const GOLD_PATH: &str = "/v1/cloudflare-catalog-prefix/namespaces/gold/tables/parcel_panel";

async fn mount_gold(server: &MockServer, refs: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00002.json",
            "metadata": {
                "current-snapshot-id": OTHER,
                "snapshots": [{"snapshot-id": TAGGED}, {"snapshot-id": OTHER}],
                "refs": refs
            }
        })))
        .mount(server)
        .await;
}

const STAGED: i64 = 999_990_000_000_000_004;

#[tokio::test]
async fn ingested_batch_objects_are_the_identities_main_records() -> Result<(), Box<dyn Error>> {
    // Root ADR-0183: a load staged on its branch is not in the table readers see until published.
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00003.json",
            "metadata": {
                "current-snapshot-id": OTHER,
                "refs": {
                    "main": {"snapshot-id": OTHER, "type": "branch"},
                    "refresh_fixture": {"snapshot-id": STAGED, "type": "branch"}
                },
                "snapshots": [
                    {"snapshot-id": TAGGED, "summary": {
                        "operation": "append",
                        "foundation.ingest-batch-objects": "fixture-load-a,fixture-load-b"}},
                    {"snapshot-id": OTHER, "parent-snapshot-id": TAGGED, "summary": {"operation": "replace"}},
                    {"snapshot-id": STAGED, "parent-snapshot-id": OTHER, "summary": {
                        "operation": "append",
                        "foundation.ingest-batch-objects": "fixture-load-c"}}
                ]
            }
        })))
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    let ingested = catalog
        .load_ingested_batch_objects("gold.parcel_panel")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;
    assert_eq!(
        ingested.into_iter().collect::<Vec<_>>(),
        ["fixture-load-a", "fixture-load-b"]
    );
    Ok(())
}

async fn mount_staged(server: &MockServer, main: i64) {
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00004.json",
            "metadata": {
                "current-snapshot-id": main,
                "refs": {
                    "main": {"snapshot-id": main, "type": "branch"},
                    "refresh_fixture": {"snapshot-id": STAGED, "type": "branch"}
                },
                "snapshots": [
                    {"snapshot-id": TAGGED, "summary": {"operation": "append"}},
                    {"snapshot-id": OTHER, "parent-snapshot-id": TAGGED, "summary": {"operation": "replace"}},
                    {"snapshot-id": STAGED, "parent-snapshot-id": TAGGED, "summary": {"operation": "append"}}
                ]
            }
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_branch_is_published_as_main_in_one_commit_only_as_a_fast_forward(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // main is still TAGGED, the snapshot the branch was staged on.
    mount_staged(&server, TAGGED).await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "requirements": [
                {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": TAGGED},
                {"type": "assert-ref-snapshot-id", "ref": "refresh_fixture", "snapshot-id": STAGED}
            ],
            "updates": [
                {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": STAGED},
                {"action": "remove-snapshot-ref", "ref-name": "refresh_fixture"}
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    assert_eq!(
        catalog
            .publish_branch("gold.parcel_panel", "refresh_fixture")
            .await?,
        STAGED
    );
    assert!(
        catalog
            .publish_branch("gold.parcel_panel", "no_such_branch")
            .await
            .is_err(),
        "a missing branch was published"
    );
    // The branch is current: a resumed load keeps it.
    assert!(
        !catalog
            .remove_stale_branch("gold.parcel_panel", "refresh_fixture")
            .await?
    );
    Ok(())
}

/// ADR-0328: an edition's record outlives its snapshot summary. Without it, `refuse_older_edition`
/// stops refusing an older vintage 30 days after the newer one landed (root ADR-0237 expiry).
#[tokio::test]
async fn publishing_a_branch_records_its_new_editions_as_table_properties_in_the_same_commit(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00004.json",
            "metadata": {
                "current-snapshot-id": TAGGED,
                "properties": {"foundation.edition.209908": "1"},
                "refs": {
                    "main": {"snapshot-id": TAGGED, "type": "branch"},
                    "changes_fixture": {"snapshot-id": STAGED, "type": "branch"}
                },
                "snapshots": [
                    {"snapshot-id": TAGGED, "summary": {"operation": "append", "foundation.edition": "209908"}},
                    {"snapshot-id": STAGED, "parent-snapshot-id": TAGGED,
                     "summary": {"operation": "append", "foundation.edition": "209911"}}
                ]
            }
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "requirements": [
                {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": TAGGED},
                {"type": "assert-ref-snapshot-id", "ref": "changes_fixture", "snapshot-id": STAGED}
            ],
            "updates": [
                {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": STAGED},
                {"action": "remove-snapshot-ref", "ref-name": "changes_fixture"},
                {"action": "set-properties", "updates": {"foundation.edition.209911": STAGED.to_string()}}
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    assert_eq!(
        catalog
            .publish_branch("gold.parcel_panel", "changes_fixture")
            .await?,
        STAGED
    );
    Ok(())
}

#[tokio::test]
async fn a_branch_main_moved_away_from_is_never_published_and_is_removed_as_stale(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // main moved to OTHER after the branch was staged on TAGGED.
    mount_staged(&server, OTHER).await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "requirements": [
                {"type": "assert-ref-snapshot-id", "ref": "refresh_fixture", "snapshot-id": STAGED}
            ],
            "updates": [{"action": "remove-snapshot-ref", "ref-name": "refresh_fixture"}]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    let refused = catalog
        .publish_branch("gold.parcel_panel", "refresh_fixture")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("a branch off main's lineage was published"))?;
    assert!(refused.to_string().contains("main moved"), "{refused}");
    assert!(
        catalog
            .remove_stale_branch("gold.parcel_panel", "refresh_fixture")
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn a_snapshot_record_count_is_its_summary_total_records() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00002.json",
            "metadata": {
                "current-snapshot-id": OTHER,
                "snapshots": [
                    {"snapshot-id": TAGGED, "summary": {"operation": "overwrite", "total-records": "5945767"}},
                    {"snapshot-id": OTHER, "summary": {"operation": "overwrite"}}
                ]
            }
        })))
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    assert_eq!(
        catalog
            .load_snapshot_record_count("gold.parcel_panel", TAGGED)
            .await?,
        Some(5_945_767)
    );
    assert!(
        catalog
            .load_snapshot_record_count("gold.parcel_panel", OTHER)
            .await
            .is_err(),
        "a summary without total-records was read as a count"
    );
    assert!(
        catalog
            .load_snapshot_record_count("gold.parcel_panel", 999_990_000_000_000_009)
            .await
            .is_err(),
        "an expired snapshot was read as a count"
    );
    Ok(())
}

/// Root ADR-0310: a bake pinned to the snapshot it started from reads that snapshot after `main`
/// moved on (generation 5 of the parcel lane was thrown away after 15 h on 2026-10-24 because the
/// bake read only the current snapshot); an expired pin is named as such.
#[tokio::test]
async fn a_pinned_snapshot_is_read_after_main_moved_and_an_expired_one_is_named(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00003.json",
            "metadata": {
                "current-snapshot-id": OTHER,
                "snapshots": [
                    {"snapshot-id": TAGGED, "timestamp-ms": 1_777_777_000_000_i64,
                     "manifest-list": "r2://lakehouse/gold/parcel_panel/metadata/snap-pinned.avro"},
                    {"snapshot-id": OTHER, "timestamp-ms": 1_777_777_777_000_i64,
                     "manifest-list": "r2://lakehouse/gold/parcel_panel/metadata/snap-now.avro"}
                ]
            }
        })))
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    let pinned = catalog
        .load_snapshot_manifest_list("gold.parcel_panel", TAGGED)
        .await?
        .ok_or("the table was not found")?;
    assert_eq!(pinned.snapshot_id, TAGGED);
    assert_eq!(
        pinned.manifest_list_location,
        "r2://lakehouse/gold/parcel_panel/metadata/snap-pinned.avro"
    );
    let expired = catalog
        .load_snapshot_manifest_list("gold.parcel_panel", 999_990_000_000_000_009)
        .await
        .err()
        .ok_or("an expired pin was read")?;
    assert!(
        expired.to_string().contains("no longer holds snapshot"),
        "{expired}"
    );
    Ok(())
}

#[tokio::test]
async fn a_tag_is_committed_only_where_no_reference_of_its_name_exists(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    mount_gold(
        &server,
        serde_json::json!({"main": {"snapshot-id": OTHER, "type": "branch"}}),
    )
    .await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .and(header("authorization", "Bearer secret-token"))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "requirements": [
                {"type": "assert-ref-snapshot-id", "ref": "served-parcel-by-pnu-1", "snapshot-id": null}
            ],
            "updates": [{
                "action": "set-snapshot-ref",
                "ref-name": "served-parcel-by-pnu-1",
                "type": "tag",
                "snapshot-id": TAGGED
            }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    catalog
        .create_tag("gold.parcel_panel", "served-parcel-by-pnu-1", TAGGED)
        .await?;
    let refs = catalog
        .load_snapshot_refs("gold.parcel_panel")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;
    assert_eq!(refs.snapshot_ids, vec![TAGGED, OTHER]);
    assert_eq!(refs.refs["main"].kind, "branch");
    Ok(())
}

#[tokio::test]
async fn a_tag_is_never_moved_nor_set_on_an_expired_snapshot() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    mount_gold(
        &server,
        serde_json::json!({
            "main": {"snapshot-id": OTHER, "type": "branch"},
            "served-parcel-by-pnu-1": {"snapshot-id": TAGGED, "type": "tag"}
        }),
    )
    .await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    // The same tag on the same snapshot is a re-run, not a second commit.
    catalog
        .create_tag("gold.parcel_panel", "served-parcel-by-pnu-1", TAGGED)
        .await?;
    for (name, snapshot, refusal) in [
        ("served-parcel-by-pnu-1", OTHER, "never moved"),
        ("main", TAGGED, "never moved"),
        (
            "served-parcel-by-pnu-2",
            999_990_000_000_000_009,
            "no longer holds",
        ),
    ] {
        let error = catalog
            .create_tag("gold.parcel_panel", name, snapshot)
            .await
            .err()
            .ok_or_else(|| std::io::Error::other(format!("{name} on {snapshot} was tagged")))?;
        assert!(error.to_string().contains(refusal), "{error}");
    }
    Ok(())
}

#[tokio::test]
async fn a_tag_is_removed_only_while_it_names_the_expected_snapshot() -> Result<(), Box<dyn Error>>
{
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    mount_gold(
        &server,
        serde_json::json!({
            "main": {"snapshot-id": OTHER, "type": "branch"},
            "served-parcel-by-pnu-1": {"snapshot-id": TAGGED, "type": "tag"}
        }),
    )
    .await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .and(wiremock::matchers::body_json(serde_json::json!({
            "requirements": [
                {"type": "assert-ref-snapshot-id", "ref": "served-parcel-by-pnu-1", "snapshot-id": TAGGED}
            ],
            "updates": [{"action": "remove-snapshot-ref", "ref-name": "served-parcel-by-pnu-1"}]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    catalog
        .remove_tag("gold.parcel_panel", "served-parcel-by-pnu-1", TAGGED)
        .await?;
    // Absent is already removed; a branch and another snapshot's tag are never removed.
    catalog
        .remove_tag("gold.parcel_panel", "served-parcel-by-pnu-9", TAGGED)
        .await?;
    for (name, snapshot) in [("main", OTHER), ("served-parcel-by-pnu-1", OTHER)] {
        assert!(
            catalog
                .remove_tag("gold.parcel_panel", name, snapshot)
                .await
                .is_err(),
            "{name} on {snapshot} was removed"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_conflicting_tag_commit_is_an_error() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    mount_gold(
        &server,
        serde_json::json!({"main": {"snapshot-id": OTHER, "type": "branch"}}),
    )
    .await;
    Mock::given(method("POST"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(409))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let error = catalog
        .create_tag("gold.parcel_panel", "served-parcel-by-pnu-1", TAGGED)
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("a refused commit was reported as a tag"))?;
    assert!(error.to_string().contains("conflicted"), "{error}");
    Ok(())
}

/// A token that may only read the catalog cannot pin: the refusal names the missing permission
/// instead of a bare status (root ADR-0146 §2).
#[tokio::test]
async fn a_read_only_token_is_refused_naming_catalog_write() -> Result<(), Box<dyn Error>> {
    for status in [401, 403] {
        let server = MockServer::start().await;
        mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
        mount_gold(
            &server,
            serde_json::json!({"main": {"snapshot-id": OTHER, "type": "branch"}}),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(GOLD_PATH))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;

        let catalog = IcebergRestCatalog::new(config(&server))?;
        let error = catalog
            .create_tag("gold.parcel_panel", "served-parcel-by-pnu-1", TAGGED)
            .await
            .err()
            .ok_or_else(|| std::io::Error::other("a refused commit was reported as a tag"))?;
        assert!(
            error.to_string().contains("catalog write permission"),
            "{status}: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_resumed_load_reads_what_its_branch_records_or_mains_before_it_exists(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(GOLD_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://lakehouse/gold/parcel_panel/metadata/00005.json",
            "metadata": {
                "current-snapshot-id": OTHER,
                "refs": {
                    "main": {"snapshot-id": OTHER, "type": "branch"},
                    "load_fixture": {"snapshot-id": STAGED, "type": "branch"}
                },
                "snapshots": [
                    {"snapshot-id": OTHER, "summary": {
                        "operation": "append", "foundation.ingest-batch-objects": "part-0001"}},
                    {"snapshot-id": STAGED, "parent-snapshot-id": OTHER, "summary": {
                        "operation": "append", "foundation.ingest-batch-objects": "part-0002"}}
                ]
            }
        })))
        .mount(&server)
        .await;
    let catalog = IcebergRestCatalog::new(config(&server))?;
    let staged = catalog
        .load_staged_batch_objects("gold.parcel_panel", "load_fixture")
        .await?;
    assert_eq!(
        staged.into_iter().collect::<Vec<_>>(),
        ["part-0001", "part-0002"]
    );
    let not_made = catalog
        .load_staged_batch_objects("gold.parcel_panel", "load_other")
        .await?;
    assert_eq!(not_made.into_iter().collect::<Vec<_>>(), ["part-0001"]);
    Ok(())
}

#[tokio::test]
async fn reads_mains_snapshot_history_newest_first_without_other_branches(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "prefix").await;
    Mock::given(method("GET"))
        .and(path("/v1/prefix/namespaces/gold/tables/synthetic_panel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://bucket/gold/synthetic_panel/metadata/00004.json",
            "metadata": {
                "current-snapshot-id": 3,
                "refs": {
                    "main": {"snapshot-id": 3, "type": "branch"},
                    "staged": {"snapshot-id": 9, "type": "branch"}
                },
                "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 1000, "summary": {"operation": "append", "added-records": "10",
                     "foundation.ingest-batch-objects": "bronze/a.zip,bronze/b.zip"}},
                    {"snapshot-id": 3, "parent-snapshot-id": 1, "timestamp-ms": 3000,
                     "summary": {"operation": "overwrite", "added-records": "12",
                                 "foundation.source-iceberg-snapshots": "{\"silver.a\":\"7\"}"}},
                    {"snapshot-id": 9, "parent-snapshot-id": 3, "timestamp-ms": 9000, "summary": {"operation": "append"}}
                ]
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let history = catalog
        .load_main_snapshot_history("gold.synthetic_panel")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;

    let ids: Vec<i64> = history
        .iter()
        .map(|snapshot| snapshot.snapshot_id)
        .collect();
    assert_eq!(
        ids,
        vec![3, 1],
        "main's lineage, newest first, not the staged branch"
    );
    assert_eq!(history[0].timestamp_ms, Some(3000));
    assert_eq!(
        history[0].summary.get("added-records").map(String::as_str),
        Some("12")
    );
    assert_eq!(
        history[0]
            .summary
            .get("foundation.source-iceberg-snapshots")
            .map(String::as_str),
        Some("{\"silver.a\":\"7\"}")
    );
    assert_eq!(history[0].added_records(), Some(12));
    assert_eq!(
        history[0].source_snapshots(),
        std::collections::BTreeMap::from([("silver.a".to_owned(), 7)])
    );
    assert!(history[1].source_snapshots().is_empty());
    assert_eq!(
        history[1].ingested_objects(),
        vec!["bronze/a.zip", "bronze/b.zip"]
    );
    assert!(history[0].ingested_objects().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_table_never_written_has_an_empty_history() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "prefix").await;
    // The R2 catalog leaves out `current-snapshot-id` and `snapshots` until the first commit.
    Mock::given(method("GET"))
        .and(path("/v1/prefix/namespaces/silver/tables/synthetic_empty"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://bucket/silver/synthetic_empty/metadata/00000.json",
            "metadata": {"format-version": 2, "last-sequence-number": 0, "refs": {}}
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let history = catalog
        .load_main_snapshot_history("silver.synthetic_empty")
        .await?;
    assert_eq!(history, Some(Vec::new()));
    Ok(())
}
