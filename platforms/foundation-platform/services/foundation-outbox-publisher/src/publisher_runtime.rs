//! The publisher loop, the R2 smoke and inventory, the parcel marker anchor rebuild, and the
//! broadcaster and object-storage settings they read. Moved out of `main.rs` unchanged.

use super::{
    bail, env, fs, optional_env_value, r2_smoke_object_key, required_env_value,
    runtime_environment, tile_derivative_object_storage, watch, Arc, CatalogEventBroadcaster,
    Context, EventBroadcaster, LoggingBroadcaster, LoggingObjectStorage, ObjectStorageService,
    ObjectStorageSmokeReport, OutboxScope, OutboxWorker, Path, PgParcelMarkerAnchorRebuilder,
    PgPool, PgVectorTileManifestReader, PublisherConfig, R2InventoryRequest, R2ObjectStorage,
    RebuildParcelMarkerAnchors, RebuildParcelMarkerAnchorsInput, WebhookBroadcaster,
};

pub(super) async fn run_publisher() -> anyhow::Result<()> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")?;
    let fallback_broadcaster = fallback_broadcaster()?;
    let catalog_broadcaster = catalog_broadcaster(pool.clone(), fallback_broadcaster).await?;
    let config = PublisherConfig::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let catalog_worker = OutboxWorker::new(pool, catalog_broadcaster, config, OutboxScope::Catalog);
    let catalog_task = tokio::spawn(async move { catalog_worker.run(shutdown_rx).await });

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")?;
    shutdown_tx
        .send(true)
        .context("failed to send shutdown signal")?;

    catalog_task.await.context("catalog worker task failed")??;

    Ok(())
}

pub(super) async fn run_r2_smoke() -> anyhow::Result<()> {
    let runtime_environment =
        runtime_environment::RuntimeEnvironment::from_env_with_execution_context()?;
    validate_runtime_r2_bucket(runtime_environment)?;
    let key = r2_smoke_object_key()?;
    let storage =
        R2ObjectStorage::from_env().context("failed to configure R2 object storage for smoke")?;
    let body = format!("foundation-platform r2 smoke\nkey={key}\n").into_bytes();
    let report = storage
        .round_trip_smoke(key, body)
        .await
        .context("R2 smoke round trip failed")?;

    tracing::info!(
        object_key = %report.key,
        bytes_verified = report.bytes_verified,
        put_request_count = report.put_request_count,
        get_request_count = report.get_request_count,
        delete_request_count = report.delete_request_count,
        "R2 smoke round trip succeeded"
    );

    if let Some(metrics_path) = optional_env_value("FOUNDATION_PLATFORM_R2_SMOKE_METRICS_PATH")? {
        write_r2_smoke_metrics(Path::new(&metrics_path), &report)?;
    }

    Ok(())
}

pub(super) async fn run_publisher_once() -> anyhow::Result<()> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")?;
    let fallback_broadcaster = fallback_broadcaster()?;
    let catalog_broadcaster = catalog_broadcaster(pool.clone(), fallback_broadcaster).await?;
    let config = PublisherConfig::default();
    let catalog_stats = OutboxWorker::new(pool, catalog_broadcaster, config, OutboxScope::Catalog)
        .tick()
        .await
        .context("catalog outbox publish-once tick failed")?;
    tracing::info!(
        catalog_published = catalog_stats.published,
        catalog_retried = catalog_stats.retried,
        catalog_dead_lettered = catalog_stats.dead_lettered,
        "outbox publish-once succeeded"
    );
    Ok(())
}

pub(super) async fn run_parcel_marker_anchor_rebuild() -> anyhow::Result<()> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let source_snapshot_id =
        env::var("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_SOURCE_SNAPSHOT_ID")
            .context("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_SOURCE_SNAPSHOT_ID is required")?;
    let algorithm_version = env::var("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ALGORITHM_VERSION")
        .unwrap_or_else(|_| "postgis-st_maximuminscribedcircle-v1".to_owned());
    let request_id = optional_env_value("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_REQUEST_ID")?;

    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")?;
    let use_case =
        RebuildParcelMarkerAnchors::new(Arc::new(PgParcelMarkerAnchorRebuilder::new(pool)));
    let report = use_case
        .execute(RebuildParcelMarkerAnchorsInput {
            source_snapshot_id,
            algorithm_version,
            requested_by_staff_id: None,
            request_id,
        })
        .await
        .context("parcel marker anchor rebuild failed")?;

    tracing::info!(
        generation_run_id = %report.generation_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        source_table = %report.source_table,
        algorithm = report.algorithm.wire_name(),
        algorithm_version = %report.algorithm_version,
        scanned_row_count = report.scanned_row_count,
        loaded_row_count = report.loaded_row_count,
        rejected_row_count = report.rejected_row_count,
        superseded_row_count = report.superseded_row_count,
        "parcel marker anchor rebuild succeeded"
    );

    if let Some(summary_path) =
        optional_env_value("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_REBUILD_SUMMARY_PATH")?
    {
        write_parcel_marker_anchor_rebuild_summary(Path::new(&summary_path), &report)?;
    }

    Ok(())
}

pub(super) fn write_parcel_marker_anchor_rebuild_summary(
    path: &Path,
    report: &catalog_application::ports::ParcelMarkerAnchorRebuildReport,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("parcel marker anchor rebuild summary path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create parcel marker anchor rebuild summary directory {}",
            parent.display()
        )
    })?;
    let summary = serde_json::json!({
        "schema_version": "foundation-platform.parcel_marker_anchor_rebuild_summary.v1",
        "generated_at_utc": chrono::Utc::now().to_rfc3339(),
        "generation_run_id": report.generation_run_id,
        "source_snapshot_id": report.source_snapshot_id,
        "source_table": report.source_table,
        "algorithm": report.algorithm.wire_name(),
        "algorithm_version": report.algorithm_version,
        "scanned_row_count": report.scanned_row_count,
        "loaded_row_count": report.loaded_row_count,
        "rejected_row_count": report.rejected_row_count,
        "superseded_row_count": report.superseded_row_count,
    });
    let payload = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize parcel marker anchor rebuild summary")?;
    fs::write(path, payload).with_context(|| {
        format!(
            "failed to write parcel marker anchor rebuild summary {}",
            path.display()
        )
    })
}

pub(super) fn write_r2_smoke_metrics(
    path: &Path,
    report: &ObjectStorageSmokeReport,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("R2 smoke metrics path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create R2 smoke metrics directory {}",
            parent.display()
        )
    })?;
    fs::write(path, report.to_prometheus_metrics("live_r2_smoke"))
        .with_context(|| format!("failed to write R2 smoke metrics file {}", path.display()))
}

pub(super) async fn run_r2_inventory() -> anyhow::Result<()> {
    let runtime_environment =
        runtime_environment::RuntimeEnvironment::from_env_with_execution_context()?;
    validate_runtime_r2_bucket(runtime_environment)?;
    let request = r2_inventory_request()?;
    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 object storage for inventory")?;
    let report = storage
        .inventory(request)
        .await
        .context("R2 inventory failed")?;

    tracing::info!(
        prefix = report.prefix().unwrap_or("<root>"),
        max_keys = report.max_keys(),
        key_count = report.key_count(),
        is_truncated = report.is_truncated(),
        common_prefixes = ?report.common_prefixes(),
        objects = ?report.objects(),
        "R2 inventory succeeded"
    );

    Ok(())
}

pub(super) fn r2_inventory_request() -> anyhow::Result<R2InventoryRequest> {
    let prefix = optional_env_value("FOUNDATION_PLATFORM_R2_INVENTORY_PREFIX")?;
    let max_keys = optional_env_value("FOUNDATION_PLATFORM_R2_INVENTORY_MAX_KEYS")?
        .map(|raw| raw.parse::<i32>())
        .transpose()
        .context("FOUNDATION_PLATFORM_R2_INVENTORY_MAX_KEYS must be an integer")?;

    R2InventoryRequest::new(prefix.as_deref(), max_keys).context("invalid R2 inventory request")
}

pub(super) async fn catalog_broadcaster(
    pool: PgPool,
    fallback: Arc<dyn EventBroadcaster>,
) -> anyhow::Result<Arc<dyn EventBroadcaster>> {
    let runtime_environment =
        runtime_environment::RuntimeEnvironment::from_env_with_execution_context()?;
    let object_storage_driver = object_storage_driver()?;
    runtime_environment::validate_catalog_driver(runtime_environment, &object_storage_driver)?;
    // The second store is the tile edge's bucket, where the v2 runtime manifest goes (root ADR-0254).
    let (object_storage, runtime_manifest_storage): (
        Arc<dyn ObjectStorageService>,
        Arc<dyn ObjectStorageService>,
    ) = match object_storage_driver.as_str() {
        "log" => (
            Arc::new(LoggingObjectStorage),
            Arc::new(LoggingObjectStorage),
        ),
        "r2" => {
            validate_runtime_r2_bucket(runtime_environment)?;
            let tiles = tile_derivative_object_storage::TileDerivativeR2Config::from_env()
                .context(
                    "the runtime manifest is written to the tile-derivative bucket (root ADR-0254)",
                )?;
            (
                Arc::new(
                    R2ObjectStorage::from_env().context("failed to configure R2 object storage")?,
                ),
                Arc::new(R2ObjectStorage::from_config(tiles.writer)),
            )
        }
        driver => {
            bail!(
                "FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER must be 'log' or 'r2', got '{driver}'"
            );
        }
    };

    let fanout = match foundation_outbox::kafka_broadcaster::from_env(fallback.clone()).await? {
        Some(kafka_broadcaster) => Arc::new(kafka_broadcaster) as Arc<dyn EventBroadcaster>,
        None => fallback,
    };

    let reader = Arc::new(PgVectorTileManifestReader::new(pool));
    Ok(Arc::new(
        CatalogEventBroadcaster::new(reader.clone(), object_storage, fanout)
            .with_runtime_manifest(reader, runtime_manifest_storage),
    ))
}

pub(super) fn fallback_broadcaster() -> anyhow::Result<Arc<dyn EventBroadcaster>> {
    let Some(raw_endpoint_specs) =
        optional_env_value("FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_ENDPOINTS")?
    else {
        return Ok(Arc::new(LoggingBroadcaster));
    };
    let signature_secret = required_env_value("FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET")?;

    let mut builder = WebhookBroadcaster::builder()
        .signature_secret(signature_secret.as_str())
        .context("invalid FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET")?;
    for (name, url) in parse_webhook_endpoint_specs(raw_endpoint_specs.as_str())? {
        builder = builder
            .endpoint(name.as_str(), url.as_str())
            .context("invalid FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_ENDPOINTS")?;
    }

    Ok(Arc::new(builder.build().context(
        "failed to configure outbox webhook broadcaster",
    )?))
}

pub(super) fn parse_webhook_endpoint_specs(raw: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut specs = Vec::new();
    for raw_part in raw.split(';') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }

        let (name, url) = part
            .split_once('=')
            .context("webhook endpoint must use name=url format")?;
        let name = name.trim();
        let url = url.trim();
        if name.is_empty() {
            bail!("webhook endpoint name must not be empty");
        }
        if url.is_empty() {
            bail!("webhook endpoint url must not be empty");
        }

        specs.push((name.to_owned(), url.to_owned()));
    }

    if specs.is_empty() {
        bail!("at least one webhook endpoint is required");
    }

    Ok(specs)
}

pub(super) fn object_storage_driver() -> anyhow::Result<String> {
    let driver =
        required_env_value("FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER")?.to_ascii_lowercase();
    Ok(driver)
}

pub(super) fn validate_runtime_r2_bucket(
    runtime_environment: runtime_environment::RuntimeEnvironment,
) -> anyhow::Result<()> {
    let bucket = required_env_value("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET")?;
    runtime_environment::validate_foundation_r2_bucket(runtime_environment, &bucket)
}
