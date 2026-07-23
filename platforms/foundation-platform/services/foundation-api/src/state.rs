//! Shared application state injected into HTTP routes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use catalog_application::{
    ArchiveIndustrialComplex, PromoteVectorTileManifest, RebuildParcelMarkerAnchors,
    RegisterIndustrialComplex, RollbackVectorTileManifest, UpdateIndustrialComplex,
    UpdateParcelKind,
};
use catalog_infrastructure::{
    PgCatalogRepository, PgCatalogUnitOfWork, PgParcelMarkerAnchorRebuilder,
};
use foundation_normalization_application::{
    ApplyNormalizationProposal, NormalizationUnitOfWork, ReviewNormalizationProposal,
    RollbackNormalizationApplication, SubmitNormalizationProposal,
};
use foundation_normalization_infrastructure::PgNormalizationUnitOfWork;
use lakehouse_application::ports::{
    IndustrialComplexGoldPointerReader, LakehouseRegistryUnitOfWork,
};
use lakehouse_application::{RecordLakehouseBatchRun, RegisterLakehouseObjectArtifact};
use lakehouse_infrastructure::{
    PgIndustrialComplexGoldPointerReader, PgLakehouseBatchRunAudit, PgLakehouseRegistryUnitOfWork,
};
use sqlx::postgres::PgPoolOptions;

use crate::identity_authorization::{HttpIdentityAuthorization, IdentityAuthorization};
use crate::identity_http_client::HttpIdentityClient;
use crate::identity_token_verifier::IdentityTokenVerifier;
use crate::traffic::TrafficConfig;

const IDENTITY_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_IDENTITY_AUTHORIZATION_TIMEOUT_MS: u64 = 2_000;
const IDENTITY_NETWORK_BUDGET_SLICES: u32 = 5;

fn identity_network_request_timeout(authorization_timeout: Duration) -> anyhow::Result<Duration> {
    let request_timeout = authorization_timeout / IDENTITY_NETWORK_BUDGET_SLICES;
    if request_timeout.is_zero() {
        return Err(anyhow::anyhow!(
            "Identity authorization timeout is too small to allocate bounded network attempts"
        ));
    }
    Ok(request_timeout)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppConfig {
    pub database_url: String,
    pub database: DatabaseBudgetConfig,
    pub identity_api_base_url: String,
    pub zitadel_issuer_url: String,
    pub zitadel_audience: String,
    pub identity_authorization_timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseBudgetConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_ms: u64,
    pub statement_timeout_ms: u64,
    pub idle_timeout_seconds: u64,
}

impl Default for DatabaseBudgetConfig {
    fn default() -> Self {
        Self {
            max_connections: 8,
            min_connections: 1,
            acquire_timeout_ms: 500,
            statement_timeout_ms: 2500,
            idle_timeout_seconds: 300,
        }
    }
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_vars(|key| std::env::var(key).ok())
    }

    fn from_vars(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let database = DatabaseBudgetConfig::from_vars(&lookup)?;
        Ok(Self {
            database_url: required_var(&lookup, "DATABASE_URL")?,
            database,
            identity_api_base_url: required_var(&lookup, "IDENTITY_API_BASE_URL")?,
            zitadel_issuer_url: required_var(&lookup, "ZITADEL_ISSUER_URL")?,
            zitadel_audience: required_var(&lookup, "FOUNDATION_PLATFORM_ZITADEL_AUDIENCE")?,
            identity_authorization_timeout_ms: optional_positive_u64_var(
                &lookup,
                "FOUNDATION_PLATFORM_IDENTITY_AUTHORIZATION_TIMEOUT_MS",
                DEFAULT_IDENTITY_AUTHORIZATION_TIMEOUT_MS,
            )?,
        })
    }

    pub fn validate_identity_authorization_budget(
        &self,
        traffic: TrafficConfig,
    ) -> anyhow::Result<()> {
        identity_network_request_timeout(Duration::from_millis(
            self.identity_authorization_timeout_ms,
        ))?;
        if self.identity_authorization_timeout_ms >= traffic.request_timeout_ms {
            return Err(anyhow::anyhow!(
                "FOUNDATION_PLATFORM_IDENTITY_AUTHORIZATION_TIMEOUT_MS must be strictly smaller than FOUNDATION_PLATFORM_HTTP_REQUEST_TIMEOUT_MS"
            ));
        }
        Ok(())
    }
}

impl DatabaseBudgetConfig {
    fn from_vars(lookup: &impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let config = Self {
            max_connections: optional_positive_u32_var(
                lookup,
                "FOUNDATION_PLATFORM_DB_MAX_CONNECTIONS",
                Self::default().max_connections,
            )?,
            min_connections: optional_positive_u32_var(
                lookup,
                "FOUNDATION_PLATFORM_DB_MIN_CONNECTIONS",
                Self::default().min_connections,
            )?,
            acquire_timeout_ms: optional_positive_u64_var(
                lookup,
                "FOUNDATION_PLATFORM_DB_ACQUIRE_TIMEOUT_MS",
                Self::default().acquire_timeout_ms,
            )?,
            statement_timeout_ms: optional_positive_u64_var(
                lookup,
                "FOUNDATION_PLATFORM_DB_STATEMENT_TIMEOUT_MS",
                Self::default().statement_timeout_ms,
            )?,
            idle_timeout_seconds: optional_positive_u64_var(
                lookup,
                "FOUNDATION_PLATFORM_DB_IDLE_TIMEOUT_SECONDS",
                Self::default().idle_timeout_seconds,
            )?,
        };

        if config.min_connections > config.max_connections {
            return Err(anyhow::anyhow!(
                "FOUNDATION_PLATFORM_DB_MIN_CONNECTIONS must be less than or equal to FOUNDATION_PLATFORM_DB_MAX_CONNECTIONS"
            ));
        }

        Ok(config)
    }
}

pub struct AppState {
    database_pool: sqlx::PgPool,
    database_max_connections: u32,
    http_metrics: ApiHttpMetrics,
    pub catalog_repo: Arc<PgCatalogRepository>,
    pub industrial_complex_gold_pointer_reader: Arc<dyn IndustrialComplexGoldPointerReader>,
    pub register_complex: RegisterIndustrialComplex,
    pub update_complex: UpdateIndustrialComplex,
    pub archive_complex: ArchiveIndustrialComplex,
    pub update_parcel_kind: UpdateParcelKind,
    pub promote_vector_tile_manifest: PromoteVectorTileManifest,
    pub rollback_vector_tile_manifest: RollbackVectorTileManifest,
    pub rebuild_parcel_marker_anchors: RebuildParcelMarkerAnchors,
    pub record_lakehouse_batch_run: RecordLakehouseBatchRun,
    pub register_lakehouse_object_artifact: RegisterLakehouseObjectArtifact,
    pub submit_normalization_proposal: SubmitNormalizationProposal,
    pub review_normalization_proposal: ReviewNormalizationProposal,
    pub apply_normalization_proposal: ApplyNormalizationProposal,
    pub rollback_normalization_application: RollbackNormalizationApplication,
    pub identity_authorization: Arc<dyn IdentityAuthorization>,
}

#[cfg(test)]
struct RejectingIdentityAuthorization;

#[cfg(test)]
#[async_trait::async_trait]
impl IdentityAuthorization for RejectingIdentityAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
        _required_principal_kind: crate::identity_authorization::RequiredPrincipalKind,
        _resource: &str,
        _action: &str,
        _resource_id: Option<&str>,
        _trace_id: &str,
    ) -> Result<
        crate::identity_authorization::AuthorizedPrincipal,
        crate::identity_authorization::IdentityAuthorizationError,
    > {
        Err(crate::identity_authorization::IdentityAuthorizationError::Unauthorized)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApiDatabasePoolMetric {
    pub pool_size: u32,
    pub idle_connections: usize,
    pub max_connections: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiHttpRequestMetric {
    pub method: String,
    pub route: String,
    pub status: u16,
    pub count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiHttpDurationMetric {
    pub method: String,
    pub route: String,
    pub status: u16,
    pub le: String,
    pub count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiOverloadRejectionMetric {
    pub reason: String,
    pub count: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ApiHttpRequestMetricKey {
    method: String,
    route: String,
    status: u16,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ApiHttpDurationMetricKey {
    method: String,
    route: String,
    status: u16,
    le: &'static str,
}

#[derive(Debug, Default)]
struct ApiHttpMetrics {
    http_requests: Mutex<BTreeMap<ApiHttpRequestMetricKey, u64>>,
    http_durations: Mutex<BTreeMap<ApiHttpDurationMetricKey, u64>>,
    overload_rejections: Mutex<BTreeMap<String, u64>>,
}

impl ApiHttpMetrics {
    fn record_http_request(&self, method: &str, route: &str, status: u16, duration_seconds: f64) {
        let Ok(mut counters) = self.http_requests.lock() else {
            return;
        };
        let key = ApiHttpRequestMetricKey {
            method: method.to_owned(),
            route: route.to_owned(),
            status,
        };
        let count = counters.entry(key).or_insert(0);
        *count = count.saturating_add(1);
        drop(counters);

        let safe_duration = if duration_seconds.is_finite() && duration_seconds >= 0.0 {
            duration_seconds
        } else {
            0.0
        };
        let Ok(mut durations) = self.http_durations.lock() else {
            return;
        };
        for (upper_bound, le) in HTTP_DURATION_BUCKETS {
            if upper_bound.is_infinite() || safe_duration <= upper_bound {
                let key = ApiHttpDurationMetricKey {
                    method: method.to_owned(),
                    route: route.to_owned(),
                    status,
                    le,
                };
                let count = durations.entry(key).or_insert(0);
                *count = count.saturating_add(1);
            }
        }
    }

    fn http_request_metrics(&self) -> Vec<ApiHttpRequestMetric> {
        let Ok(counters) = self.http_requests.lock() else {
            return Vec::new();
        };
        counters
            .iter()
            .map(|(key, count)| ApiHttpRequestMetric {
                method: key.method.clone(),
                route: key.route.clone(),
                status: key.status,
                count: *count,
            })
            .collect()
    }

    fn http_duration_metrics(&self) -> Vec<ApiHttpDurationMetric> {
        let Ok(counters) = self.http_durations.lock() else {
            return Vec::new();
        };
        counters
            .iter()
            .map(|(key, count)| ApiHttpDurationMetric {
                method: key.method.clone(),
                route: key.route.clone(),
                status: key.status,
                le: key.le.to_owned(),
                count: *count,
            })
            .collect()
    }

    fn record_overload_rejection(&self, reason: &str) {
        let Ok(mut counters) = self.overload_rejections.lock() else {
            return;
        };
        let count = counters.entry(reason.to_owned()).or_insert(0);
        *count = count.saturating_add(1);
    }

    fn overload_rejection_metrics(&self) -> Vec<ApiOverloadRejectionMetric> {
        let Ok(counters) = self.overload_rejections.lock() else {
            return Vec::new();
        };
        counters
            .iter()
            .map(|(reason, count)| ApiOverloadRejectionMetric {
                reason: reason.clone(),
                count: *count,
            })
            .collect()
    }
}

const HTTP_DURATION_BUCKETS: [(f64, &str); 9] = [
    (0.05, "0.05"),
    (0.1, "0.1"),
    (0.25, "0.25"),
    (0.5, "0.5"),
    (1.0, "1"),
    (2.5, "2.5"),
    (5.0, "5"),
    (10.0, "10"),
    (f64::INFINITY, "+Inf"),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseBatchRunMetric {
    pub contract: String,
    pub created_at_unix_seconds: i64,
    pub recorded_at_unix_seconds: i64,
    pub row_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestionRunMetric {
    pub source_slug: String,
    pub status: String,
    pub finished_at_unix_seconds: i64,
    pub duration_seconds: i64,
    pub logical_records_seen: i64,
    pub objects_written: i64,
    pub raw_response_size_bytes: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxQueueMetric {
    pub scope: String,
    pub pending_event_count: i64,
    pub retry_event_count: i64,
    pub oldest_pending_age_seconds: i64,
}

#[derive(sqlx::FromRow)]
struct LakehouseBatchRunMetricRow {
    contract: String,
    created_at_unix_seconds: i64,
    recorded_at_unix_seconds: i64,
    row_count: i64,
}

#[derive(sqlx::FromRow)]
struct IngestionRunMetricRow {
    source_slug: String,
    status: String,
    finished_at_unix_seconds: i64,
    duration_seconds: i64,
    logical_records_seen: i64,
    objects_written: i64,
    raw_response_size_bytes: i64,
}

#[derive(sqlx::FromRow)]
struct OutboxQueueMetricRow {
    scope: String,
    pending_event_count: i64,
    retry_event_count: i64,
    oldest_pending_age_seconds: i64,
}

impl From<LakehouseBatchRunMetricRow> for LakehouseBatchRunMetric {
    fn from(row: LakehouseBatchRunMetricRow) -> Self {
        Self {
            contract: row.contract,
            created_at_unix_seconds: row.created_at_unix_seconds,
            recorded_at_unix_seconds: row.recorded_at_unix_seconds,
            row_count: row.row_count,
        }
    }
}

impl From<IngestionRunMetricRow> for IngestionRunMetric {
    fn from(row: IngestionRunMetricRow) -> Self {
        Self {
            source_slug: row.source_slug,
            status: row.status,
            finished_at_unix_seconds: row.finished_at_unix_seconds,
            duration_seconds: row.duration_seconds,
            logical_records_seen: row.logical_records_seen,
            objects_written: row.objects_written,
            raw_response_size_bytes: row.raw_response_size_bytes,
        }
    }
}

impl From<OutboxQueueMetricRow> for OutboxQueueMetric {
    fn from(row: OutboxQueueMetricRow) -> Self {
        Self {
            scope: row.scope,
            pending_event_count: row.pending_event_count,
            retry_event_count: row.retry_event_count,
            oldest_pending_age_seconds: row.oldest_pending_age_seconds,
        }
    }
}

impl AppState {
    pub async fn bootstrap(traffic: TrafficConfig) -> anyhow::Result<Self> {
        let config = AppConfig::from_env()?;
        config.validate_identity_authorization_budget(traffic)?;
        Self::bootstrap_with_config(config).await
    }

    #[cfg(test)]
    pub fn bootstrap_for_test() -> anyhow::Result<Self> {
        let pool = sqlx::PgPool::connect_lazy(&test_database_url())?;
        Ok(Self::from_pool(
            pool,
            DatabaseBudgetConfig::default().max_connections,
            Arc::new(RejectingIdentityAuthorization),
        ))
    }

    #[cfg(test)]
    pub fn bootstrap_for_test_with_identity_authorization(
        identity_authorization: Arc<dyn IdentityAuthorization>,
    ) -> anyhow::Result<Self> {
        let pool = sqlx::PgPool::connect_lazy(&test_database_url())?;
        Ok(Self::from_pool(
            pool,
            DatabaseBudgetConfig::default().max_connections,
            identity_authorization,
        ))
    }

    #[cfg(test)]
    pub fn bootstrap_for_test_with_normalization_uow_and_identity_authorization(
        normalization_uow: Arc<dyn NormalizationUnitOfWork>,
        identity_authorization: Arc<dyn IdentityAuthorization>,
    ) -> anyhow::Result<Self> {
        let pool = sqlx::PgPool::connect_lazy(&test_database_url())?;
        Ok(Self::from_pool_with_normalization_uow(
            pool,
            DatabaseBudgetConfig::default().max_connections,
            normalization_uow,
            identity_authorization,
        ))
    }

    #[cfg(test)]
    pub fn bootstrap_for_test_with_lakehouse_uow_and_identity_authorization(
        lakehouse_uow: Arc<dyn LakehouseRegistryUnitOfWork>,
        identity_authorization: Arc<dyn IdentityAuthorization>,
    ) -> anyhow::Result<Self> {
        let pool = sqlx::PgPool::connect_lazy(&test_database_url())?;
        let normalization_uow = Arc::new(PgNormalizationUnitOfWork::new(pool.clone()));
        Ok(Self::from_pool_with_units_of_work(
            pool,
            DatabaseBudgetConfig::default().max_connections,
            normalization_uow,
            lakehouse_uow,
            identity_authorization,
        ))
    }

    async fn bootstrap_with_config(config: AppConfig) -> anyhow::Result<Self> {
        let statement_timeout = format!("{}ms", config.database.statement_timeout_ms);
        let pool = PgPoolOptions::new()
            .max_connections(config.database.max_connections)
            .min_connections(config.database.min_connections)
            .acquire_timeout(Duration::from_millis(config.database.acquire_timeout_ms))
            .idle_timeout(Duration::from_secs(config.database.idle_timeout_seconds))
            .after_connect(move |connection, _meta| {
                let statement_timeout = statement_timeout.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                        .bind(&statement_timeout)
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&config.database_url)
            .await?;

        let authorization_timeout = Duration::from_millis(config.identity_authorization_timeout_ms);
        let network_request_timeout = identity_network_request_timeout(authorization_timeout)?;
        let connect_timeout = IDENTITY_CONNECT_TIMEOUT.min(network_request_timeout);
        let verifier = IdentityTokenVerifier::new(
            config.zitadel_issuer_url,
            config.zitadel_audience,
            connect_timeout,
            network_request_timeout,
        )?;
        let identity_client = HttpIdentityClient::new(
            config.identity_api_base_url,
            connect_timeout,
            network_request_timeout,
        )?;
        let identity_authorization = Arc::new(HttpIdentityAuthorization::new(
            verifier,
            identity_client,
            authorization_timeout,
        ));

        let state = Self::from_pool(
            pool,
            config.database.max_connections,
            identity_authorization,
        );
        Ok(state)
    }

    fn from_pool(
        pool: sqlx::PgPool,
        database_max_connections: u32,
        identity_authorization: Arc<dyn IdentityAuthorization>,
    ) -> Self {
        let normalization_uow = Arc::new(PgNormalizationUnitOfWork::new(pool.clone()));
        Self::from_pool_with_normalization_uow(
            pool,
            database_max_connections,
            normalization_uow,
            identity_authorization,
        )
    }

    fn from_pool_with_normalization_uow(
        pool: sqlx::PgPool,
        database_max_connections: u32,
        normalization_uow: Arc<dyn NormalizationUnitOfWork>,
        identity_authorization: Arc<dyn IdentityAuthorization>,
    ) -> Self {
        let lakehouse_uow = Arc::new(PgLakehouseRegistryUnitOfWork::new(pool.clone()));
        Self::from_pool_with_units_of_work(
            pool,
            database_max_connections,
            normalization_uow,
            lakehouse_uow,
            identity_authorization,
        )
    }

    fn from_pool_with_units_of_work(
        pool: sqlx::PgPool,
        database_max_connections: u32,
        normalization_uow: Arc<dyn NormalizationUnitOfWork>,
        lakehouse_uow: Arc<dyn LakehouseRegistryUnitOfWork>,
        identity_authorization: Arc<dyn IdentityAuthorization>,
    ) -> Self {
        // Catalog mutations and matching outbox writes share one transaction boundary.
        let catalog_repo = Arc::new(PgCatalogRepository::new(pool.clone()));
        let industrial_complex_gold_pointer_reader =
            Arc::new(PgIndustrialComplexGoldPointerReader::new(pool.clone()));
        let catalog_uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
        let register_complex = RegisterIndustrialComplex::new(catalog_uow.clone());
        let update_complex = UpdateIndustrialComplex::new(catalog_uow.clone());
        let archive_complex = ArchiveIndustrialComplex::new(catalog_uow.clone());
        let update_parcel_kind = UpdateParcelKind::new(catalog_uow.clone());
        let promote_vector_tile_manifest = PromoteVectorTileManifest::new(catalog_uow.clone());
        let rollback_vector_tile_manifest = RollbackVectorTileManifest::new(catalog_uow);
        let parcel_marker_anchor_rebuilder =
            Arc::new(PgParcelMarkerAnchorRebuilder::new(pool.clone()));
        let rebuild_parcel_marker_anchors =
            RebuildParcelMarkerAnchors::new(parcel_marker_anchor_rebuilder);
        let lakehouse_batch_audit = Arc::new(PgLakehouseBatchRunAudit::new(pool.clone()));
        let record_lakehouse_batch_run = RecordLakehouseBatchRun::new(lakehouse_batch_audit);
        let register_lakehouse_object_artifact =
            RegisterLakehouseObjectArtifact::new(lakehouse_uow);
        let submit_normalization_proposal =
            SubmitNormalizationProposal::new(normalization_uow.clone());
        let review_normalization_proposal =
            ReviewNormalizationProposal::new(normalization_uow.clone());
        let apply_normalization_proposal =
            ApplyNormalizationProposal::new(normalization_uow.clone());
        let rollback_normalization_application =
            RollbackNormalizationApplication::new(normalization_uow);

        Self {
            database_pool: pool,
            database_max_connections,
            http_metrics: ApiHttpMetrics::default(),
            catalog_repo,
            industrial_complex_gold_pointer_reader,
            register_complex,
            update_complex,
            archive_complex,
            update_parcel_kind,
            promote_vector_tile_manifest,
            rollback_vector_tile_manifest,
            rebuild_parcel_marker_anchors,
            record_lakehouse_batch_run,
            register_lakehouse_object_artifact,
            submit_normalization_proposal,
            review_normalization_proposal,
            apply_normalization_proposal,
            rollback_normalization_application,
            identity_authorization,
        }
    }

    pub async fn database_ready(&self) -> bool {
        tokio::time::timeout(
            Duration::from_millis(500),
            sqlx::query_scalar::<_, i32>("SELECT 1::int4").fetch_one(&self.database_pool),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }

    pub fn database_pool_metric(&self) -> ApiDatabasePoolMetric {
        ApiDatabasePoolMetric {
            pool_size: self.database_pool.size(),
            idle_connections: self.database_pool.num_idle(),
            max_connections: self.database_max_connections,
        }
    }

    pub fn record_http_request(
        &self,
        method: &str,
        route: &str,
        status: u16,
        duration_seconds: f64,
    ) {
        self.http_metrics
            .record_http_request(method, route, status, duration_seconds);
    }

    pub fn http_request_metrics(&self) -> Vec<ApiHttpRequestMetric> {
        self.http_metrics.http_request_metrics()
    }

    pub fn http_duration_metrics(&self) -> Vec<ApiHttpDurationMetric> {
        self.http_metrics.http_duration_metrics()
    }

    pub fn record_overload_rejection(&self, reason: &str) {
        self.http_metrics.record_overload_rejection(reason);
    }

    pub fn overload_rejection_metrics(&self) -> Vec<ApiOverloadRejectionMetric> {
        self.http_metrics.overload_rejection_metrics()
    }

    pub async fn latest_lakehouse_batch_run_metrics(&self) -> Vec<LakehouseBatchRunMetric> {
        let query = sqlx::query_as::<_, LakehouseBatchRunMetricRow>(
            "SELECT DISTINCT ON (contract)
                    contract,
                    EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at_unix_seconds,
                    EXTRACT(EPOCH FROM recorded_at)::BIGINT AS recorded_at_unix_seconds,
                    row_count
             FROM catalog.lakehouse_batch_run
             WHERE source_snapshot_truncated = false
               AND persisted_row_count = row_count
               AND write_disposition <> 'validate_only'
             ORDER BY contract ASC, created_at DESC, recorded_at DESC, id DESC",
        )
        .fetch_all(&self.database_pool);

        match tokio::time::timeout(Duration::from_millis(500), query).await {
            Ok(Ok(rows)) => rows
                .into_iter()
                .map(LakehouseBatchRunMetric::from)
                .collect(),
            Ok(Err(_)) | Err(_) => Vec::new(),
        }
    }

    pub async fn latest_ingestion_run_metrics(&self) -> Vec<IngestionRunMetric> {
        let query = sqlx::query_as::<_, IngestionRunMetricRow>(
            "SELECT DISTINCT ON (source.slug)
                    source.slug AS source_slug,
                    run.status,
                    EXTRACT(EPOCH FROM run.finished_at)::BIGINT AS finished_at_unix_seconds,
                    EXTRACT(EPOCH FROM (run.finished_at - run.started_at))::BIGINT AS duration_seconds,
                    run.logical_records_seen,
                    run.objects_written,
                    COALESCE(object_stats.raw_response_size_bytes, 0)::BIGINT AS raw_response_size_bytes
             FROM catalog.ingestion_run run
             JOIN catalog.source_catalog source
               ON source.id = run.source_catalog_id
             LEFT JOIN (
                 SELECT ingestion_run_id,
                        COALESCE(SUM(payload_size_bytes), 0)::BIGINT AS raw_response_size_bytes
                 FROM catalog.bronze_object
                 GROUP BY ingestion_run_id
             ) object_stats
               ON object_stats.ingestion_run_id = run.id
             WHERE run.finished_at IS NOT NULL
             ORDER BY source.slug ASC, run.finished_at DESC, run.started_at DESC, run.id DESC",
        )
        .fetch_all(&self.database_pool);

        match tokio::time::timeout(Duration::from_millis(500), query).await {
            Ok(Ok(rows)) => rows.into_iter().map(IngestionRunMetric::from).collect(),
            Ok(Err(_)) | Err(_) => Vec::new(),
        }
    }

    pub async fn outbox_queue_metrics(&self) -> Vec<OutboxQueueMetric> {
        let query = sqlx::query_as::<_, OutboxQueueMetricRow>(
            "SELECT scope,
                    COUNT(*) FILTER (WHERE published_at IS NULL)::BIGINT AS pending_event_count,
                    COUNT(*) FILTER (
                        WHERE published_at IS NULL
                          AND retry_count > 0
                    )::BIGINT AS retry_event_count,
                    COALESCE(
                        EXTRACT(EPOCH FROM (
                            now() - MIN(occurred_at) FILTER (WHERE published_at IS NULL)
                        ))::BIGINT,
                        0
                    ) AS oldest_pending_age_seconds
             FROM (
                 SELECT 'catalog' AS scope, published_at, retry_count, occurred_at
                 FROM catalog.outbox_event
             ) outbox
             GROUP BY scope
             ORDER BY scope ASC",
        )
        .fetch_all(&self.database_pool);

        match tokio::time::timeout(Duration::from_millis(500), query).await {
            Ok(Ok(rows)) => rows.into_iter().map(OutboxQueueMetric::from).collect(),
            Ok(Err(_)) | Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
fn test_database_url() -> String {
    std::env::var("DATABASE_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "postgres://localhost/foundation_platform_dev".to_owned())
}

fn required_var(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &'static str,
) -> anyhow::Result<String> {
    let raw = lookup(key).ok_or_else(|| anyhow::anyhow!("{key} must be set"))?;
    let value = raw.trim();
    if value.is_empty() {
        Err(anyhow::anyhow!("{key} must not be empty"))
    } else {
        Ok(value.to_owned())
    }
}

fn optional_positive_u32_var(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &'static str,
    default: u32,
) -> anyhow::Result<u32> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Err(anyhow::anyhow!("{key} must not be empty"));
    }
    let parsed = value
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("{key} must be a positive integer"))?;
    if parsed == 0 {
        return Err(anyhow::anyhow!("{key} must be positive"));
    }
    Ok(parsed)
}

fn optional_positive_u64_var(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &'static str,
    default: u64,
) -> anyhow::Result<u64> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Err(anyhow::anyhow!("{key} must not be empty"));
    }
    let parsed = value
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("{key} must be a positive integer"))?;
    if parsed == 0 {
        return Err(anyhow::anyhow!("{key} must be positive"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::{identity_network_request_timeout, AppConfig};
    use crate::traffic::TrafficConfig;

    fn vars(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn identity_network_calls_leave_budget_for_two_policy_attempts() -> anyhow::Result<()> {
        let authorization_timeout = Duration::from_secs(2);
        let request_timeout = identity_network_request_timeout(authorization_timeout)?;

        assert_eq!(request_timeout, Duration::from_millis(400));
        assert!(request_timeout * 4 < authorization_timeout);
        Ok(())
    }

    #[test]
    fn app_config_requires_database_url() -> anyhow::Result<()> {
        let vars = vars(&[("ZITADEL_ISSUER_URL", "https://issuer.example.test")]);

        let Err(error) = AppConfig::from_vars(|key| vars.get(key).cloned()) else {
            return Err(anyhow::anyhow!("DATABASE_URL should be required"));
        };

        assert!(error.to_string().contains("DATABASE_URL"));
        Ok(())
    }

    #[test]
    fn app_config_requires_zitadel_issuer_url() -> anyhow::Result<()> {
        let vars = vars(&[
            (
                "DATABASE_URL",
                "postgres://foundation_platform:secret@localhost:15434/foundation_platform",
            ),
            ("IDENTITY_API_BASE_URL", "https://identity.example.test"),
        ]);

        let Err(error) = AppConfig::from_vars(|key| vars.get(key).cloned()) else {
            return Err(anyhow::anyhow!("ZITADEL_ISSUER_URL should be required"));
        };

        assert!(error.to_string().contains("ZITADEL_ISSUER_URL"));
        Ok(())
    }

    #[test]
    fn app_config_requires_zitadel_audience() -> anyhow::Result<()> {
        let vars = vars(&[
            (
                "DATABASE_URL",
                "postgres://foundation_platform:secret@localhost:15434/foundation_platform",
            ),
            ("IDENTITY_API_BASE_URL", "https://identity.example.test"),
            ("ZITADEL_ISSUER_URL", "https://issuer.example.test"),
        ]);

        let Err(error) = AppConfig::from_vars(|key| vars.get(key).cloned()) else {
            return Err(anyhow::anyhow!(
                "FOUNDATION_PLATFORM_ZITADEL_AUDIENCE should be required"
            ));
        };

        assert!(error
            .to_string()
            .contains("FOUNDATION_PLATFORM_ZITADEL_AUDIENCE"));
        Ok(())
    }

    #[test]
    fn app_config_trims_required_values() -> anyhow::Result<()> {
        let vars = vars(&[
            (
                "DATABASE_URL",
                " postgres://foundation_platform:secret@localhost:15434/foundation_platform ",
            ),
            ("IDENTITY_API_BASE_URL", " https://identity.example.test "),
            ("ZITADEL_ISSUER_URL", " https://issuer.example.test "),
            ("FOUNDATION_PLATFORM_ZITADEL_AUDIENCE", " foundation-api "),
        ]);

        let config = AppConfig::from_vars(|key| vars.get(key).cloned())?;

        assert_eq!(
            config.database_url,
            "postgres://foundation_platform:secret@localhost:15434/foundation_platform"
        );
        assert_eq!(config.zitadel_issuer_url, "https://issuer.example.test");
        assert_eq!(
            config.identity_api_base_url,
            "https://identity.example.test"
        );
        assert_eq!(config.zitadel_audience, "foundation-api");
        Ok(())
    }

    #[test]
    fn app_config_parses_database_budget_defaults() -> anyhow::Result<()> {
        let vars = vars(&[
            (
                "DATABASE_URL",
                "postgres://foundation_platform:secret@localhost:15434/foundation_platform",
            ),
            ("IDENTITY_API_BASE_URL", "https://identity.example.test"),
            ("ZITADEL_ISSUER_URL", "https://issuer.example.test"),
            ("FOUNDATION_PLATFORM_ZITADEL_AUDIENCE", "foundation-api"),
        ]);

        let config = AppConfig::from_vars(|key| vars.get(key).cloned())?;

        assert_eq!(config.database.max_connections, 8);
        assert_eq!(config.database.min_connections, 1);
        assert_eq!(config.database.acquire_timeout_ms, 500);
        assert_eq!(config.database.statement_timeout_ms, 2500);
        assert_eq!(config.database.idle_timeout_seconds, 300);
        Ok(())
    }

    #[test]
    fn identity_dependency_budget_must_be_strictly_smaller_than_http_budget() -> anyhow::Result<()>
    {
        let vars = vars(&[
            (
                "DATABASE_URL",
                "postgres://foundation_api:secret@localhost:15434/foundation",
            ),
            ("IDENTITY_API_BASE_URL", "https://identity.example.test"),
            ("ZITADEL_ISSUER_URL", "https://issuer.example.test"),
            ("FOUNDATION_PLATFORM_ZITADEL_AUDIENCE", "foundation-api"),
            (
                "FOUNDATION_PLATFORM_IDENTITY_AUTHORIZATION_TIMEOUT_MS",
                "1000",
            ),
        ]);
        let config = AppConfig::from_vars(|key| vars.get(key).cloned())?;

        assert!(config
            .validate_identity_authorization_budget(TrafficConfig {
                request_timeout_ms: 1001,
                ..TrafficConfig::default()
            })
            .is_ok());
        let error = match config.validate_identity_authorization_budget(TrafficConfig {
            request_timeout_ms: 1000,
            ..TrafficConfig::default()
        }) {
            Ok(()) => anyhow::bail!("equal Identity and HTTP budgets must fail validation"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("FOUNDATION_PLATFORM_IDENTITY_AUTHORIZATION_TIMEOUT_MS"));
        Ok(())
    }

    #[test]
    fn app_config_parses_database_budget_overrides() -> anyhow::Result<()> {
        let vars = vars(&[
            (
                "DATABASE_URL",
                "postgres://foundation_platform:secret@localhost:15434/foundation_platform",
            ),
            ("IDENTITY_API_BASE_URL", "https://identity.example.test"),
            ("ZITADEL_ISSUER_URL", "https://issuer.example.test"),
            ("FOUNDATION_PLATFORM_ZITADEL_AUDIENCE", "foundation-api"),
            ("FOUNDATION_PLATFORM_DB_MAX_CONNECTIONS", "12"),
            ("FOUNDATION_PLATFORM_DB_MIN_CONNECTIONS", "2"),
            ("FOUNDATION_PLATFORM_DB_ACQUIRE_TIMEOUT_MS", "750"),
            ("FOUNDATION_PLATFORM_DB_STATEMENT_TIMEOUT_MS", "3000"),
            ("FOUNDATION_PLATFORM_DB_IDLE_TIMEOUT_SECONDS", "120"),
        ]);

        let config = AppConfig::from_vars(|key| vars.get(key).cloned())?;

        assert_eq!(config.database.max_connections, 12);
        assert_eq!(config.database.min_connections, 2);
        assert_eq!(config.database.acquire_timeout_ms, 750);
        assert_eq!(config.database.statement_timeout_ms, 3000);
        assert_eq!(config.database.idle_timeout_seconds, 120);
        Ok(())
    }

    #[test]
    fn app_config_rejects_invalid_database_budget() -> anyhow::Result<()> {
        let vars = vars(&[
            (
                "DATABASE_URL",
                "postgres://foundation_platform:secret@localhost:15434/foundation_platform",
            ),
            ("ZITADEL_ISSUER_URL", "https://issuer.example.test"),
            ("FOUNDATION_PLATFORM_DB_MAX_CONNECTIONS", "2"),
            ("FOUNDATION_PLATFORM_DB_MIN_CONNECTIONS", "3"),
        ]);

        let Err(error) = AppConfig::from_vars(|key| vars.get(key).cloned()) else {
            return Err(anyhow::anyhow!("min > max should be rejected"));
        };

        assert!(error
            .to_string()
            .contains("FOUNDATION_PLATFORM_DB_MIN_CONNECTIONS"));
        Ok(())
    }
}
