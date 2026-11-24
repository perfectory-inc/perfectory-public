//! Staff routes that read the data catalog (root ADR-0117, ADR-0119, ADR-0609).
//!
//! No fact lives in a catalog store of its own (root ADR-0609 §1): a dataset is a node of the
//! pipeline graph, its columns are its Iceberg table's current schema, its runs are the
//! orchestrator's and its quality results are Dagster's asset check evaluations. Every route is
//! staff-only and authorized by the Identity Platform as `foundation.metadata:read`. Foundation
//! asks fixed questions and returns its own types, so the console depends on none of those stores.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::{
    routing::{get, post},
    Router,
};
use foundation_contracts::data_catalog::{
    AuditDigestView, AuditEventPage, AuditEventView, AuditImportSource, AuditImportStatusPage,
    ByPnuCutoverApproval, ByPnuCutoverEvent, ByPnuCutovers, CiTimeTrend, CiWeek, DataCatalogEntity,
    DataCatalogEntityKind, DataCatalogField, DataCatalogNeighbourhood, DataCatalogSearchPage,
    DataContractQuality, DataQualityCheck, DataQualityDetail, DataQualityResult, DataQualityStatus,
    DatasetSnapshot, DatasetSnapshotLink, DatasetSnapshots, EdgeCheck, EdgeHostCheck, FailureCause,
    OperationsLinks, OperatorActionRecorded, OperatorActionRequest, ParcelEditionPromotion,
    ParcelEditionPromotionReadiness, ParcelEditionPromotionRequest, ParcelEditionReadiness,
    ParcelEditionStage, ParcelEditionStages, ParcelEditions, ScheduledJobDataset,
    ScheduledJobProblem, ScheduledJobRun, ScheduledJobRunOutcome, ScheduledJobStart,
    ScheduledJobStatus, ScheduledJobsStatus, ServiceChecks,
};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use super::api_error::ApiError;
use crate::state::AppState;

mod alerts;
mod audit;
mod by_pnu_cutovers;
mod entities;
mod facts;
mod health;
mod main_red;
mod operations;
mod operator_actions;
mod parcel_editions;
mod repository_drift;
mod security_alerts;
mod service_checks;
mod snapshots;

pub(super) use alerts::problem_metrics as scheduled_job_problem_metrics;
pub(super) use main_red::main_red_metrics;
pub(super) use repository_drift::repository_drift_metrics;
pub(super) use security_alerts::advisory_metrics as security_advisory_metrics;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Foundation Platform staff data catalog API",
        version = "1.0.0",
        description = "Find data and read its description, columns and one step of lineage either way (root ADR-0117, ADR-0119); see how the scheduled jobs ran and what the data contracts' checks found (root ADR-0165); and how the live hostnames are reached from Korea and how long pull request CI takes (root ADR-0209); and whether each cadastral edition is all there and who served which (root ADR-0379)."
    ),
    paths(
        entities::search,
        entities::entity,
        operations::scheduled_jobs,
        operations::quality_checks,
        operator_actions::record_operator_action,
        snapshots::dataset_snapshots,
        service_checks::service_checks,
        parcel_editions::parcel_editions,
        by_pnu_cutovers::by_pnu_cutovers,
        audit::audit_events,
        audit::export_audit_events,
        audit::audit_import_status
    ),
    components(schemas(
        DataCatalogEntity,
        DataCatalogEntityKind,
        DataCatalogField,
        DataCatalogNeighbourhood,
        DataCatalogSearchPage,
        ScheduledJobsStatus,
        ScheduledJobStatus,
        ScheduledJobRun,
        ScheduledJobRunOutcome,
        ScheduledJobStart,
        ScheduledJobDataset,
        ScheduledJobProblem,
        OperatorActionRequest,
        OperatorActionRecorded,
        AuditEventPage,
        AuditEventView,
        AuditImportStatusPage,
        AuditImportSource,
        AuditDigestView,
        FailureCause,
        OperationsLinks,
        DatasetSnapshots,
        DatasetSnapshot,
        DatasetSnapshotLink,
        DataQualityStatus,
        DataContractQuality,
        DataQualityCheck,
        DataQualityResult,
        DataQualityDetail,
        ServiceChecks,
        EdgeCheck,
        EdgeHostCheck,
        CiTimeTrend,
        CiWeek,
        ParcelEditions,
        ByPnuCutovers,
        ByPnuCutoverApproval,
        ByPnuCutoverEvent,
        ParcelEditionReadiness,
        ParcelEditionStages,
        ParcelEditionStage,
        ParcelEditionPromotion,
        ParcelEditionPromotionReadiness,
        ParcelEditionPromotionRequest,
        ApiErrorResponse,
        InternalApiErrorResponse,
    )),
    modifiers(&DataCatalogSecurity),
    security(("data_catalog_staff_bearer" = []))
)]
struct DataCatalogApiDoc;

/// The deterministic `OpenAPI` document of the staff data catalog API.
#[must_use]
pub fn data_catalog_openapi_document() -> utoipa::openapi::OpenApi {
    DataCatalogApiDoc::openapi()
}

struct DataCatalogSecurity;

impl Modify for DataCatalogSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi
            .components
            .get_or_insert_default()
            .add_security_scheme(
                "data_catalog_staff_bearer",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .description(Some("Authorized Foundation Platform staff identity"))
                        .build(),
                ),
            );
    }
}

pub(super) fn routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    let read = super::STAFF_METADATA_READ;
    Router::new()
        .route(
            "/data-catalog/v1/search",
            super::protected_route(get(entities::search), state, read, None),
        )
        .route(
            "/data-catalog/v1/entity",
            super::protected_route(get(entities::entity), state, read, None),
        )
        .route(
            "/data-catalog/v1/scheduled-jobs",
            super::protected_route(get(operations::scheduled_jobs), state, read, None),
        )
        .route(
            "/data-catalog/v1/quality-checks",
            super::protected_route(get(operations::quality_checks), state, read, None),
        )
        .route(
            "/data-catalog/v1/dataset-snapshots",
            super::protected_route(get(snapshots::dataset_snapshots), state, read, None),
        )
        .route(
            "/data-catalog/v1/service-checks",
            super::protected_route(get(service_checks::service_checks), state, read, None),
        )
        .route(
            "/data-catalog/v1/parcel-editions",
            super::protected_route(get(parcel_editions::parcel_editions), state, read, None),
        )
        .route(
            "/data-catalog/v1/by-pnu-cutovers",
            super::protected_route(get(by_pnu_cutovers::by_pnu_cutovers), state, read, None),
        )
        .route(
            "/data-catalog/v1/scheduled-jobs/{job_id}/operator-actions",
            super::protected_route(
                post(operator_actions::record_operator_action),
                state,
                super::STAFF_JOBS_OPERATE,
                Some("job_id"),
            ),
        )
        // The audit chain (root ADR-0569 §7).
        .route(
            "/data-catalog/v1/audit/events",
            super::protected_route(
                get(audit::audit_events),
                state,
                super::STAFF_AUDIT_READ,
                None,
            ),
        )
        .route(
            "/data-catalog/v1/audit/events/export",
            super::protected_route(
                get(audit::export_audit_events),
                state,
                super::STAFF_AUDIT_READ,
                None,
            ),
        )
        .route(
            "/data-catalog/v1/audit/import-status",
            super::protected_route(
                get(audit::audit_import_status),
                state,
                super::STAFF_AUDIT_READ,
                None,
            ),
        )
}

fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every GraphQL document this module and its submodules send, read from their source so a
    /// new one is checked without being listed: each `const ...: &str = "query...";`. Cargo runs
    /// a test in its package directory.
    fn graphql_documents() -> std::io::Result<Vec<(String, String)>> {
        let routes = std::path::Path::new("src/routes");
        let mut files = vec![routes.join("data_catalog.rs")];
        // A module's own folder (`operations/`, `facts/`) holds documents too.
        let mut folders = vec![routes.join("data_catalog")];
        while let Some(folder) = folders.pop() {
            for entry in std::fs::read_dir(folder)? {
                let path = entry?.path();
                if path.is_dir() {
                    folders.push(path);
                } else {
                    files.push(path);
                }
            }
        }
        let mut found = Vec::new();
        for file in files {
            let source = std::fs::read_to_string(&file)?.replace("\r\n", "\n");
            for chunk in source.split("\nconst ").skip(1) {
                let Some((name, rest)) = chunk.split_once(": &str = \"") else {
                    continue;
                };
                let Some((body, _)) = rest.split_once("\";\n") else {
                    continue;
                };
                if body.starts_with("query") || body.starts_with("fragment") {
                    found.push((name.to_owned(), body.replace("\\\"", "\"")));
                }
            }
        }
        Ok(found)
    }

    #[test]
    fn every_graphql_document_closes_what_it_opens() -> std::io::Result<()> {
        // 2026-10-09 to 10-12 the flows query lacked one `}`: DataHub refused it, and every job
        // read as "runs could not be read" in production while the fake catalog never parsed it.
        let documents = graphql_documents()?;
        assert!(documents.len() >= 2, "found only {documents:?}");
        for (name, body) in documents {
            let mut open = Vec::new();
            for character in body.chars() {
                match character {
                    '{' | '(' | '[' => open.push(character),
                    '}' | ')' | ']' => {
                        let expected = match character {
                            '}' => '{',
                            ')' => '(',
                            _ => '[',
                        };
                        assert_eq!(
                            open.pop(),
                            Some(expected),
                            "{name} closes {character} it never opened"
                        );
                    }
                    _ => {}
                }
            }
            assert!(open.is_empty(), "{name} leaves {open:?} open");
        }
        Ok(())
    }
}
