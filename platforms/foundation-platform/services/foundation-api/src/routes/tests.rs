use super::{
    canonical_route_label, cors_allowed_origins_from, enforce_concurrency_limit, metrics,
    metrics_body, readiness_status, record_http_metrics, router, MetricsBodyInput,
    TrafficMiddlewareState,
};
use crate::identity_authorization::{
    AuthorizedPrincipal, IdentityAuthorization, IdentityAuthorizationError, RequiredPrincipalKind,
};
use crate::state::{
    ApiDatabasePoolMetric, ApiHttpDurationMetric, ApiHttpRequestMetric, AppState,
    IngestionRunMetric, LakehouseBatchRunMetric, OutboxQueueMetric,
};
use crate::traffic::{TrafficConfig, TrafficRuntime};
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderValue, Method, Request, StatusCode};
use axum::middleware;
use axum::routing::get;
use axum::Router;
use lakehouse_application::ports::LakehouseRegistryUnitOfWork;
use lakehouse_application::{
    RegisterLakehouseObjectArtifactCommand, RegisterLakehouseObjectArtifactReceipt,
};
use lakehouse_domain::LakehouseError;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower::ServiceExt;

struct ServiceIdentityAuthorization;

#[derive(Default)]
struct RecordingLakehouseRegistryUnitOfWork {
    commands: Mutex<Vec<RegisterLakehouseObjectArtifactCommand>>,
}

#[async_trait]
impl LakehouseRegistryUnitOfWork for RecordingLakehouseRegistryUnitOfWork {
    async fn register_object_artifact(
        &self,
        command: RegisterLakehouseObjectArtifactCommand,
    ) -> Result<RegisterLakehouseObjectArtifactReceipt, LakehouseError> {
        self.commands.lock().await.push(command.clone());
        Ok(RegisterLakehouseObjectArtifactReceipt {
            artifact_id: "artifact-1".to_owned(),
            qualified_name: command.qualified_name,
            object_key: command.object_key,
        })
    }
}

#[async_trait]
impl IdentityAuthorization for ServiceIdentityAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        _resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        if required_principal_kind == RequiredPrincipalKind::Service
            && matches!(
                (resource, action),
                ("foundation.catalog", "read")
                    | ("foundation.lakehouse", "write")
                    | ("foundation.normalization", "propose")
            )
        {
            return Ok(AuthorizedPrincipal {
                principal_id: uuid::Uuid::now_v7(),
                trace_id: trace_id.to_owned(),
            });
        }
        Err(IdentityAuthorizationError::Forbidden)
    }
}

fn service_identity_authorization() -> Arc<dyn IdentityAuthorization> {
    Arc::new(ServiceIdentityAuthorization)
}

async fn slow_test_handler() -> StatusCode {
    tokio::time::sleep(Duration::from_millis(200)).await;
    StatusCode::OK
}

fn db_pool_metric() -> ApiDatabasePoolMetric {
    ApiDatabasePoolMetric {
        pool_size: 0,
        idle_connections: 0,
        max_connections: 8,
    }
}

fn empty_metrics_body_input<'a>() -> super::MetricsBodyInput<'a> {
    super::MetricsBodyInput {
        http_requests: &[],
        http_durations: &[],
        overload_rejections: &[],
        lakehouse_batch_runs: &[],
        ingestion_runs: &[],
        outbox_queues: &[],
    }
}

fn valid_normalization_proposal_body(
    commit_allowed: bool,
    requires_human_review: bool,
    validation_accepted: bool,
    proposal_raw_record_id: &str,
) -> serde_json::Value {
    serde_json::json!({
        "request": {
            "tenant_id": "tenant-alpha",
            "source_system": "foundation-platform-r2",
            "raw_record_id": "r2://foundation-platform/raw/company-1.json",
            "raw_object_key": "bronze/source=fixture/company-1.json",
            "raw_checksum_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "raw_record": {
                "name": "(주) 에이씨엠이",
                "address": "SYNTHETIC-CITY SYNTHETIC-DISTRICT"
            },
            "trace_context": {
                "trace_id": "trace-normalization-1",
                "tenant_id": "tenant-alpha",
                "human_user_id": null,
                "product_id": "foundation-platform"
            },
            "target_schema": {
                "type": "industrial_complex.normalized"
            },
            "target_kind": "industrial_complex",
            "target_identity": {
                "official_complex_code": "IC-001"
            },
            "target_schema_version": "industrial_complex.normalized.v1",
            "dictionaries": {}
        },
        "proposal": {
            "raw_record_id": proposal_raw_record_id,
            "schema_version": "industrial_complex.normalized.v1",
            "record": {
                "official_complex_code": "IC-001",
                "name": "ACME",
                "area_m2": 1200
            },
            "confidence": 0.86,
            "evidence": {
                "fields": ["raw.name"],
                "source": "raw_record"
            },
            "reasoning_summary": "한국어 원문을 회사명 표준형으로 정규화했습니다."
        },
        "validation": {
            "accepted": validation_accepted,
            "issues": []
        },
        "trace_context": {
            "trace_id": "trace-normalization-1",
            "tenant_id": "tenant-alpha",
            "human_user_id": null,
            "product_id": "foundation-platform"
        },
        "commit_allowed": commit_allowed,
        "requires_human_review": requires_human_review,
        "submission_metadata": {
            "producer": "intelligence-platform",
            "model_profile_id": "normalization-ko",
            "model_id": "local-normalizer-v1",
            "prompt_id": "industrial-complex-normalize",
            "prompt_version": "v1",
            "policy_id": "normalization-policy",
            "policy_version": "v1"
        }
    })
}

fn valid_building_register_floor_normalization_proposal_body() -> serde_json::Value {
    serde_json::json!({
        "request": {
            "tenant_id": "tenant-alpha",
            "source_system": "foundation-platform-r2-bronze",
            "raw_record_id": "datagokr__building_register_floor/11110/10100/page-000001#row-42",
            "raw_object_key": "bronze/source=datagokr__building_register_floor/sigungu=11110/bjdong=10100/page-000001.json",
            "raw_checksum_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "trace_context": {
                "trace_id": "trace-floor-normalization-1",
                "tenant_id": "tenant-alpha",
                "human_user_id": null,
                "product_id": "foundation-platform"
            },
            "target_schema": {
                "type": "building_register_floor.normalized"
            },
            "target_kind": "building_register_floor",
            "target_identity": {
                "raw_record_id": "datagokr__building_register_floor/11110/10100/page-000001#row-42",
                "source_system": "foundation-platform-r2-bronze"
            },
            "target_schema_version": "building_register_floor.normalized.v1",
            "dictionaries": {}
        },
        "proposal": {
            "raw_record_id": "datagokr__building_register_floor/11110/10100/page-000001#row-42",
            "schema_version": "building_register_floor.normalized.v1",
            "record": {
                "floor_label": "지하 1층",
                "floor_kind": "basement",
                "floor_number": 1,
                "proposal_required": false
            },
            "confidence": 0.91,
            "evidence": {
                "fields": ["raw.floor_division_name", "raw.floor_number"],
                "source": "building_register_floor_silver_handoff"
            }
        },
        "validation": {
            "accepted": true,
            "issues": []
        },
        "trace_context": {
            "trace_id": "trace-floor-normalization-1",
            "tenant_id": "tenant-alpha",
            "human_user_id": null,
            "product_id": "foundation-platform"
        },
        "commit_allowed": false,
        "requires_human_review": true,
        "submission_metadata": {
            "producer": "intelligence-platform",
            "model_profile_id": "qwen-floor-normalization",
            "model_id": "qwen3.6",
            "prompt_id": "building-register-floor-normalize",
            "prompt_version": "v1",
            "policy_id": "building-register-floor-normalization",
            "policy_version": "v1"
        }
    })
}

fn valid_building_register_unit_normalization_proposal_body() -> serde_json::Value {
    serde_json::json!({
        "request": {
            "tenant_id": "tenant-alpha",
            "source_system": "foundation-platform.silver.building_register_units",
            "raw_record_id": "building-register-unit:bronze/source=hubgokr__building_register_exclusive_unit/OPN209912310000000003.zip#line-000001",
            "raw_object_key": "bronze/source=hubgokr__building_register_exclusive_unit/OPN209912310000000003.zip",
            "raw_checksum_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "trace_context": {
                "trace_id": "trace-unit-normalization-1",
                "tenant_id": "tenant-alpha",
                "human_user_id": null,
                "product_id": "foundation-platform"
            },
            "target_schema": {
                "type": "building_register_unit.normalized"
            },
            "target_kind": "building_register_unit",
            "target_identity": {
                "raw_record_id": "building-register-unit:bronze/source=hubgokr__building_register_exclusive_unit/OPN209912310000000003.zip#line-000001",
                "source_system": "foundation-platform.silver.building_register_units"
            },
            "target_schema_version": "building_register_unit.normalized.v1",
            "dictionaries": {}
        },
        "proposal": {
            "raw_record_id": "building-register-unit:bronze/source=hubgokr__building_register_exclusive_unit/OPN209912310000000003.zip#line-000001",
            "schema_version": "building_register_unit.normalized.v1",
            "record": {
                "building_link_method": "canonical_dong",
                "building_mgm_bldrgst_pk": "SYNTHETIC-BUILDING-PK-0001",
                "normalization_reason": "no_unit_number",
                "normalization_status": "proposal_required",
                "review_message_ko": "비표준 호명이라 관리자 검토가 필요합니다.",
                "unit_number": null
            },
            "confidence": 0.95,
            "evidence": {
                "fields": ["raw.unit_name", "entity_context.neighbor_unit_examples"],
                "source": "building_register_unit_silver_handoff"
            }
        },
        "validation": {
            "accepted": true,
            "issues": []
        },
        "trace_context": {
            "trace_id": "trace-unit-normalization-1",
            "tenant_id": "tenant-alpha",
            "human_user_id": null,
            "product_id": "foundation-platform"
        },
        "commit_allowed": false,
        "requires_human_review": true,
        "submission_metadata": {
            "producer": "intelligence-platform",
            "model_profile_id": "normalization-ko",
            "model_id": "qwen3.6",
            "prompt_id": "building-register-unit-normalize",
            "prompt_version": "v1",
            "policy_id": "building-register-unit-normalization",
            "policy_version": "v1"
        }
    })
}

fn normalization_service_request(
    body: &serde_json::Value,
    token: Option<&str>,
) -> Result<Request<Body>, axum::http::Error> {
    normalization_service_request_to("/internal/normalization/proposals", body, token)
}

fn normalization_service_request_to(
    uri: &str,
    body: &serde_json::Value,
    token: Option<&str>,
) -> Result<Request<Body>, axum::http::Error> {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    builder.body(Body::from(body.to_string()))
}

fn normalization_admin_request_with_intelligence_service_token(
    uri: &str,
    body: &serde_json::Value,
) -> Result<Request<Body>, axum::http::Error> {
    normalization_service_request_to(
        uri,
        body,
        Some("foundation-platform-intelligence-token-32-valid"),
    )
}

fn normalization_admin_request(
    uri: &str,
    body: &serde_json::Value,
) -> Result<Request<Body>, axum::http::Error> {
    normalization_service_request_to(uri, body, Some("foundation-platform-staff-token"))
}

async fn with_intelligence_service_token<F, Fut>(test: F) -> Result<(), Box<dyn Error>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn Error>>>,
{
    test().await
}

mod cors_and_labels;
mod health_and_metrics;
mod normalization;
mod routing;
