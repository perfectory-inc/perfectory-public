//! Foundation Platform pipeline graph HTTP handler.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::state::{AppState, IngestionRunMetric, LakehouseBatchRunMetric, OutboxQueueMetric};

const PIPELINE_GRAPH_REGISTRY_PATH_ENV: &str = "FOUNDATION_PLATFORM_PIPELINE_GRAPH_REGISTRY_PATH";
const DEFAULT_PIPELINE_GRAPH_REGISTRY_PATH: &str = "docs/catalog/pipeline-graph.v1.json";

static PIPELINE_GRAPH_ARTIFACTS: OnceLock<Result<PipelineGraphArtifacts, String>> = OnceLock::new();

pub async fn get_pipeline_graph(
    State(state): State<Arc<AppState>>,
) -> Result<Json<JsonValue>, PipelineGraphError> {
    let database_ready = state.database_ready().await;
    let (lakehouse_batch_runs, ingestion_runs, outbox_queues) = if database_ready {
        tokio::join!(
            state.latest_lakehouse_batch_run_metrics(),
            state.latest_ingestion_run_metrics(),
            state.outbox_queue_metrics()
        )
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };

    let snapshot = RuntimeSnapshot {
        database_ready,
        lakehouse_batch_runs,
        ingestion_runs,
        outbox_queues,
    };

    Ok(Json(pipeline_graph_response(&snapshot)?))
}

fn pipeline_graph_artifacts() -> Result<&'static PipelineGraphArtifacts, PipelineGraphError> {
    match PIPELINE_GRAPH_ARTIFACTS.get_or_init(load_pipeline_graph_artifacts_from_env) {
        Ok(artifacts) => Ok(artifacts),
        Err(message) => Err(PipelineGraphError {
            message: message.to_owned(),
        }),
    }
}

fn pipeline_graph_response(snapshot: &RuntimeSnapshot) -> Result<JsonValue, PipelineGraphError> {
    pipeline_graph_response_from_artifacts(snapshot, pipeline_graph_artifacts()?)
}

fn pipeline_graph_response_from_artifacts(
    snapshot: &RuntimeSnapshot,
    artifacts: &PipelineGraphArtifacts,
) -> Result<JsonValue, PipelineGraphError> {
    let binding_index = runtime_binding_index(&artifacts.registry)?;
    let mut graph = artifacts.registry.clone();
    let graph_object = graph.as_object_mut().ok_or_else(|| PipelineGraphError {
        message: "pipeline graph registry artifact root is not a JSON object".to_owned(),
    })?;

    let mut runtime = runtime_overlay(snapshot, &binding_index);
    add_missing_capability_overlays(&mut runtime, &artifacts.registry)?;
    graph_object.insert("runtime".to_owned(), runtime);
    Ok(graph)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PipelineGraphArtifacts {
    registry: JsonValue,
}

fn load_pipeline_graph_artifacts_from_env() -> Result<PipelineGraphArtifacts, String> {
    let registry_path = artifact_path_from(
        std::env::var(PIPELINE_GRAPH_REGISTRY_PATH_ENV)
            .ok()
            .as_deref(),
        DEFAULT_PIPELINE_GRAPH_REGISTRY_PATH,
    );
    load_pipeline_graph_artifacts_from_path(&registry_path)
}

fn load_pipeline_graph_artifacts_from_path(
    registry_path: &Path,
) -> Result<PipelineGraphArtifacts, String> {
    Ok(PipelineGraphArtifacts {
        registry: read_json_artifact("pipeline graph registry", registry_path)?,
    })
}

fn read_json_artifact(label: &str, path: &Path) -> Result<JsonValue, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {label} artifact {}: {err}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|err| format!("invalid {label} artifact {}: {err}", path.display()))
}

fn artifact_path_from(raw: Option<&str>, default_relative_path: &str) -> PathBuf {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(
            || repository_root().join(default_relative_path),
            PathBuf::from,
        )
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .components()
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeSnapshot {
    database_ready: bool,
    lakehouse_batch_runs: Vec<LakehouseBatchRunMetric>,
    ingestion_runs: Vec<IngestionRunMetric>,
    outbox_queues: Vec<OutboxQueueMetric>,
}

#[derive(Debug, Default, Eq, PartialEq)]
struct RuntimeBindingIndex {
    ingestion_sources: HashMap<String, String>,
    lakehouse_contracts: HashMap<String, String>,
    outbox_scopes: HashMap<String, String>,
}

fn runtime_binding_index(registry: &JsonValue) -> Result<RuntimeBindingIndex, PipelineGraphError> {
    let nodes = registry
        .get("nodes")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| PipelineGraphError {
            message: "pipeline graph registry artifact nodes is not an array".to_owned(),
        })?;
    let mut index = RuntimeBindingIndex::default();

    for node in nodes {
        let node_id =
            node.get("id")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| PipelineGraphError {
                    message: "pipeline graph node is missing id".to_owned(),
                })?;

        let Some(bindings) = node.get("runtime_bindings") else {
            continue;
        };
        let bindings = bindings.as_array().ok_or_else(|| PipelineGraphError {
            message: format!("{node_id}.runtime_bindings is not an array"),
        })?;

        for binding in bindings {
            let kind = binding
                .get("kind")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| PipelineGraphError {
                    message: format!("{node_id}.runtime_bindings[].kind is required"),
                })?;
            let value = binding
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| PipelineGraphError {
                    message: format!("{node_id}.runtime_bindings[].value is required"),
                })?;

            match kind {
                "ingestion_source" => {
                    insert_runtime_binding(&mut index.ingestion_sources, kind, value, node_id)?;
                }
                "lakehouse_contract" => {
                    insert_runtime_binding(&mut index.lakehouse_contracts, kind, value, node_id)?;
                }
                "outbox_scope" => {
                    insert_runtime_binding(&mut index.outbox_scopes, kind, value, node_id)?;
                }
                _ => {
                    return Err(PipelineGraphError {
                        message: format!("{node_id}.runtime_bindings[] has unknown kind: {kind}"),
                    });
                }
            }
        }
    }

    Ok(index)
}

fn insert_runtime_binding(
    bindings: &mut HashMap<String, String>,
    kind: &str,
    value: &str,
    node_id: &str,
) -> Result<(), PipelineGraphError> {
    if value.trim().is_empty() {
        return Err(PipelineGraphError {
            message: format!("{node_id}.runtime_bindings[] has blank value for kind {kind}"),
        });
    }

    if let Some(previous_node_id) = bindings.insert(value.to_owned(), node_id.to_owned()) {
        if previous_node_id != node_id {
            return Err(PipelineGraphError {
                message: format!(
                    "runtime binding {kind}:{value} is duplicated by {previous_node_id} and {node_id}"
                ),
            });
        }
    }

    Ok(())
}

fn runtime_overlay(snapshot: &RuntimeSnapshot, binding_index: &RuntimeBindingIndex) -> JsonValue {
    let mut nodes = JsonMap::new();

    for metric in &snapshot.ingestion_runs {
        if let Some(node_id) = binding_index.ingestion_sources.get(&metric.source_slug) {
            nodes.insert(node_id.to_owned(), ingestion_overlay(metric));
        }
    }

    for metric in &snapshot.lakehouse_batch_runs {
        if let Some(node_id) = binding_index.lakehouse_contracts.get(&metric.contract) {
            nodes.insert(node_id.to_owned(), lakehouse_overlay(metric));
        }
    }

    for metric in &snapshot.outbox_queues {
        if let Some(node_id) = binding_index.outbox_scopes.get(&metric.scope) {
            nodes.insert(node_id.to_owned(), outbox_overlay(metric));
        }
    }

    nodes.insert(
        "lakehouse-slo-policy".to_owned(),
        slo_policy_overlay(snapshot),
    );

    json!({
        "database_ready": snapshot.database_ready,
        "nodes": nodes,
    })
}

fn add_missing_capability_overlays(
    runtime: &mut JsonValue,
    registry: &JsonValue,
) -> Result<(), PipelineGraphError> {
    let runtime_nodes = runtime
        .get_mut("nodes")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| PipelineGraphError {
            message: "pipeline graph runtime nodes root is not a JSON object".to_owned(),
        })?;

    for node in registry
        .get("nodes")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| PipelineGraphError {
            message: "pipeline graph registry artifact nodes is not an array".to_owned(),
        })?
    {
        if node.get("type").and_then(JsonValue::as_str) != Some("missing_capability") {
            continue;
        }

        let Some(node_id) = node.get("id").and_then(JsonValue::as_str) else {
            continue;
        };
        let status = node
            .get("status")
            .and_then(JsonValue::as_str)
            .unwrap_or("missing");
        let reason = node
            .get("blocking_reason")
            .or_else(|| node.get("description"))
            .and_then(JsonValue::as_str)
            .unwrap_or("missing capability has no runtime evidence");
        let target_state = node
            .get("target_state")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        let evidence_refs = node
            .get("evidence_refs")
            .cloned()
            .unwrap_or_else(|| json!([]));

        runtime_nodes.insert(
            node_id.to_owned(),
            json!({
                "status": status,
                "reason": reason,
                "observed": {
                    "target_state": target_state,
                    "evidence_refs": evidence_refs
                }
            }),
        );
    }

    Ok(())
}

fn ingestion_overlay(metric: &IngestionRunMetric) -> JsonValue {
    json!({
        "status": ingestion_status(metric),
        "reason": format!(
            "latest ingestion run status={} records_seen={} objects_written={}",
            metric.status, metric.logical_records_seen, metric.objects_written
        ),
        "observed": {
            "finished_at_unix_seconds": metric.finished_at_unix_seconds,
            "duration_seconds": metric.duration_seconds,
            "logical_records_seen": metric.logical_records_seen,
            "objects_written": metric.objects_written,
            "raw_response_size_bytes": metric.raw_response_size_bytes
        }
    })
}

fn ingestion_status(metric: &IngestionRunMetric) -> &'static str {
    if metric.status.eq_ignore_ascii_case("failed") {
        return "failed";
    }

    if metric.status.eq_ignore_ascii_case("running") {
        return "running";
    }

    if metric.status.eq_ignore_ascii_case("succeeded") && metric.objects_written > 0 {
        return "healthy";
    }

    "stale"
}

fn lakehouse_overlay(metric: &LakehouseBatchRunMetric) -> JsonValue {
    json!({
        "status": if metric.row_count > 0 { "healthy" } else { "stale" },
        "reason": format!(
            "latest successful lakehouse batch contract={} row_count={}",
            metric.contract, metric.row_count
        ),
        "observed": {
            "created_at_unix_seconds": metric.created_at_unix_seconds,
            "recorded_at_unix_seconds": metric.recorded_at_unix_seconds,
            "row_count": metric.row_count
        }
    })
}

fn outbox_overlay(metric: &OutboxQueueMetric) -> JsonValue {
    json!({
        "status": outbox_status(metric),
        "reason": format!(
            "outbox scope={} pending={} retry={} oldest_pending_age_seconds={}",
            metric.scope,
            metric.pending_event_count,
            metric.retry_event_count,
            metric.oldest_pending_age_seconds
        ),
        "observed": {
            "pending_event_count": metric.pending_event_count,
            "retry_event_count": metric.retry_event_count,
            "oldest_pending_age_seconds": metric.oldest_pending_age_seconds
        }
    })
}

const fn outbox_status(metric: &OutboxQueueMetric) -> &'static str {
    if metric.retry_event_count > 0 {
        return "failed";
    }

    if metric.pending_event_count > 0 {
        return "stale";
    }

    "healthy"
}

fn slo_policy_overlay(snapshot: &RuntimeSnapshot) -> JsonValue {
    json!({
        "status": slo_policy_status(snapshot),
        "reason": slo_policy_reason(snapshot),
        "observed": {
            "database_ready": snapshot.database_ready,
            "lakehouse_batch_run_count": snapshot.lakehouse_batch_runs.len(),
            "ingestion_run_count": snapshot.ingestion_runs.len(),
            "outbox_queue_count": snapshot.outbox_queues.len(),
            "outbox_retry_event_count": snapshot
                .outbox_queues
                .iter()
                .map(|metric| metric.retry_event_count)
                .sum::<i64>(),
            "outbox_pending_event_count": snapshot
                .outbox_queues
                .iter()
                .map(|metric| metric.pending_event_count)
                .sum::<i64>()
        }
    })
}

fn slo_policy_status(snapshot: &RuntimeSnapshot) -> &'static str {
    if !snapshot.database_ready {
        return "unknown";
    }

    if snapshot
        .ingestion_runs
        .iter()
        .any(|metric| metric.status.eq_ignore_ascii_case("failed"))
        || snapshot
            .outbox_queues
            .iter()
            .any(|metric| metric.retry_event_count > 0)
    {
        return "failed";
    }

    let signal_count = snapshot.lakehouse_batch_runs.len()
        + snapshot.ingestion_runs.len()
        + snapshot.outbox_queues.len();
    if signal_count == 0
        || snapshot
            .lakehouse_batch_runs
            .iter()
            .any(|metric| metric.row_count <= 0)
        || snapshot
            .outbox_queues
            .iter()
            .any(|metric| metric.pending_event_count > 0)
    {
        return "stale";
    }

    "healthy"
}

fn slo_policy_reason(snapshot: &RuntimeSnapshot) -> String {
    format!(
        "database_ready={} lakehouse_batches={} ingestion_runs={} outbox_queues={}",
        snapshot.database_ready,
        snapshot.lakehouse_batch_runs.len(),
        snapshot.ingestion_runs.len(),
        snapshot.outbox_queues.len()
    )
}

#[derive(Debug)]
pub struct PipelineGraphError {
    message: String,
}

#[derive(Serialize)]
struct PipelineGraphErrorResponse<'a> {
    code: &'a str,
    message: &'a str,
}

impl IntoResponse for PipelineGraphError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PipelineGraphErrorResponse {
                code: "pipeline_graph_registry_invalid",
                message: &self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{
        artifact_path_from, load_pipeline_graph_artifacts_from_path, pipeline_graph_artifacts,
        pipeline_graph_response, pipeline_graph_response_from_artifacts, runtime_binding_index,
        runtime_overlay, RuntimeSnapshot,
    };
    use crate::state::{IngestionRunMetric, LakehouseBatchRunMetric, OutboxQueueMetric};
    use serde_json::json;
    use std::path::PathBuf;

    fn registry_bindings() -> super::RuntimeBindingIndex {
        runtime_binding_index(
            &pipeline_graph_artifacts()
                .expect("pipeline graph artifacts")
                .registry,
        )
        .expect("runtime bindings")
    }

    #[test]
    fn registry_is_the_foundation_platform_pipeline_graph_contract() {
        let graph = &pipeline_graph_artifacts()
            .expect("pipeline graph artifacts")
            .registry;

        assert_eq!(
            graph["schema_version"],
            "foundation-platform.pipeline_graph.v1"
        );
        assert_eq!(graph["owner"], "foundation-platform");
        assert_eq!(
            graph["viewer_policy"]["canonical_store"],
            "foundation-platform"
        );
        assert_eq!(graph["viewer_policy"]["ui_state_store"], "product-viewer");
        assert!(graph["nodes"].as_array().expect("nodes array").len() >= 32);
        assert!(graph["edges"].as_array().expect("edges array").len() >= 30);
    }

    #[test]
    fn v1_outbox_fanout_ids_use_final_contract_names() {
        let registry = &pipeline_graph_artifacts()
            .expect("pipeline graph artifacts")
            .registry;
        let example: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docs/catalog/pipeline-graph.v1.example.json"
        ))
        .expect("pipeline graph example");

        for graph in [registry, &example] {
            let nodes = graph["nodes"].as_array().expect("nodes array");
            let stable_nodes = nodes
                .iter()
                .filter(|node| node["id"] == "outbox-catalog-event-fanout")
                .collect::<Vec<_>>();
            assert_eq!(stable_nodes.len(), 1);
            assert_eq!(stable_nodes[0]["title"], "Catalog fanout");
            assert_eq!(
                stable_nodes[0]["description"],
                "Outbox fanout carrying Catalog changes to product receivers."
            );
            assert_eq!(
                stable_nodes[0]["runtime_bindings"],
                json!([{"kind": "outbox_scope", "value": "catalog"}])
            );
            assert!(!nodes
                .iter()
                .any(|node| node["id"] == "outbox-catalog-fanout"));

            let edges = graph["edges"].as_array().expect("edges array");
            for (stable_id, from, to) in [
                (
                    "industrial-complex-gold-pointer-publish-to-outbox-catalog-event-fanout",
                    "industrial-complex-gold-pointer-publish",
                    "outbox-catalog-event-fanout",
                ),
                (
                    "outbox-catalog-event-fanout-to-event-fabric-registry",
                    "outbox-catalog-event-fanout",
                    "event-fabric-registry",
                ),
                (
                    "outbox-catalog-event-fanout-to-gongzzang-catalog-consumer-receiver",
                    "outbox-catalog-event-fanout",
                    "gongzzang-catalog-consumer-receiver",
                ),
                (
                    "outbox-catalog-event-fanout-to-dawneer-catalog-consumer-receiver",
                    "outbox-catalog-event-fanout",
                    "dawneer-catalog-consumer-receiver",
                ),
            ] {
                let edge = edges
                    .iter()
                    .find(|edge| edge["id"] == stable_id)
                    .expect("stable v1 edge id");
                assert_eq!(edge["from"], from);
                assert_eq!(edge["to"], to);
            }
        }
    }

    #[test]
    fn artifact_path_uses_env_override_or_repo_default() {
        assert_eq!(
            artifact_path_from(
                Some(" target/custom/pipeline-graph.json "),
                "docs/catalog/pipeline-graph.v1.json",
            ),
            PathBuf::from("target/custom/pipeline-graph.json")
        );

        assert!(
            artifact_path_from(None, "docs/catalog/pipeline-graph.v1.json")
                .ends_with("docs/catalog/pipeline-graph.v1.json")
        );
    }

    #[test]
    fn pipeline_graph_artifacts_load_from_files() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-pipeline-graph-artifacts-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root)?;
        let registry_path = root.join("registry.json");
        std::fs::write(
            &registry_path,
            serde_json::to_vec(&json!({
                "schema_version": "foundation-platform.pipeline_graph.v1",
                "generated_at_utc": "2026-05-19T00:00:00Z",
                "owner": "foundation-platform",
                "viewer_policy": {
                    "canonical_store": "foundation-platform",
                    "ui_state_store": "product-viewer"
                },
                "nodes": [
                    {
                        "id": "custom-cutover-blocker",
                        "type": "missing_capability",
                        "status": "blocked",
                        "owner": "foundation-platform",
                        "title": "Custom blocker",
                        "description": "Loaded from artifact file",
                        "blocking_reason": "external proof required",
                        "target_state": "evidence attached",
                        "evidence_refs": [],
                        "runtime_bindings": []
                    }
                ],
                "edges": []
            }))?,
        )?;

        let artifacts =
            load_pipeline_graph_artifacts_from_path(&registry_path).expect("file-backed artifacts");
        let graph = pipeline_graph_response_from_artifacts(
            &RuntimeSnapshot {
                database_ready: false,
                lakehouse_batch_runs: Vec::new(),
                ingestion_runs: Vec::new(),
                outbox_queues: Vec::new(),
            },
            &artifacts,
        )
        .expect("file-backed graph response");

        assert_eq!(
            graph["schema_version"],
            "foundation-platform.pipeline_graph.v1"
        );
        assert_eq!(
            graph["runtime"]["nodes"]["custom-cutover-blocker"]["reason"],
            "external proof required"
        );

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn response_returns_the_registry_contract() {
        let graph = pipeline_graph_response(&RuntimeSnapshot {
            database_ready: false,
            lakehouse_batch_runs: Vec::new(),
            ingestion_runs: Vec::new(),
            outbox_queues: Vec::new(),
        })
        .expect("pipeline graph response");

        assert_eq!(
            graph["schema_version"],
            "foundation-platform.pipeline_graph.v1"
        );
        assert_eq!(graph["viewer_policy"]["ui_state_store"], "product-viewer");
        assert_eq!(graph["runtime"]["database_ready"], false);
    }

    #[test]
    fn runtime_overlay_resolves_nodes_from_registry_bindings() {
        let registry = json!({
            "nodes": [
                {
                    "id": "custom-building-ingest",
                    "runtime_bindings": [
                        {
                            "kind": "ingestion_source",
                            "value": "custom-building-source"
                        }
                    ]
                },
                {
                    "id": "custom-gold-dataset",
                    "runtime_bindings": [
                        {
                            "kind": "lakehouse_contract",
                            "value": "gold.custom_dataset"
                        }
                    ]
                },
                {
                    "id": "custom-outbox-fanout",
                    "runtime_bindings": [
                        {
                            "kind": "outbox_scope",
                            "value": "custom-scope"
                        }
                    ]
                }
            ]
        });
        let bindings = runtime_binding_index(&registry).expect("runtime bindings");
        let snapshot = RuntimeSnapshot {
            database_ready: true,
            lakehouse_batch_runs: vec![LakehouseBatchRunMetric {
                contract: "gold.custom_dataset".to_owned(),
                created_at_unix_seconds: 1_779_120_000,
                recorded_at_unix_seconds: 1_779_120_030,
                row_count: 42,
            }],
            ingestion_runs: vec![IngestionRunMetric {
                source_slug: "custom-building-source".to_owned(),
                status: "succeeded".to_owned(),
                finished_at_unix_seconds: 1_779_120_060,
                duration_seconds: 90,
                logical_records_seen: 500,
                objects_written: 5,
                raw_response_size_bytes: 65_536,
            }],
            outbox_queues: vec![OutboxQueueMetric {
                scope: "custom-scope".to_owned(),
                pending_event_count: 0,
                retry_event_count: 0,
                oldest_pending_age_seconds: 0,
            }],
        };

        let overlay = runtime_overlay(&snapshot, &bindings);

        assert_eq!(
            overlay["nodes"]["custom-building-ingest"]["status"],
            "healthy"
        );
        assert_eq!(overlay["nodes"]["custom-gold-dataset"]["status"], "healthy");
        assert_eq!(
            overlay["nodes"]["custom-outbox-fanout"]["status"],
            "healthy"
        );
    }

    #[test]
    fn runtime_overlay_marks_observed_pipeline_nodes_without_changing_registry() {
        let snapshot = RuntimeSnapshot {
            database_ready: true,
            lakehouse_batch_runs: vec![LakehouseBatchRunMetric {
                contract: "gold.complex_catalog".to_owned(),
                created_at_unix_seconds: 1_779_120_000,
                recorded_at_unix_seconds: 1_779_120_030,
                row_count: 42,
            }],
            ingestion_runs: vec![IngestionRunMetric {
                source_slug: "data-go-kr-building-register".to_owned(),
                status: "succeeded".to_owned(),
                finished_at_unix_seconds: 1_779_120_060,
                duration_seconds: 90,
                logical_records_seen: 500,
                objects_written: 5,
                raw_response_size_bytes: 65_536,
            }],
            outbox_queues: vec![OutboxQueueMetric {
                scope: "catalog".to_owned(),
                pending_event_count: 0,
                retry_event_count: 0,
                oldest_pending_age_seconds: 0,
            }],
        };

        let bindings = registry_bindings();
        let overlay = runtime_overlay(&snapshot, &bindings);

        assert_eq!(overlay["database_ready"], true);
        assert_eq!(
            overlay["nodes"]["building-register-bronze-ingest"]["status"],
            "healthy"
        );
        assert_eq!(
            overlay["nodes"]["gold-complex-catalog"]["status"],
            "healthy"
        );
        assert_eq!(
            overlay["nodes"]["outbox-catalog-event-fanout"]["status"],
            "healthy"
        );
        assert_eq!(
            pipeline_graph_artifacts().expect("registry").registry["nodes"][11]["status"],
            "blocked"
        );
    }

    #[test]
    fn response_exposes_missing_capabilities_as_runtime_evidence_nodes() {
        let graph = pipeline_graph_response(&RuntimeSnapshot {
            database_ready: false,
            lakehouse_batch_runs: Vec::new(),
            ingestion_runs: Vec::new(),
            outbox_queues: Vec::new(),
        })
        .expect("pipeline graph response");

        assert_eq!(
            graph["runtime"]["nodes"]["production-orchestrator"]["status"],
            "missing"
        );
        assert_eq!(
            graph["runtime"]["nodes"]["openlineage-production-receiver"]["status"],
            "missing"
        );
        assert!(
            graph["runtime"]["nodes"]["openlineage-production-receiver"]["reason"]
                .as_str()
                .expect("reason")
                .contains("production receiver endpoint")
        );
        assert!(
            graph["runtime"]["nodes"]["production-orchestrator"]["reason"]
                .as_str()
                .expect("reason")
                .contains("approved production orchestrator run evidence")
        );
        assert!(
            graph["runtime"]["nodes"]["production-orchestrator"]["observed"]
                .get("blocker_status")
                .is_none()
        );
    }

    #[test]
    fn runtime_overlay_derives_slo_policy_status_from_observed_signals() {
        let healthy = RuntimeSnapshot {
            database_ready: true,
            lakehouse_batch_runs: vec![LakehouseBatchRunMetric {
                contract: "silver.industrial_complexes".to_owned(),
                created_at_unix_seconds: 1_779_120_000,
                recorded_at_unix_seconds: 1_779_120_030,
                row_count: 10,
            }],
            ingestion_runs: vec![IngestionRunMetric {
                source_slug: "vworld-cadastral".to_owned(),
                status: "succeeded".to_owned(),
                finished_at_unix_seconds: 1_779_120_060,
                duration_seconds: 90,
                logical_records_seen: 500,
                objects_written: 5,
                raw_response_size_bytes: 65_536,
            }],
            outbox_queues: vec![OutboxQueueMetric {
                scope: "catalog".to_owned(),
                pending_event_count: 0,
                retry_event_count: 0,
                oldest_pending_age_seconds: 0,
            }],
        };

        let bindings = registry_bindings();
        assert_eq!(
            runtime_overlay(&healthy, &bindings)["nodes"]["lakehouse-slo-policy"]["status"],
            "healthy"
        );

        let failed = RuntimeSnapshot {
            outbox_queues: vec![OutboxQueueMetric {
                scope: "catalog".to_owned(),
                pending_event_count: 1,
                retry_event_count: 1,
                oldest_pending_age_seconds: 600,
            }],
            ..healthy
        };

        assert_eq!(
            runtime_overlay(&failed, &bindings)["nodes"]["lakehouse-slo-policy"]["status"],
            "failed"
        );
    }

    #[test]
    fn response_exposes_static_missing_capabilities_without_completion_metadata() {
        let graph = pipeline_graph_response(&RuntimeSnapshot {
            database_ready: true,
            lakehouse_batch_runs: Vec::new(),
            ingestion_runs: Vec::new(),
            outbox_queues: Vec::new(),
        })
        .expect("pipeline graph response");

        assert_eq!(
            graph["runtime"]["nodes"]["production-orchestrator"]["status"],
            "missing"
        );
        assert_eq!(
            graph["runtime"]["nodes"]["consumer-deployed-receiver-e2e"]["status"],
            "blocked"
        );
        assert!(
            graph["runtime"]["nodes"]["production-orchestrator"]["reason"]
                .as_str()
                .expect("reason")
                .contains("production orchestrator run evidence")
        );
        assert!(graph["runtime"].get("cutover").is_none());
    }
}
