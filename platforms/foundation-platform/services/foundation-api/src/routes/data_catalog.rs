//! Staff routes that read the data catalog (root ADR-0117, ADR-0119).
//!
//! The catalog's store is `DataHub`, reached only from Foundation on the shared metadata network;
//! no browser talks to it. Every route is staff-only and authorized by the Identity Platform as
//! `foundation.metadata:read`. Foundation asks fixed questions and returns its own types, so the
//! console never depends on `DataHub`'s `GraphQL` schema and another store could stand behind it.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use foundation_contracts::data_catalog::{
    DataCatalogEntity, DataCatalogEntityKind, DataCatalogField, DataCatalogNeighbourhood,
    DataCatalogSearchPage,
};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{IntoParams, Modify, OpenApi};

use super::api_error::ApiError;
use crate::state::AppState;

const GMS_URL_ENV: &str = "FOUNDATION_PLATFORM_DATAHUB_GMS_URL";
const MAX_PAGE: u32 = 50;
const MAX_NEIGHBOURS: u32 = 200;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Foundation Platform staff data catalog API",
        version = "1.0.0",
        description = "Find data and read its description, columns and one step of lineage either way (root ADR-0117, ADR-0119)."
    ),
    paths(search, entity),
    components(schemas(
        DataCatalogEntity,
        DataCatalogEntityKind,
        DataCatalogField,
        DataCatalogNeighbourhood,
        DataCatalogSearchPage,
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
            super::protected_route(get(search), state, read, None),
        )
        .route(
            "/data-catalog/v1/entity",
            super::protected_route(get(entity), state, read, None),
        )
}

/// What to search for.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct SearchQuery {
    /// Words to find in names and descriptions; `*` or empty lists everything.
    q: Option<String>,
    /// Zero-based offset of the first result.
    start: Option<u32>,
    /// Results per page, 1 to 50 (default 20).
    count: Option<u32>,
}

/// Which entity to read.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct EntityQuery {
    /// The entity's `urn`, exactly as a search result or a neighbour gave it.
    urn: String,
}

/// Finds datasets by name or description.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/search",
    operation_id = "searchDataCatalog",
    params(SearchQuery),
    responses(
        (status = 200, body = DataCatalogSearchPage),
        (status = 400, description = "A page size outside 1..=50", body = ApiErrorResponse),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
        (status = 503, description = "The data catalog is not configured or not reachable", body = ApiErrorResponse),
    )
)]
async fn search(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<DataCatalogSearchPage>, ApiError> {
    let count = query.count.unwrap_or(20);
    if count == 0 || count > MAX_PAGE {
        return Err(ApiError::BadRequest(format!(
            "count must be between 1 and {MAX_PAGE}"
        )));
    }
    let start = query.start.unwrap_or(0);
    let text = query
        .q
        .as_deref()
        .map(str::trim)
        .filter(|q| !q.is_empty())
        .unwrap_or("*");
    let data = graphql(
        SEARCH_QUERY,
        json!({ "q": text, "start": start, "count": count }),
    )
    .await?;
    let found = &data["searchAcrossEntities"];
    let results = found["searchResults"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| entity_from(&row["entity"]))
                .collect()
        })
        .unwrap_or_default();
    Ok(Json(DataCatalogSearchPage {
        total: as_u32(&found["total"]),
        start: as_u32(&found["start"]),
        results,
    }))
}

/// Reads one entity with its columns and one step of lineage either way.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/entity",
    operation_id = "getDataCatalogEntity",
    params(EntityQuery),
    responses(
        (status = 200, body = DataCatalogNeighbourhood),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
        (status = 404, description = "No such entity", body = ApiErrorResponse),
        (status = 503, description = "The data catalog is not configured or not reachable", body = ApiErrorResponse),
    )
)]
async fn entity(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<EntityQuery>,
) -> Result<Json<DataCatalogNeighbourhood>, ApiError> {
    if !query.urn.starts_with("urn:li:dataset:") && !query.urn.starts_with("urn:li:dataJob:") {
        return Err(ApiError::NotFound(
            "only dataset and job urns are in the data catalog".to_owned(),
        ));
    }
    let data = graphql(
        ENTITY_QUERY,
        json!({ "urn": query.urn, "count": MAX_NEIGHBOURS }),
    )
    .await?;
    let node = &data["entity"];
    let Some(entity) = entity_from(node) else {
        return Err(ApiError::NotFound("no such data catalog entity".to_owned()));
    };
    let fields = node["schemaMetadata"]["fields"]
        .as_array()
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| {
                    Some(DataCatalogField {
                        path: field["fieldPath"].as_str()?.to_owned(),
                        native_type: text(&field["nativeDataType"]),
                        description: text(&field["description"]),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Json(DataCatalogNeighbourhood {
        entity,
        fields,
        upstream: neighbours(&node["up"]),
        downstream: neighbours(&node["down"]),
    }))
}

const ENTITY_FRAGMENT: &str = "fragment E on Entity { urn type
  ... on Dataset { name platform { name } properties { name description } editableProperties { description } }
  ... on DataJob { jobId properties { name description } dataFlow { orchestrator } } }";

const SEARCH_QUERY: &str = "query($q: String!, $start: Int!, $count: Int!) {
  searchAcrossEntities(input: { types: [DATASET], query: $q, start: $start, count: $count }) {
    total start searchResults { entity { ...E } } } }";

const ENTITY_QUERY: &str = "query($urn: String!, $count: Int!) { entity(urn: $urn) { ...E
  ... on Dataset { schemaMetadata { fields { fieldPath nativeDataType description } }
    up: lineage(input: { direction: UPSTREAM, start: 0, count: $count }) { relationships { entity { ...E } } }
    down: lineage(input: { direction: DOWNSTREAM, start: 0, count: $count }) { relationships { entity { ...E } } } }
  ... on DataJob {
    up: lineage(input: { direction: UPSTREAM, start: 0, count: $count }) { relationships { entity { ...E } } }
    down: lineage(input: { direction: DOWNSTREAM, start: 0, count: $count }) { relationships { entity { ...E } } } } } }";

fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

/// Asks the catalog store one of the fixed questions above and returns its `data`.
async fn graphql(query: &str, variables: JsonValue) -> Result<JsonValue, ApiError> {
    let base = std::env::var(GMS_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::Unavailable("the data catalog is not configured".to_owned()))?;
    let url = format!("{}/api/graphql", base.trim_end_matches('/'));
    let body = json!({ "query": format!("{query}\n{ENTITY_FRAGMENT}"), "variables": variables });
    let response = http().post(url).json(&body).send().await.map_err(|error| {
        tracing::warn!(%error, "data catalog unreachable");
        ApiError::Unavailable("the data catalog is not reachable".to_owned())
    })?;
    if !response.status().is_success() {
        return Err(ApiError::Unavailable(format!(
            "the data catalog answered {}",
            response.status()
        )));
    }
    let mut answer: JsonValue = response
        .json()
        .await
        .map_err(|error| ApiError::Internal(format!("data catalog answer is not JSON: {error}")))?;
    if let Some(errors) = answer.get("errors").filter(|errors| !errors.is_null()) {
        return Err(ApiError::Internal(format!(
            "data catalog refused the query: {errors}"
        )));
    }
    Ok(answer["data"].take())
}

fn text(value: &JsonValue) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn as_u32(value: &JsonValue) -> u32 {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(0)
}

/// Turns one GraphQL entity into the wire type; anything but a dataset or job is left out.
fn entity_from(value: &JsonValue) -> Option<DataCatalogEntity> {
    let urn = value["urn"].as_str()?.to_owned();
    let properties = &value["properties"];
    match value["type"].as_str()? {
        "DATASET" => Some(DataCatalogEntity {
            name: text(&properties["name"])
                .or_else(|| text(&value["name"]))
                .unwrap_or_else(|| urn.clone()),
            urn,
            kind: DataCatalogEntityKind::Dataset,
            platform: text(&value["platform"]["name"]),
            // A description a person edited in the catalog wins over the one the source stated.
            description: text(&value["editableProperties"]["description"])
                .or_else(|| text(&properties["description"])),
        }),
        "DATA_JOB" => Some(DataCatalogEntity {
            name: text(&properties["name"])
                .or_else(|| text(&value["jobId"]))
                .unwrap_or_else(|| urn.clone()),
            urn,
            kind: DataCatalogEntityKind::Job,
            platform: text(&value["dataFlow"]["orchestrator"]),
            description: text(&properties["description"]),
        }),
        _ => None,
    }
}

fn neighbours(lineage: &JsonValue) -> Vec<DataCatalogEntity> {
    lineage["relationships"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| entity_from(&row["entity"]))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_prefers_the_edited_description_and_falls_back_to_the_urn_for_a_name() {
        let value = json!({
            "urn": "urn:li:dataset:(urn:li:dataPlatform:synthetic,silver.synthetic,PROD)",
            "type": "DATASET",
            "name": null,
            "platform": { "name": "synthetic" },
            "properties": { "name": "silver.synthetic", "description": "stated" },
            "editableProperties": { "description": "edited" }
        });
        let entity = entity_from(&value);
        assert_eq!(
            entity
                .as_ref()
                .map(|e| (e.name.as_str(), e.description.as_deref())),
            Some(("silver.synthetic", Some("edited")))
        );
        let bare = json!({ "urn": "urn:li:dataset:x", "type": "DATASET" });
        assert_eq!(
            entity_from(&bare).map(|e| e.name),
            Some("urn:li:dataset:x".to_owned())
        );
    }

    #[test]
    fn a_job_is_named_by_its_properties_and_other_types_are_left_out() {
        let job = json!({
            "urn": "urn:li:dataJob:x",
            "type": "DATA_JOB",
            "jobId": "build x",
            "properties": { "name": "", "description": "declared" },
            "dataFlow": { "orchestrator": "synthetic" }
        });
        let entity = entity_from(&job);
        assert_eq!(
            entity.map(|e| (e.kind, e.name, e.platform)),
            Some((
                DataCatalogEntityKind::Job,
                "build x".to_owned(),
                Some("synthetic".to_owned())
            ))
        );
        assert_eq!(
            entity_from(&json!({ "urn": "urn:li:chart:x", "type": "CHART" })),
            None
        );
    }
}
