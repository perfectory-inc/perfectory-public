//! Finding data and reading one dataset with its columns and one step of lineage either way
//! (root ADR-0609 §1, §3).
//!
//! An entity is a node of the pipeline graph (`docs/catalog/pipeline-graph.v1.json`, the file
//! `GET /catalog/v1/pipeline-graph` serves): its identifier is the node's `id`, its neighbours are
//! the graph's edges, and the columns of a node that names an Iceberg table are that table's
//! current schema as the lakehouse catalog states it. Nothing here is copied into another store,
//! so an answer cannot be older than the release (the graph) or the table (its columns).

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    Json,
};
use foundation_contracts::data_catalog::{
    DataCatalogEntity, DataCatalogEntityKind, DataCatalogField, DataCatalogNeighbourhood,
    DataCatalogSearchPage,
};
use foundation_contracts::error::ApiErrorResponse;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use utoipa::IntoParams;

use super::ApiError;
use crate::routes::pipeline_graph;
use crate::state::AppState;

const MAX_PAGE: u32 = 50;
const MAX_NEIGHBOURS: usize = 200;

/// What to search for.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct SearchQuery {
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
pub(super) struct EntityQuery {
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
pub(super) async fn search(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<DataCatalogSearchPage>, ApiError> {
    let count = query.count.unwrap_or(20);
    if count == 0 || count > MAX_PAGE {
        return Err(ApiError::BadRequest(format!(
            "count must be between 1 and {MAX_PAGE}"
        )));
    }
    Ok(Json(search_page(
        graph()?,
        query.q.as_deref().unwrap_or(""),
        query.start.unwrap_or(0),
        count,
    )))
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
pub(super) async fn entity(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<EntityQuery>,
) -> Result<Json<DataCatalogNeighbourhood>, ApiError> {
    let graph = graph()?;
    let Some(node) = nodes(graph).find(|node| node["id"].as_str() == Some(query.urn.as_str()))
    else {
        return Err(ApiError::NotFound("no such data catalog entity".to_owned()));
    };
    let Some(entity) = entity_of(node) else {
        return Err(ApiError::NotFound("no such data catalog entity".to_owned()));
    };
    let fields = match text(&node["table_name"]) {
        Some(table) => columns_of(&table).await?,
        None => Vec::new(),
    };
    let (upstream, downstream) = neighbours(graph, &query.urn);
    Ok(Json(DataCatalogNeighbourhood {
        entity,
        fields,
        upstream,
        downstream,
    }))
}

fn graph() -> Result<&'static JsonValue, ApiError> {
    pipeline_graph::registry().map_err(|error| {
        tracing::warn!(%error, "pipeline graph unreadable");
        ApiError::Unavailable("the pipeline graph could not be read".to_owned())
    })
}

fn nodes<'a>(graph: &'a JsonValue) -> impl Iterator<Item = &'a JsonValue> + 'a {
    graph["nodes"].as_array().into_iter().flatten()
}

fn text(value: &JsonValue) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// A graph node as the wire type.
///
/// Named by its title, described by its table name (when it has one) and its description, and
/// placed by the node's type.
fn entity_of(node: &JsonValue) -> Option<DataCatalogEntity> {
    let id = text(&node["id"])?;
    let table = text(&node["table_name"]);
    let description = match (table.clone(), text(&node["description"])) {
        (Some(table), Some(description)) => Some(format!("{table}\n{description}")),
        (table, description) => table.or(description),
    };
    Some(DataCatalogEntity {
        name: text(&node["title"]).or(table).unwrap_or_else(|| id.clone()),
        urn: id,
        kind: DataCatalogEntityKind::Dataset,
        platform: text(&node["type"]),
        description,
    })
}

/// Whether every word of `words` (lowercase) is in the node's id, title, table name or description.
fn matches(node: &JsonValue, words: &[String]) -> bool {
    let haystack = ["id", "title", "table_name", "description"]
        .iter()
        .filter_map(|key| node[*key].as_str())
        .collect::<Vec<_>>()
        .join("\n")
        .to_lowercase();
    words.iter().all(|word| haystack.contains(word.as_str()))
}

/// One page of the nodes `query` matches, in the graph's own order.
fn search_page(graph: &JsonValue, query: &str, start: u32, count: u32) -> DataCatalogSearchPage {
    let words: Vec<String> = query
        .split_whitespace()
        .filter(|word| *word != "*")
        .map(str::to_lowercase)
        .collect();
    let found: Vec<DataCatalogEntity> = nodes(graph)
        .filter(|node| matches(node, &words))
        .filter_map(entity_of)
        .collect();
    DataCatalogSearchPage {
        total: u32::try_from(found.len()).unwrap_or(u32::MAX),
        start,
        results: found
            .into_iter()
            .skip(usize::try_from(start).unwrap_or(usize::MAX))
            .take(usize::try_from(count).unwrap_or(usize::MAX))
            .collect(),
    }
}

/// The nodes one edge before `id` and one edge after it, each once, in the graph's edge order.
fn neighbours(graph: &JsonValue, id: &str) -> (Vec<DataCatalogEntity>, Vec<DataCatalogEntity>) {
    let side = |own: &str, other: &str| {
        let mut seen: Vec<&str> = Vec::new();
        for edge in graph["edges"].as_array().into_iter().flatten() {
            if edge[own].as_str() != Some(id) {
                continue;
            }
            let found = edge[other]
                .as_str()
                .filter(|found| *found != id && !seen.contains(found));
            if let Some(found) = found {
                seen.push(found);
            }
        }
        seen.into_iter()
            .filter_map(|found| nodes(graph).find(|node| node["id"].as_str() == Some(found)))
            .filter_map(entity_of)
            .take(MAX_NEIGHBOURS)
            .collect::<Vec<_>>()
    };
    (side("to", "from"), side("from", "to"))
}

/// The columns of `table`'s current schema; none for a table the lakehouse does not have yet.
async fn columns_of(table: &str) -> Result<Vec<DataCatalogField>, ApiError> {
    // Table metadata only, so the read-only token (root ADR-0247), as the operations map reads it.
    let catalog = LakehouseCatalogConfig::reader_from_env()
        .map_err(|_| ApiError::Unavailable("the lakehouse catalog is not configured".to_owned()))
        .and_then(|config| {
            IcebergRestCatalog::new(config).map_err(|error| {
                tracing::warn!(%error, "lakehouse catalog client could not be built");
                ApiError::Unavailable("the lakehouse catalog is not configured".to_owned())
            })
        })?;
    let columns = catalog.load_current_columns(table).await.map_err(|error| {
        tracing::warn!(%error, table, "lakehouse catalog unreachable");
        ApiError::Unavailable("the lakehouse catalog is not reachable".to_owned())
    })?;
    Ok(columns
        .unwrap_or_default()
        .into_iter()
        .map(|column| DataCatalogField {
            path: column.name,
            native_type: Some(column.data_type),
            description: column.doc,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn graph() -> JsonValue {
        json!({
            "nodes": [
                {"id": "synthetic-source", "type": "source_group", "title": "합성 원천",
                 "description": "합성 원천 파일"},
                {"id": "silver-synthetic", "type": "silver_table", "title": "합성 실버",
                 "description": "원천을 그대로 쌓는다", "table_name": "silver.synthetic"},
                {"id": "gold-synthetic", "type": "gold_table", "title": "합성 골드",
                 "table_name": "gold.synthetic"},
                {"id": "serving-synthetic", "type": "serving_surface", "title": "합성 서빙"},
                {"type": "silver_table", "title": "id 없는 노드"}
            ],
            "edges": [
                {"id": "a", "from": "synthetic-source", "to": "silver-synthetic"},
                {"id": "b", "from": "silver-synthetic", "to": "gold-synthetic"},
                {"id": "c", "from": "silver-synthetic", "to": "gold-synthetic"},
                {"id": "d", "from": "gold-synthetic", "to": "serving-synthetic"},
                {"id": "e", "from": "silver-synthetic", "to": "not-a-node"}
            ]
        })
    }

    fn ids(entities: &[DataCatalogEntity]) -> Vec<&str> {
        entities.iter().map(|entity| entity.urn.as_str()).collect()
    }

    #[test]
    fn an_entity_is_a_graph_node_named_by_its_title_with_its_table_in_the_description() {
        let graph = graph();
        let silver = entity_of(&graph["nodes"][1]);
        assert_eq!(
            silver,
            Some(DataCatalogEntity {
                urn: "silver-synthetic".to_owned(),
                kind: DataCatalogEntityKind::Dataset,
                name: "합성 실버".to_owned(),
                platform: Some("silver_table".to_owned()),
                description: Some("silver.synthetic\n원천을 그대로 쌓는다".to_owned()),
            })
        );
        let gold = entity_of(&graph["nodes"][2]);
        assert_eq!(
            gold.and_then(|entity| entity.description),
            Some("gold.synthetic".to_owned())
        );
        assert_eq!(entity_of(&graph["nodes"][4]), None, "a node without an id");
    }

    #[test]
    fn search_takes_every_word_in_any_case_and_pages_in_graph_order() {
        let graph = graph();
        let all = search_page(&graph, "*", 0, 50);
        assert_eq!(all.total, 4);
        assert_eq!(
            ids(&all.results),
            [
                "synthetic-source",
                "silver-synthetic",
                "gold-synthetic",
                "serving-synthetic"
            ]
        );
        assert_eq!(search_page(&graph, "  ", 0, 50).total, 4);
        // A table name, in another case; both words must be there.
        assert_eq!(
            ids(&search_page(&graph, "SILVER.Synthetic", 0, 50).results),
            ["silver-synthetic"]
        );
        assert_eq!(
            ids(&search_page(&graph, "합성 골드", 0, 50).results),
            ["gold-synthetic"]
        );
        assert_eq!(search_page(&graph, "합성 없는말", 0, 50).total, 0);
        let second = search_page(&graph, "", 1, 2);
        assert_eq!((second.total, second.start), (4, 1));
        assert_eq!(ids(&second.results), ["silver-synthetic", "gold-synthetic"]);
        assert!(search_page(&graph, "", 9, 2).results.is_empty());
    }

    #[test]
    fn neighbours_are_one_edge_away_each_once_and_only_nodes_of_the_graph() {
        let graph = graph();
        let (upstream, downstream) = neighbours(&graph, "silver-synthetic");
        assert_eq!(ids(&upstream), ["synthetic-source"]);
        assert_eq!(ids(&downstream), ["gold-synthetic"]);
        let (upstream, downstream) = neighbours(&graph, "synthetic-source");
        assert!(upstream.is_empty());
        assert_eq!(ids(&downstream), ["silver-synthetic"]);
    }
}
