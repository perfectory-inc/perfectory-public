//! Wire contract of the staff data catalog (root ADR-0117, ADR-0119).
//!
//! Staff read the data catalog through Foundation; the catalog's own store (`DataHub`) is behind
//! this API and never reached by a browser. The staff console (Dawneer) uses these types by path.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// What kind of thing a catalog entity is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataCatalogEntityKind {
    /// A table, file set or other body of data.
    Dataset,
    /// A job that reads datasets and writes others.
    Job,
}

/// One entity as a list, a search result or a graph node shows it.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogEntity {
    /// The catalog's identifier; pass it back unchanged to read the entity or its neighbours.
    pub urn: String,
    /// Dataset or job.
    pub kind: DataCatalogEntityKind,
    /// The name people know it by.
    pub name: String,
    /// The system it lives in, when the catalog records one.
    pub platform: Option<String>,
    /// What it is, in the words its owner wrote.
    pub description: Option<String>,
}

/// One page of search results.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogSearchPage {
    /// Entities the query matches in total, not just on this page.
    pub total: u32,
    /// Zero-based offset of the first result on this page.
    pub start: u32,
    /// Results on this page, best match first.
    pub results: Vec<DataCatalogEntity>,
}

/// One column of a dataset.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogField {
    /// Column path.
    pub path: String,
    /// Type as the catalog records it.
    pub native_type: Option<String>,
    /// What the column holds.
    pub description: Option<String>,
}

/// An entity and the entities one step before and after it.
///
/// A dataset's neighbours are the jobs that write and read it; a job's neighbours are the datasets
/// it reads and writes. A screen walks the graph by asking for a neighbour's neighbours.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogNeighbourhood {
    /// The entity asked about.
    pub entity: DataCatalogEntity,
    /// Its columns, for a dataset whose schema the catalog holds.
    pub fields: Vec<DataCatalogField>,
    /// What it comes from.
    pub upstream: Vec<DataCatalogEntity>,
    /// What comes from it.
    pub downstream: Vec<DataCatalogEntity>,
}
