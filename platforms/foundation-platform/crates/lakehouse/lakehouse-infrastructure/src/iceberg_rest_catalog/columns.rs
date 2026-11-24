//! The columns of a table's current schema, read from the same table-loading answer.

use lakehouse_domain::LakehouseError;
use serde::Deserialize;

use super::{IcebergRestCatalog, IcebergTableMetadata};

/// One column of a table's current schema, as the catalog's table metadata states it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergColumn {
    /// The column's name.
    pub name: String,
    /// Its Iceberg type: a primitive's name (`string`, `long`, `timestamptz`, `decimal(18,2)`), or
    /// `struct`, `list` or `map` for a nested one.
    pub data_type: String,
    /// What the writer documented about it, when it did.
    pub doc: Option<String>,
}

impl IcebergRestCatalog {
    /// The columns of the table's current schema, in the schema's order.
    ///
    /// Returns `Ok(None)` when the table does not exist.
    ///
    /// # Errors
    ///
    /// Returns `LakehouseError` when the catalog cannot be reached.
    pub async fn load_current_columns(
        &self,
        table_name: &str,
    ) -> Result<Option<Vec<IcebergColumn>>, LakehouseError> {
        Ok(self
            .load_table_response(table_name)
            .await?
            .map(|payload| current_columns(&payload.metadata)))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct IcebergSchemaMetadata {
    #[serde(rename = "schema-id", default)]
    schema_id: Option<i64>,
    #[serde(default)]
    fields: Vec<IcebergFieldMetadata>,
}

#[derive(Debug, Deserialize)]
struct IcebergFieldMetadata {
    name: String,
    /// A primitive's name, or an object for a struct, list or map.
    #[serde(rename = "type", default)]
    kind: serde_json::Value,
    #[serde(default)]
    doc: Option<String>,
}

/// The columns of the schema `current-schema-id` names; of the newest schema when the metadata
/// names none.
fn current_columns(metadata: &IcebergTableMetadata) -> Vec<IcebergColumn> {
    let current = metadata
        .current_schema_id
        .and_then(|id| {
            metadata
                .schemas
                .iter()
                .find(|schema| schema.schema_id == Some(id))
        })
        .or_else(|| metadata.schemas.last());
    current
        .map(|schema| {
            schema
                .fields
                .iter()
                .map(|field| IcebergColumn {
                    name: field.name.clone(),
                    data_type: match &field.kind {
                        serde_json::Value::String(primitive) => primitive.clone(),
                        nested => nested
                            .get("type")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned(),
                    },
                    doc: field
                        .doc
                        .as_deref()
                        .map(str::trim)
                        .filter(|doc| !doc.is_empty())
                        .map(ToOwned::to_owned),
                })
                .collect()
        })
        .unwrap_or_default()
}
