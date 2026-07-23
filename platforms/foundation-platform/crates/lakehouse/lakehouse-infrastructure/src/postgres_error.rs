//! Shared `PostgreSQL` error mapping for Lakehouse adapters.

use lakehouse_domain::LakehouseError;

#[allow(clippy::needless_pass_by_value)]
pub fn map_sqlx(error: sqlx::Error) -> LakehouseError {
    LakehouseError::Persistence(error.to_string())
}
