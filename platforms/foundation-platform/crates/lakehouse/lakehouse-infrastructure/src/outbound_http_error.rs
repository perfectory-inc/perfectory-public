//! Outbound HTTP error mapping for Lakehouse adapters.

use lakehouse_domain::LakehouseError;
use outbound_http_infrastructure::OutboundHttpError;

/// Translates provider-neutral HTTP failures at the Lakehouse adapter boundary.
#[must_use]
pub fn into_lakehouse_error(error: OutboundHttpError) -> LakehouseError {
    LakehouseError::Upstream(error.into_message())
}
