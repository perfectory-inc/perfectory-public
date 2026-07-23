//! PostgreSQL service-principal and capability reads.

use async_trait::async_trait;
use authorization_domain::Permission;
use identity_contracts::PrincipalId;
use service_identity_domain::{ServiceIdentityError, ValidatedServicePrincipal};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const SERVICE_PRINCIPAL_CAPABILITIES_SQL: &str =
    "SELECT principal.id AS principal_id, capability.capability
     FROM identity.service_principal AS principal
     LEFT JOIN identity.service_capability_grant AS capability
       ON capability.service_principal_id = principal.id
     WHERE principal.zitadel_subject = $1
     ORDER BY capability.capability";

/// Reads Identity-owned service principals and capability grants.
#[async_trait]
pub trait ServicePrincipalReader: Send + Sync {
    /// Resolves one verified Zitadel subject to its principal and capabilities.
    ///
    /// # Errors
    /// Returns [`ServiceIdentityError`] when Identity persistence cannot be read.
    async fn read_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<ValidatedServicePrincipal>, ServiceIdentityError>;
}

/// `PostgreSQL` service-principal and capability reader.
pub struct PgServicePrincipalCapabilityReader {
    pool: PgPool,
}

impl PgServicePrincipalCapabilityReader {
    /// Creates a reader backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ServicePrincipalReader for PgServicePrincipalCapabilityReader {
    async fn read_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<ValidatedServicePrincipal>, ServiceIdentityError> {
        let rows = sqlx::query(SERVICE_PRINCIPAL_CAPABILITIES_SQL)
            .bind(subject)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let values = rows
            .iter()
            .map(row_to_principal_capability)
            .collect::<Result<Vec<_>, _>>()?;
        map_principal_rows(values)
    }
}

struct ServicePrincipalCapabilityRow {
    principal_id: Uuid,
    capability: Option<String>,
}

fn map_principal_rows(
    rows: Vec<ServicePrincipalCapabilityRow>,
) -> Result<Option<ValidatedServicePrincipal>, ServiceIdentityError> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let principal_id = first.principal_id;
    if rows.iter().any(|row| row.principal_id != principal_id) {
        return Err(ServiceIdentityError::Infrastructure(
            "service principal rows contain mixed identifiers".to_owned(),
        ));
    }
    let mut capabilities = rows
        .into_iter()
        .filter_map(|row| row.capability)
        .map(|capability| {
            Permission::parse(capability)
                .map_err(|error| ServiceIdentityError::Infrastructure(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    capabilities.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    capabilities.dedup();
    Ok(Some(ValidatedServicePrincipal {
        principal_id: PrincipalId::new(principal_id),
        capabilities,
    }))
}

fn row_to_principal_capability(
    row: &PgRow,
) -> Result<ServicePrincipalCapabilityRow, ServiceIdentityError> {
    Ok(ServicePrincipalCapabilityRow {
        principal_id: row.try_get("principal_id").map_err(map_sqlx)?,
        capability: row.try_get("capability").map_err(map_sqlx)?,
    })
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> ServiceIdentityError {
    ServiceIdentityError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        map_principal_rows, ServicePrincipalCapabilityRow, SERVICE_PRINCIPAL_CAPABILITIES_SQL,
    };
    use identity_contracts::PrincipalId;
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn maps_and_deduplicates_identity_owned_capability_rows() -> Result<(), Box<dyn Error>> {
        let principal_id = Uuid::parse_str("018f30c0-7b5a-7cc0-8c9d-1f3d12f85350")?;
        let principal = map_principal_rows(vec![
            ServicePrincipalCapabilityRow {
                principal_id,
                capability: Some("catalog:read".to_owned()),
            },
            ServicePrincipalCapabilityRow {
                principal_id,
                capability: Some("catalog:read".to_owned()),
            },
            ServicePrincipalCapabilityRow {
                principal_id,
                capability: Some("lakehouse:write".to_owned()),
            },
        ])?
        .ok_or("principal")?;

        assert_eq!(principal.principal_id, PrincipalId::new(principal_id));
        assert_eq!(principal.capabilities.len(), 2);
        assert_eq!(principal.capabilities[0].as_str(), "catalog:read");
        assert_eq!(principal.capabilities[1].as_str(), "lakehouse:write");
        Ok(())
    }

    #[test]
    fn empty_rows_mean_unknown_principal() -> Result<(), Box<dyn Error>> {
        assert!(map_principal_rows(Vec::new())?.is_none());
        Ok(())
    }

    #[test]
    fn reader_uses_identity_tables_without_static_credentials() {
        assert!(SERVICE_PRINCIPAL_CAPABILITIES_SQL.contains("FROM identity.service_principal"));
        assert!(
            SERVICE_PRINCIPAL_CAPABILITIES_SQL.contains("JOIN identity.service_capability_grant")
        );
        assert!(!SERVICE_PRINCIPAL_CAPABILITIES_SQL.contains("secret"));
        assert!(!SERVICE_PRINCIPAL_CAPABILITIES_SQL.contains("token"));
    }
}
