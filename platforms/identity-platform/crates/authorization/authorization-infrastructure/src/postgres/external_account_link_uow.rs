//! Atomic external-account link persistence (root ADR-0569).
//!
//! Records are only appended. The latest record of a link `(staff_id, account, valid_from)` is its
//! state. One account belongs to at most one staff member at any instant: a new link whose interval
//! meets any link's interval is refused, under a per-account transaction lock.

use async_trait::async_trait;
use authorization_application::ports::{
    ExternalAccountLinkPersistenceError, ExternalAccountLinkUnitOfWork,
};
use chrono::{DateTime, Utc};
use identity_contracts::{IdentityEventV1, PrincipalId, StaffExternalAccountChangedV1};
use identity_shared_kernel::StaffId;
use sqlx::{PgPool, Postgres, Row, Transaction};
use staff_identity_domain::{ExternalAccount, ExternalAccountLink};
use uuid::Uuid;

use super::is_foreign_key_constraint_violation;

const TARGET_STAFF_CONSTRAINT: &str = "identity_staff_external_account_staff_fkey";
const LOCK_ACCOUNT_SQL: &str = "SELECT pg_advisory_xact_lock(
         hashtextextended('identity.staff_external_account:' || $1 || ':' || $2, 0))";
/// Every link of the account, each at its latest record.
const LINK_STATES_SQL: &str =
    "SELECT DISTINCT ON (staff_id, valid_from) staff_id, valid_from, valid_to
     FROM identity.staff_external_account
     WHERE provider = $1 AND external_id = $2
     ORDER BY staff_id, valid_from, record_seq DESC";
const INSERT_RECORD_SQL: &str = "INSERT INTO identity.staff_external_account
         (staff_id, provider, external_id, valid_from, valid_to, linked_by)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING recorded_at";
const INSERT_OUTBOX_SQL: &str =
    "INSERT INTO identity.outbox_event (event_id, type, payload, occurred_at)
     VALUES ($1, $2, $3, $4)";

/// `PostgreSQL` unit of work for external-account link records and their outbox events.
pub struct PgExternalAccountLinkUnitOfWork {
    pool: PgPool,
}

impl PgExternalAccountLinkUnitOfWork {
    /// Creates the unit of work backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

struct LinkState {
    staff_id: Uuid,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
}

#[async_trait]
impl ExternalAccountLinkUnitOfWork for PgExternalAccountLinkUnitOfWork {
    async fn link(
        &self,
        staff_id: StaffId,
        account: &ExternalAccount,
        valid_from: DateTime<Utc>,
        linked_by: StaffId,
    ) -> Result<ExternalAccountLink, ExternalAccountLinkPersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        let states = lock_and_read(&mut transaction, account).await?;
        if states
            .iter()
            .any(|state| state.valid_to.is_none_or(|valid_to| valid_to > valid_from))
        {
            return Err(ExternalAccountLinkPersistenceError::AlreadyLinked);
        }
        let record = ExternalAccountLink {
            staff_id,
            account: account.clone(),
            valid_from,
            valid_to: None,
            linked_by,
        };
        append(&mut transaction, &record).await?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(record)
    }

    async fn unlink(
        &self,
        staff_id: StaffId,
        account: &ExternalAccount,
        unlinked_by: StaffId,
    ) -> Result<ExternalAccountLink, ExternalAccountLinkPersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        let states = lock_and_read(&mut transaction, account).await?;
        let holding = states
            .into_iter()
            .find(|state| state.staff_id == staff_id.as_uuid() && state.valid_to.is_none())
            .ok_or(ExternalAccountLinkPersistenceError::NotLinked)?;
        let record = ExternalAccountLink {
            staff_id,
            account: account.clone(),
            valid_from: holding.valid_from,
            valid_to: Some(Utc::now().max(holding.valid_from)),
            linked_by: unlinked_by,
        };
        append(&mut transaction, &record).await?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(record)
    }
}

async fn lock_and_read(
    transaction: &mut Transaction<'_, Postgres>,
    account: &ExternalAccount,
) -> Result<Vec<LinkState>, ExternalAccountLinkPersistenceError> {
    sqlx::query(LOCK_ACCOUNT_SQL)
        .bind(account.provider().as_str())
        .bind(account.external_id())
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx)?;
    let rows = sqlx::query(LINK_STATES_SQL)
        .bind(account.provider().as_str())
        .bind(account.external_id())
        .fetch_all(&mut **transaction)
        .await
        .map_err(map_sqlx)?;
    rows.iter()
        .map(|row| {
            Ok(LinkState {
                staff_id: row.try_get("staff_id").map_err(map_sqlx)?,
                valid_from: row.try_get("valid_from").map_err(map_sqlx)?,
                valid_to: row.try_get("valid_to").map_err(map_sqlx)?,
            })
        })
        .collect()
}

async fn append(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ExternalAccountLink,
) -> Result<(), ExternalAccountLinkPersistenceError> {
    let recorded_at: DateTime<Utc> = sqlx::query_scalar(INSERT_RECORD_SQL)
        .bind(record.staff_id.as_uuid())
        .bind(record.account.provider().as_str())
        .bind(record.account.external_id())
        .bind(record.valid_from)
        .bind(record.valid_to)
        .bind(record.linked_by.as_uuid())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| map_insert_error(error, record.staff_id))?;
    let payload = StaffExternalAccountChangedV1 {
        schema_version: 1,
        staff_id: PrincipalId::new(record.staff_id.as_uuid()),
        provider: record.account.provider().as_str().to_owned(),
        external_id: record.account.external_id().to_owned(),
        valid_from: record.valid_from,
        valid_to: record.valid_to,
        changed_by: PrincipalId::new(record.linked_by.as_uuid()),
    };
    let event = if record.valid_to.is_none() {
        IdentityEventV1::StaffExternalAccountLinked(payload)
    } else {
        IdentityEventV1::StaffExternalAccountUnlinked(payload)
    };
    let payload = serde_json::to_value(&event)
        .map_err(|error| ExternalAccountLinkPersistenceError::Infrastructure(error.to_string()))?;
    let event_type = payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            ExternalAccountLinkPersistenceError::Infrastructure("event type is missing".to_owned())
        })?
        .to_owned();
    sqlx::query(INSERT_OUTBOX_SQL)
        .bind(Uuid::now_v7())
        .bind(event_type)
        .bind(payload)
        .bind(recorded_at)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx)?;
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn map_insert_error(error: sqlx::Error, staff_id: StaffId) -> ExternalAccountLinkPersistenceError {
    if is_foreign_key_constraint_violation(&error, TARGET_STAFF_CONSTRAINT) {
        return ExternalAccountLinkPersistenceError::StaffNotFound(staff_id.as_uuid().to_string());
    }
    map_sqlx(error)
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> ExternalAccountLinkPersistenceError {
    ExternalAccountLinkPersistenceError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{map_insert_error, TARGET_STAFF_CONSTRAINT};
    use authorization_application::ports::ExternalAccountLinkPersistenceError;
    use identity_shared_kernel::StaffId;
    use uuid::Uuid;

    #[test]
    fn a_missing_target_staff_is_not_found_and_other_failures_stay_opaque() {
        let staff_id = StaffId::new(Uuid::from_u128(7));
        assert!(matches!(
            map_insert_error(
                super::super::test_database_error("23503", TARGET_STAFF_CONSTRAINT),
                staff_id
            ),
            ExternalAccountLinkPersistenceError::StaffNotFound(_)
        ));
        assert!(matches!(
            map_insert_error(
                super::super::test_database_error("23503", "other_fkey"),
                staff_id
            ),
            ExternalAccountLinkPersistenceError::Infrastructure(_)
        ));
    }
}
