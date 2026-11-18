//! The Postgres 부동산중개업 roster, `catalog.real_estate_broker_member` and `_office` (root
//! ADR-0550 §1, §4).

use async_trait::async_trait;
use catalog_application::verify_real_estate_broker::{BrokerRegister, BrokerRegisterError};
use catalog_domain::real_estate_broker::{BrokerCredential, BrokerMatch, BrokerVerifyQuery};
use sqlx::PgPool;

/// One person by name and credential among the current roster rows, at most one row back.
///
/// When the person is on several offices' rosters (a move the file shows on both for a day), the
/// most recently loaded office answers.
/// `{column}` is `license_key` or `registration_key`: one statement each, so each uses its index.
macro_rules! find_sql {
    ($column:literal) => {
        concat!(
            "SELECT m.member_kind_name, m.position_name, m.registration_no, o.office_name,
           o.status_name, m.data_reference_date
    FROM catalog.real_estate_broker_member m
    LEFT JOIN catalog.real_estate_broker_office o
      ON o.sigungu_code = m.sigungu_code AND o.registration_no = m.registration_no
     AND o.retired_by_load_id IS NULL
    WHERE m.retired_by_load_id IS NULL AND m.name_key = $1 AND m.",
            $column,
            " = $2
    ORDER BY m.loaded_by_load_id DESC, m.id DESC
    LIMIT 1"
        )
    };
}
const FIND_BY_LICENSE_SQL: &str = find_sql!("license_key");
const FIND_BY_REGISTRATION_SQL: &str = find_sql!("registration_key");

type FoundRow = (
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Reads the current roster rows.
pub struct PgRealEstateBrokerRoster {
    pool: PgPool,
}

impl PgRealEstateBrokerRoster {
    /// Creates the roster over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BrokerRegister for PgRealEstateBrokerRoster {
    async fn find(
        &self,
        query: &BrokerVerifyQuery,
    ) -> Result<Option<BrokerMatch>, BrokerRegisterError> {
        let (sql, number) = match &query.credential {
            BrokerCredential::License(number) => (FIND_BY_LICENSE_SQL, number),
            BrokerCredential::Registration(number) => (FIND_BY_REGISTRATION_SQL, number),
        };
        let row: Option<FoundRow> = sqlx::query_as(sql)
            .bind(&query.name_key)
            .bind(number)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| BrokerRegisterError(error.to_string()))?;
        Ok(row.map(
            |(
                member_kind_name,
                position_name,
                registration_no,
                office_name,
                office_status_name,
                data_reference_date,
            )| BrokerMatch {
                member_kind_name,
                position_name,
                registration_no,
                office_name,
                office_status_name,
                data_reference_date,
            },
        ))
    }
}
