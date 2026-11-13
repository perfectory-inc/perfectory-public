//! `POST /internal/real-estate-brokers/verify` — is this one person a registered broker (root
//! ADR-0550 §4).
//!
//! Gongzzang's sign-up asks with a service identity. The answer names at most the one person
//! asked about; nothing lists people, and a name alone is refused. The body, not the path, carries
//! the name and number, and the log records only the outcome.

use std::sync::Arc;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use catalog_domain::real_estate_broker::{BrokerMatch, BrokerVerifyQuery};
use serde::{Deserialize, Serialize};

use super::ApiError;
use crate::state::AppState;

pub(super) fn routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new().route(
        "/internal/real-estate-brokers/verify",
        super::protected_route(
            post(verify_broker),
            state,
            super::SERVICE_REAL_ESTATE_BROKER_VERIFY,
            None,
        ),
    )
}

/// The question: a name and exactly one of 자격증번호 and 등록번호.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VerifyBrokerRequest {
    name: String,
    license_no: Option<String>,
    registration_no: Option<String>,
}

/// The one person found, by the fields a sign-up needs.
#[derive(Debug, Serialize)]
struct VerifiedBroker {
    member_kind_name: Option<String>,
    position_name: Option<String>,
    registration_no: String,
    office_name: Option<String>,
    office_status_name: Option<String>,
    data_reference_date: Option<String>,
}

impl From<BrokerMatch> for VerifiedBroker {
    fn from(found: BrokerMatch) -> Self {
        Self {
            member_kind_name: found.member_kind_name,
            position_name: found.position_name,
            registration_no: found.registration_no,
            office_name: found.office_name,
            office_status_name: found.office_status_name,
            data_reference_date: found.data_reference_date,
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct VerifyBrokerResponse {
    /// `match` or `no_match`.
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    broker: Option<VerifiedBroker>,
    /// The sources that answered: `registry`, then `vworld_live` when the live register was asked
    /// and answered. A `no_match` without `vworld_live` is one the live register did not confirm.
    checked: Vec<&'static str>,
}

async fn verify_broker(
    State(state): State<Arc<AppState>>,
    Json(body): Json<VerifyBrokerRequest>,
) -> Result<Json<VerifyBrokerResponse>, ApiError> {
    let query = BrokerVerifyQuery::parse(
        &body.name,
        body.license_no.as_deref(),
        body.registration_no.as_deref(),
    )
    .map_err(|error| ApiError::Unprocessable(error.to_string()))?;
    let answer = state
        .verify_real_estate_broker
        .execute(&query)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "real estate broker roster unavailable");
            ApiError::Unavailable("the broker roster is unavailable".to_owned())
        })?;
    let checked = answer
        .checked
        .iter()
        .map(|source| source.as_str())
        .collect();
    let result = if answer.found.is_some() {
        "match"
    } else {
        "no_match"
    };
    tracing::info!(result, checked = ?checked, "real estate broker verified");
    Ok(Json(VerifyBrokerResponse {
        result,
        broker: answer.found.map(VerifiedBroker::from),
        checked,
    }))
}
