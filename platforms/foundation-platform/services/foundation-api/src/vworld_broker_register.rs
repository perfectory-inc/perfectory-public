//! VWorld's live 중개업자 register, `getEBBrokerInfo`, asked on a roster miss (root ADR-0550 §5).
//!
//! The daily national file the roster is loaded from trails the register by a day or two. The
//! API filters by 중개업자명 (`brkrNm`) and 등록번호 (`jurirno`) but returns the 자격증번호
//! (`crqfcNo`) only in its rows, so a licence question asks by name and matches the rows.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use catalog_application::verify_real_estate_broker::{BrokerRegister, BrokerRegisterError};
use catalog_domain::real_estate_broker::{
    name_key, number_key, BrokerCredential, BrokerMatch, BrokerVerifyQuery,
};
use reqwest::{Client, Url};
use serde_json::Value;

/// Where the VWorld credential names are decided (canonical first, then deprecated aliases); the
/// same contract the collection adapters read, so this file names none of them.
const NAMING_CONTRACT: &str =
    include_str!("../../../config/environment-variable-naming.contract.json");
const ENDPOINT: &str = "https://api.vworld.kr/ned/data/getEBBrokerInfo";
/// The provider's largest page.
const PAGE_ROWS: usize = 1_000;
/// A name with more people than this is answered from the pages read (root ADR-0550 §5).
const MAX_PAGES: usize = 3;
/// The whole live question, every page included.
const BUDGET: Duration = Duration::from_secs(3);

/// The live register, present only where the deployment holds a VWorld key.
pub struct VworldBrokerRegister {
    client: Client,
    api_key: String,
    domain: Option<String>,
}

impl VworldBrokerRegister {
    /// The register from the environment, or none when no key is set.
    ///
    /// # Errors
    /// The HTTP client cannot be built.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let Some(api_key) = credential("api_key")? else {
            return Ok(None);
        };
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(BUDGET)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Some(Self {
            client,
            api_key,
            domain: credential("domain")?,
        }))
    }

    fn page_url(&self, query: &BrokerVerifyQuery, page: usize) -> Result<Url, BrokerRegisterError> {
        let mut url =
            Url::parse(ENDPOINT).map_err(|error| BrokerRegisterError(error.to_string()))?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs
                .append_pair("key", &self.api_key)
                .append_pair("format", "json")
                .append_pair("numOfRows", &PAGE_ROWS.to_string())
                .append_pair("pageNo", &page.to_string())
                .append_pair("brkrNm", &query.name_key);
            if let Some(domain) = &self.domain {
                pairs.append_pair("domain", domain);
            }
            if let BrokerCredential::Registration(registration) = &query.credential {
                pairs.append_pair("jurirno", registration);
            }
        }
        Ok(url)
    }
}

#[async_trait]
impl BrokerRegister for VworldBrokerRegister {
    async fn find(
        &self,
        query: &BrokerVerifyQuery,
    ) -> Result<Option<BrokerMatch>, BrokerRegisterError> {
        let started = Instant::now();
        for page in 1..=MAX_PAGES {
            let remaining = BUDGET
                .checked_sub(started.elapsed())
                .ok_or_else(|| BrokerRegisterError("the live budget ran out".to_owned()))?;
            let response = self
                .client
                .get(self.page_url(query, page)?)
                .timeout(remaining)
                .send()
                .await
                // The URL carries the key: never the error's URL, only its kind.
                .map_err(|error| {
                    BrokerRegisterError(format!("request failed (timeout: {})", error.is_timeout()))
                })?;
            if !response.status().is_success() {
                return Err(BrokerRegisterError(format!("HTTP {}", response.status())));
            }
            let body: Value = response
                .json()
                .await
                .map_err(|_| BrokerRegisterError("the answer is not JSON".to_owned()))?;
            let rows = page_rows(&body)?;
            if let Some(found) = rows.iter().find_map(|row| matching(row, query)) {
                return Ok(Some(found));
            }
            if rows.len() < PAGE_ROWS {
                return Ok(None);
            }
        }
        Ok(None)
    }
}

/// The value of one VWorld credential (`api_key`, `domain`) by the naming contract: its canonical
/// name, else a deprecated alias, whose name (never its value) is warned about.
fn credential(which: &str) -> anyhow::Result<Option<String>> {
    let contract: Value = serde_json::from_str(NAMING_CONTRACT)?;
    let names = &contract["compatibility_migrations"]["foundation-vworld-credentials"]
        ["credentials"][which];
    let canonical = names["canonical"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("the naming contract has no canonical VWorld {which}"))?;
    let aliases = names["deprecated_aliases"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let read = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    };
    if let Some(value) = read(canonical) {
        return Ok(Some(value));
    }
    for alias in aliases.iter().filter_map(Value::as_str) {
        if let Some(value) = read(alias) {
            tracing::warn!(
                alias,
                canonical,
                "a deprecated VWorld credential name supplied the value"
            );
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// The rows of one page: the one array inside the answer's one top-level object, as every VWorld
/// NED operation answers (`{"<list>": {"<list>": [..], "totalCount": ..}}`). An answer with no rows
/// carries no array.
fn page_rows(body: &Value) -> Result<Vec<&serde_json::Map<String, Value>>, BrokerRegisterError> {
    let Some(envelope) = body.as_object().and_then(|object| {
        (object.len() == 1)
            .then(|| object.values().next())
            .flatten()
    }) else {
        return Err(BrokerRegisterError(
            "the answer has no single list envelope".to_owned(),
        ));
    };
    let Some(envelope) = envelope.as_object() else {
        return Err(BrokerRegisterError(
            "the list envelope is not an object".to_owned(),
        ));
    };
    if let Some(error) = envelope
        .get("error")
        .and_then(Value::as_str)
        .filter(|error| !error.is_empty())
    {
        return Err(BrokerRegisterError(format!("provider error {error}")));
    }
    match envelope.values().find_map(Value::as_array) {
        Some(rows) => Ok(rows.iter().filter_map(Value::as_object).collect()),
        // A miss is a list that says it holds nothing; any other shape is not an answer.
        None if envelope.contains_key("totalCount") => Ok(Vec::new()),
        None => Err(BrokerRegisterError(
            "the answer is neither rows nor an empty list".to_owned(),
        )),
    }
}

fn text(row: &serde_json::Map<String, Value>, field: &str) -> Option<String> {
    row.get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn matching(
    row: &serde_json::Map<String, Value>,
    query: &BrokerVerifyQuery,
) -> Option<BrokerMatch> {
    if text(row, "brkrNm").as_deref().and_then(name_key)? != query.name_key {
        return None;
    }
    let (field, wanted) = match &query.credential {
        BrokerCredential::License(license) => ("crqfcNo", license),
        BrokerCredential::Registration(registration) => ("jurirno", registration),
    };
    if text(row, field).as_deref().and_then(number_key)? != *wanted {
        return None;
    }
    Some(BrokerMatch {
        member_kind_name: text(row, "brkrAsortCodeNm"),
        position_name: text(row, "ofcpsSeCodeNm"),
        registration_no: text(row, "jurirno")?,
        office_name: text(row, "bsnmCmpnm"),
        office_status_name: None,
        data_reference_date: text(row, "lastUpdtDt"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query(credential: BrokerCredential) -> BrokerVerifyQuery {
        BrokerVerifyQuery {
            name_key: "홍길동".to_owned(),
            credential,
        }
    }

    fn body() -> Value {
        json!({"EDBrokers": {"field": [
            {"brkrNm": "홍길동", "crqfcNo": "99999-0001", "jurirno": "99999-2026-00001",
             "brkrAsortCodeNm": "공인중개사", "ofcpsSeCodeNm": "대표", "bsnmCmpnm": "합성중개"},
            {"brkrNm": "홍 길동", "crqfcNo": "99999-0002", "jurirno": "99999-2026-00002"}
        ], "totalCount": "2", "pageNo": "1", "numOfRows": "1000"}})
    }

    #[test]
    fn a_row_matches_by_name_and_the_asked_credential() -> Result<(), BrokerRegisterError> {
        let body = body();
        let rows = page_rows(&body)?;
        assert_eq!(rows.len(), 2);
        let license = query(BrokerCredential::License("999990001".to_owned()));
        let found = rows.iter().find_map(|row| matching(row, &license));
        assert_eq!(
            found.as_ref().map(|found| found.registration_no.as_str()),
            Some("99999-2026-00001")
        );
        assert_eq!(
            found.and_then(|found| found.member_kind_name).as_deref(),
            Some("공인중개사")
        );

        let registration = query(BrokerCredential::Registration("99999202600002".to_owned()));
        assert!(rows
            .iter()
            .find_map(|row| matching(row, &registration))
            .is_some());
        let wrong = query(BrokerCredential::License("999990002x".to_owned()));
        assert!(rows.iter().find_map(|row| matching(row, &wrong)).is_none());
        Ok(())
    }

    #[test]
    fn an_empty_page_has_no_rows_and_a_provider_error_is_refused() -> Result<(), BrokerRegisterError>
    {
        assert!(page_rows(&json!({"EDBrokers": {"totalCount": "0"}}))?.is_empty());
        assert!(page_rows(&json!({"EDBrokers": {"error": "INVALID_KEY"}})).is_err());
        assert!(page_rows(&json!([])).is_err());
        Ok(())
    }
}
