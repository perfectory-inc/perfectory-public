//! 법원경매 공통코드, asked once a run (root ADR-0540 §1, §5).
//!
//! The site's screens name the codes its documents carry from one endpoint (`sccdo.js
//! loadIntgCdLst`); the run asks it once, after a list pass that was not cut short, for every group
//! the contract's `common_codes` lists, and lands the answer as its own Bronze object. The Silver
//! load turns it into `silver.court_auction_code`; no name is kept in this repository.

use std::collections::BTreeMap;

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use collection_application::{ask, Asked, SiteMethod, SiteRequest};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use super::{may_ask, Asking, Search, SourceContract, Stop};

/// One code group the run asks: `system[-owner]-group` on the site's side.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CodeGroup {
    pub(crate) system: String,
    /// The group's owner on the site's side; the system unless given.
    #[serde(default)]
    pub(crate) owner: Option<String>,
    pub(crate) group: String,
    /// The Silver columns (`table.column`) whose codes this group names.
    #[allow(dead_code)]
    pub(crate) names: Vec<String>,
}

/// The contract's `common_codes`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CommonCodes {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    pub(crate) pace_group: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) request_field: String,
    pub(crate) result_pointer: String,
    pub(crate) groups: Vec<CodeGroup>,
}

impl CommonCodes {
    /// The codes as the Bronze persist step names a search: its dataset, operation and path.
    pub(crate) fn as_search(&self) -> Search {
        Search {
            dataset_slug: self.dataset_slug.clone(),
            operation: self.operation.clone(),
            path: self.path.clone(),
            page_size: 0,
            pace_group: self.pace_group.clone(),
            headers: self.headers.clone(),
            conditions: Vec::new(),
            date_window: None,
        }
    }

    /// The groups as the site's screen asks them (`sccdo.js`): the groups of one system and owner
    /// joined by `&` after `system[-owner]-`, the owners joined by `,`, in the contract's order of
    /// first appearance.
    pub(crate) fn request_value(&self) -> String {
        let mut owners: Vec<(String, String, Vec<&str>)> = Vec::new();
        for group in &self.groups {
            let owner = group.owner.clone().unwrap_or_else(|| group.system.clone());
            match owners
                .iter_mut()
                .find(|(system, known, _)| *system == group.system && *known == owner)
            {
                Some((_, _, groups)) => groups.push(&group.group),
                None => owners.push((group.system.clone(), owner, vec![&group.group])),
            }
        }
        owners
            .into_iter()
            .map(|(system, owner, groups)| {
                let groups = groups.join("&");
                if system == owner {
                    format!("{system}-{groups}")
                } else {
                    format!("{system}-{owner}-{groups}")
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn request_body(&self) -> JsonValue {
        json!({ self.request_field.as_str(): self.request_value() })
    }
}

/// What the codes request came to.
#[derive(Debug, Default)]
pub(crate) struct CodesResult {
    /// The Bronze line: the request and the answer as received, or none when the run stopped first.
    pub(crate) lines: Vec<String>,
    /// How many codes the answer held, when it was read.
    pub(crate) codes: Option<usize>,
    pub(crate) failure: Option<String>,
}

/// Asks the run's 공통코드 once. A failure is recorded in the line and the report; it never fails
/// the run (the names of the last read stay current).
pub(crate) async fn collect_codes(
    contract: &SourceContract,
    asking: &Asking<'_>,
    now: impl Fn() -> DateTime<Utc>,
) -> anyhow::Result<(CodesResult, Option<Stop>)> {
    let codes = &contract.common_codes;
    let at = now();
    if !may_ask(&asking.switches, at)? {
        return Ok((CodesResult::default(), Some(Stop::WindowEnded)));
    }
    let gate = asking
        .gates
        .get(&codes.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", codes.pace_group))?;
    let body = codes.request_body();
    let request = SiteRequest {
        method: SiteMethod::Post,
        path: codes.path.clone(),
        headers: contract
            .collection
            .session
            .headers
            .iter()
            .chain(&codes.headers)
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        body: Some(body.clone()),
    };
    let asked = ask(
        asking.sessions,
        gate,
        &asking.rules,
        &asking.tally,
        &request,
        asking.deadline,
    )
    .await;
    let mut line = json!({
        "operation": codes.operation,
        "request": body,
        "requested_at_utc": at.to_rfc3339(),
    });
    let mut result = CodesResult::default();
    match asked {
        Asked::Unfinished => return Ok((result, Some(Stop::TimeBudget))),
        Asked::Data { reply, .. } => {
            line["http_status"] = json!(reply.status);
            let listed = serde_json::from_str::<JsonValue>(&reply.body)
                .ok()
                .and_then(|value| {
                    value
                        .pointer(&codes.result_pointer)
                        .and_then(JsonValue::as_array)
                        .map(Vec::len)
                });
            match listed {
                Some(count) => {
                    line["read_outcome"] = json!("read");
                    result.codes = Some(count);
                }
                None => {
                    let reason = format!("no list at {}", codes.result_pointer);
                    line["read_outcome"] = json!("failed");
                    line["failure"] = json!(reason);
                    result.failure = Some(reason);
                }
            }
            line["body"] = json!(reply.body);
        }
        Asked::Failed { reason, reply } => {
            line["http_status"] = json!(reply.as_ref().map(|reply| reply.status));
            line["read_outcome"] = json!("failed");
            line["failure"] = json!(reason);
            line["body"] = json!(reply.map(|reply| reply.body));
            result.failure = Some(reason);
        }
    }
    result.lines.push(serde_json::to_string(&line)?);
    Ok((result, None))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn the_groups_are_asked_as_the_sites_screen_asks_them() {
        let contract = super::super::source_contract().expect("the contract parses");
        let codes = &contract.common_codes;
        assert_eq!(codes.as_search().dataset_slug, "auction_common_code");
        // The shape `sccdo.js loadIntgCdLst` sends, measured 2026-11-12 (one answer, 277 codes).
        assert_eq!(
            codes.request_value(),
            "PGJ-AUCTN_GDS_USG_CD&BID_DVS_CD&AUCTN_DXDY_KND_CD&AEE_WEVL_MNPNT_TBLT_DVS_CD\
             &AEE_WEVL_MNPNT_ITM_CD&CORT_AUCTN_PIC_DVS_CD&DSPSL_GDS_LST_USG_CD&ULTMT_DVS_CD,\
             LJH-AUCTN_DXDY_RSLT_CD&AUCTN_LST_DVS_CD&AUCTN_RGLT_KND_CD&PRPND_RSLT_CD,\
             LJH-JH-ULTMT_DVS_CD&AUCTN_CS_NM_CD"
        );
        assert_eq!(
            codes.request_body(),
            json!({"intgGrpCdLst": codes.request_value()})
        );
        assert!(
            contract.collection.pace.contains_key(&codes.pace_group),
            "the codes are paced in a contract group"
        );
    }

    #[test]
    fn every_named_column_is_a_table_column() {
        let contract = super::super::source_contract().expect("the contract parses");
        for group in &contract.common_codes.groups {
            for named in &group.names {
                let (table, column) = named.rsplit_once('.').expect("table.column");
                let contract = lakehouse_domain::industrial_complex_lakehouse_contracts()
                    .iter()
                    .find(|contract| contract.table_name == table)
                    .unwrap_or_else(|| panic!("{named}: no table {table}"));
                assert!(
                    contract.columns.iter().any(|c| c.name == column),
                    "{named}: {table} has no column {column}"
                );
                assert!(column.ends_with("_code"), "{named} names a code column");
            }
        }
    }
}
