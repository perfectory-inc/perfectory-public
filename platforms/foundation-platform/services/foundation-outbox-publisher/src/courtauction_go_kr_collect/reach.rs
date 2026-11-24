//! Can this address ask 법원경매정보 at all? One list search, asked once (`check-courtauction-go-kr-reach`).
//!
//! The collector has only ever asked from one office address. Before its lane moves to another
//! network the question is whether the site answers that network's address, and it is asked the
//! way the collector asks: the contract's browser profile and headers, a session opened on the
//! site's own opening page, then the first page of the first condition of the item search.
//!
//! It asks **once**. [`collection_application::ask`] waits, renews a blocked session and asks again
//! without bound, which is right for a night's collection and wrong here: a block is the answer this
//! check exists to report, to the owner, and nothing here works around one. It writes nothing: no
//! Bronze object, no catalog row, no file. The last stdout line is
//! `courtauction-go-kr-reach-json {json}`; the process exits 0 whenever the site was asked, whatever
//! it answered.

use anyhow::Context;
use chrono::Utc;
use collection_application::CourtAuctionSessions;
use collection_domain::court_auction::{classify, Answer};
use serde_json::{json, Value as JsonValue};
use tokio::time::Instant;

use super::{asked_condition, list_request, session_pool, source_contract, SourceContract, PREFIX};
use crate::public_data_control_support::optional_env_value;

const KST_HOURS: i64 = 9;

/// Asks the site once from where this process runs and prints what it answered.
///
/// # Errors
/// Returns an error when the contract is invalid or names no condition to ask.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let base = optional_env_value(&format!("{PREFIX}_BASE_URI"))?
        .unwrap_or_else(|| contract.base_url.clone());
    let report = reach(&contract, &base).await?;
    println!("courtauction-go-kr-reach-json {report}");
    Ok(())
}

/// One question to `base`, read by the contract's block signals: `answered` with how many rows the
/// site says there are, `blocked` or `failed` with why, or `no_answer` when no reply arrived.
pub(crate) async fn reach(contract: &SourceContract, base: &str) -> anyhow::Result<JsonValue> {
    let search = &contract.item_search;
    let condition = search
        .conditions
        .first()
        .context("the item search names no condition")?;
    let day = (Utc::now() + chrono::Duration::hours(KST_HOURS)).date_naive();
    let (request, _body) = list_request(
        contract,
        search,
        &asked_condition(search, condition, day),
        1,
    );
    let pool = session_pool(contract, base)?;
    let started = Instant::now();
    let sent = pool.send(&request).await;
    let seconds = started.elapsed().as_secs_f64();
    let asked = json!({
        "base": base,
        "operation": search.operation,
        "path": search.path,
        "asked_at_utc": Utc::now().to_rfc3339(),
        "seconds": seconds,
        "requests": 1,
        "sessions_opened": pool.sessions().len(),
    });
    let verdict = match sent {
        Err(error) => json!({"verdict": "no_answer", "why": error.0}),
        Ok((_ticket, reply)) => {
            match classify(
                &contract.collection.block_signals,
                reply.status,
                &reply.body,
            ) {
                Answer::Data(data) => json!({
                    "verdict": "answered",
                    "http_status": reply.status,
                    "total": data["dma_pageInfo"]["totalCnt"],
                    "rows": data["dlt_srchResult"].as_array().map_or(0, Vec::len),
                }),
                Answer::Blocked(why) => json!({
                    "verdict": "blocked",
                    "http_status": reply.status,
                    "why": format!("{why:?}"),
                    "body_bytes": reply.body.len(),
                }),
                Answer::Failed(why) => json!({
                    "verdict": "failed",
                    "http_status": reply.status,
                    "why": format!("{why:?}"),
                    "body_bytes": reply.body.len(),
                }),
            }
        }
    };
    let mut report = asked;
    if let (Some(report), Some(verdict)) = (report.as_object_mut(), verdict.as_object()) {
        report.extend(verdict.clone());
    }
    Ok(report)
}
