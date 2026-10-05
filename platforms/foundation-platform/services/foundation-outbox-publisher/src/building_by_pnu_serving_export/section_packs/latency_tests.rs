//! The latency probe's own behaviour against local stand-ins: reads in flight at once, cold and
//! warm told apart by legal dong, failures counted by side and class, the preview's
//! `Server-Timing` summarised, and the Worker's CPU read from a Workers analytics stand-in. PNUs
//! sit in the repository-reserved synthetic namespace (`scripts/guard/public-fixture-safety.py`).

use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::gate;
use super::latency::{self, AnalyticsConfig, LatencyConfig, ParsedServerTiming};
use crate::by_pnu_gateway_contract::section_pack_policy;

/// Three PNUs of one legal dong, then two of another.
const PNUS: [&str; 5] = [
    "9999900000100000000",
    "9999900000100010000",
    "9999900000100020000",
    "9999900001100000000",
    "9999900001100010000",
];
const TIMING: &str = r#"outcome;desc="document", r2;dur=120;desc="gets=5 retries=0", pack-buildings;dur=120;desc="r2-head+range", pack-floors;dur=60;desc="r2-whole", total;dur=140"#;

fn pnus() -> Vec<String> {
    PNUS.iter().map(|pnu| (*pnu).to_owned()).collect()
}

async fn route(status: u16, delay_ms: u64, timing: Option<&str>) -> MockServer {
    let server = MockServer::start().await;
    let mut answer = ResponseTemplate::new(status)
        .set_body_string(r#"{"pnu":"x","buildings":[]}"#)
        .set_delay(Duration::from_millis(delay_ms));
    if let Some(timing) = timing {
        answer = answer.insert_header("Server-Timing", timing);
    }
    Mock::given(method("GET"))
        .respond_with(answer)
        .mount(&server)
        .await;
    server
}

fn config(live: &MockServer, pack: &MockServer, concurrency: usize) -> LatencyConfig {
    let work = std::env::temp_dir();
    LatencyConfig {
        generation: 1,
        live_base_url: live.uri(),
        preview_base_url: pack.uri(),
        equality_evidence: work.join("unused.json"),
        evidence_path: work.join("unused-latency.json"),
        concurrency,
        analytics: None,
    }
}

#[test]
fn the_first_read_of_each_legal_dong_is_cold() -> anyhow::Result<()> {
    assert_eq!(
        latency::cold_reads(&pnus())?,
        vec![true, false, false, true, false]
    );
    Ok(())
}

#[test]
fn server_timing_is_read_as_the_worker_writes_it() {
    assert_eq!(
        latency::parse_server_timing(TIMING),
        ParsedServerTiming {
            total_ms: Some(140.0),
            r2_ms: Some(120.0),
            sources: vec!["r2-head+range".to_owned(), "r2-whole".to_owned()],
        }
    );
    assert_eq!(
        latency::parse_server_timing(r#"outcome;desc="edge-response", total;dur=7"#),
        ParsedServerTiming {
            total_ms: Some(7.0),
            r2_ms: None,
            sources: Vec::new(),
        }
    );
}

/// Five PNUs, each route 200 ms slow, four in flight: a sequential probe would take two seconds,
/// this one well under; each read is still timed on its own (at least the route's delay).
#[tokio::test]
async fn reads_run_concurrently_and_are_split_cold_and_warm() -> anyhow::Result<()> {
    let live = route(200, 200, None).await;
    let pack = route(200, 200, Some(TIMING)).await;
    let started = std::time::Instant::now();
    let evidence = latency::probe(&config(&live, &pack, 4), &pnus()).await?;
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        (evidence.answered, evidence.mismatched, evidence.failed),
        (5, 0, 0)
    );
    assert_eq!((evidence.cold_answered, evidence.warm_answered), (2, 3));
    assert!(evidence.pack_ms.p50 >= 200.0 && evidence.pack_warm_ms.p50 >= 200.0);
    assert!(evidence.live_ms.p50 >= 200.0 && evidence.live_warm_ms.p50 >= 200.0);
    assert_eq!(evidence.concurrency, 4);
    assert_eq!(evidence.server_timing.answers, 5);
    assert_eq!(evidence.server_timing.total_ms.p50, 140.0);
    assert_eq!(evidence.server_timing.sources.get("r2-whole"), Some(&5));
    // No analytics: no CPU, and the gate stays shut.
    assert!(evidence.worker_cpu.is_none());
    assert!(!evidence.verdict()?);
    Ok(())
}

#[tokio::test]
async fn failures_are_counted_by_side_and_class() -> anyhow::Result<()> {
    let live = route(200, 0, None).await;
    let unavailable = route(
        503,
        0,
        Some(r#"outcome;desc="r2-unavailable", total;dur=6000"#),
    )
    .await;
    let evidence = latency::probe(&config(&live, &unavailable, 2), &pnus()).await?;
    assert_eq!((evidence.answered, evidence.failed), (0, 5));
    assert_eq!(evidence.failures.get("pack:http-503"), Some(&5));
    assert!(!evidence.failures.keys().any(|key| key.starts_with("live:")));
    assert_eq!(evidence.examples.len(), 5);
    Ok(())
}

async fn analytics(groups: serde_json::Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"viewer": {"accounts": [{"workersInvocationsAdaptive": groups}]}},
            "errors": null,
        })))
        .mount(&server)
        .await;
    server
}

fn analytics_config(server: &MockServer) -> AnalyticsConfig {
    AnalyticsConfig {
        endpoint: format!("{}/graphql", server.uri()),
        account_id: "account".to_owned(),
        api_token: "token".to_owned(),
        script: "foundation-building-gateway-preview".to_owned(),
        wait: Duration::ZERO,
        poll: Duration::ZERO,
    }
}

/// The CPU the gate holds: the worst group's quantiles, every status counted, and a cut-off
/// (`exceededResources`, error 1102) refusing the gate however fast the reads were.
#[tokio::test]
async fn worker_cpu_is_read_from_analytics_and_gates() -> anyhow::Result<()> {
    let bound = section_pack_policy()?.cutover_gate.worker_cpu_p99_max_ms;
    let live = route(200, 0, None).await;
    let pack = route(200, 0, Some(TIMING)).await;
    let within = analytics(json!([
        {"sum": {"requests": 5}, "dimensions": {"status": "success"},
         "quantiles": {"cpuTimeP50": 1500.0, "cpuTimeP99": bound * 1000.0}},
    ]))
    .await;
    let mut probed = config(&live, &pack, 2);
    probed.analytics = Some(analytics_config(&within));
    let evidence = latency::probe(&probed, &pnus()).await?;
    let cpu = evidence.worker_cpu.clone().unwrap_or_default();
    assert_eq!(
        (cpu.requests, cpu.cpu_p50_ms, cpu.cpu_p99_ms),
        (5, 1.5, bound)
    );
    assert_eq!(cpu.exceeded_resources(), 0);
    assert_eq!(evidence.bound_cpu_p99_ms, bound);
    // Every condition but the sample size (five PNUs, not the contract's) holds.
    let mut full = evidence.clone();
    full.sample_size = u64::try_from(section_pack_policy()?.cutover_gate.latency_sample_size)?;
    full.answered = full.sample_size;
    assert!(full.verdict()?, "{full:?}");

    let cut_off = analytics(json!([
        {"sum": {"requests": 4}, "dimensions": {"status": "success"},
         "quantiles": {"cpuTimeP50": 1500.0, "cpuTimeP99": 3000.0}},
        {"sum": {"requests": 1}, "dimensions": {"status": gate::EXCEEDED_RESOURCES},
         "quantiles": {"cpuTimeP50": 10000.0, "cpuTimeP99": 10000.0}},
    ]))
    .await;
    probed.analytics = Some(analytics_config(&cut_off));
    let evidence = latency::probe(&probed, &pnus()).await?;
    let cpu = evidence.worker_cpu.clone().unwrap_or_default();
    assert_eq!((cpu.requests, cpu.exceeded_resources()), (5, 1));
    assert_eq!(cpu.cpu_p99_ms, 10.0);
    let mut full = evidence;
    full.sample_size = u64::try_from(section_pack_policy()?.cutover_gate.latency_sample_size)?;
    full.answered = full.sample_size;
    assert!(!full.verdict()?, "a cut-off invocation passed");
    Ok(())
}

#[tokio::test]
async fn analytics_errors_are_refused_not_read_as_no_cpu() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": null,
            "errors": [{"message": "not authorized"}],
        })))
        .mount(&server)
        .await;
    let live = route(200, 0, None).await;
    let pack = route(200, 0, None).await;
    let mut probed = config(&live, &pack, 1);
    probed.analytics = Some(analytics_config(&server));
    let refused = latency::probe(&probed, &pnus()[..1]).await;
    assert!(refused.is_err());
    Ok(())
}
