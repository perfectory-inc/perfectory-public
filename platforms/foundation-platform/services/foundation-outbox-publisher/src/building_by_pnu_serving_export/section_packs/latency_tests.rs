//! The latency probe's own behaviour against local stand-ins: reads in flight at once, cold and
//! warm told apart by legal dong, failures counted by side and class, the preview's
//! `Server-Timing` summarised, and the Worker's CPU read from a Workers analytics stand-in. PNUs
//! sit in the repository-reserved synthetic namespace (`scripts/guard/public-fixture-safety.py`).

use std::io::Write as _;
use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::analytics::AnalyticsConfig;
use super::gate;
use super::latency::{self, LatencyConfig, ParsedServerTiming};
use super::load::{self, LoadPlan};
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

const BODY: &str = r#"{"pnu":"x","buildings":[]}"#;

fn gzip(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

/// Mounts `body` on `server`. With `gzip_when_asked`, a request asking for gzip gets it compressed
/// with `Content-Encoding: gzip` (the Worker passing the member through); every other request
/// gets the plain bytes.
pub(super) async fn answer_on(
    server: &MockServer,
    status: u16,
    delay_ms: u64,
    body: &[u8],
    timing: Option<&str>,
    gzip_when_asked: bool,
) -> anyhow::Result<()> {
    let template = |bytes: Vec<u8>| {
        let answer = ResponseTemplate::new(status)
            .set_body_bytes(bytes)
            .set_delay(Duration::from_millis(delay_ms));
        match timing {
            Some(timing) => answer.insert_header("Server-Timing", timing),
            None => answer,
        }
    };
    if gzip_when_asked {
        Mock::given(method("GET"))
            .and(header("accept-encoding", "gzip"))
            .respond_with(template(gzip(body)?).insert_header("Content-Encoding", "gzip"))
            .with_priority(1)
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .respond_with(template(body.to_vec()))
        .with_priority(2)
        .mount(server)
        .await;
    Ok(())
}

async fn route(status: u16, delay_ms: u64, timing: Option<&str>, gzip: bool) -> MockServer {
    let server = MockServer::start().await;
    answer_on(&server, status, delay_ms, BODY.as_bytes(), timing, gzip)
        .await
        .unwrap_or_else(|error| panic!("{error:#}"));
    server
}

/// A no-gzip sample that held: every read answered, uncompressed, with the same content.
fn no_gzip_held(sent: u64) -> gate::NoGzipEvidence {
    gate::NoGzipEvidence {
        sent,
        answered: sent,
        identity: sent,
        ..gate::NoGzipEvidence::default()
    }
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
        preview_script: "foundation-building-gateway-preview".to_owned(),
        load: None,
        no_gzip_sample_size: 2,
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
            outcome: Some("document".to_owned()),
            total_ms: Some(140.0),
            r2_ms: Some(120.0),
            sources: vec!["r2-head+range".to_owned(), "r2-whole".to_owned()],
        }
    );
    assert_eq!(
        latency::parse_server_timing(r#"outcome;desc="edge-response", total;dur=7"#),
        ParsedServerTiming {
            outcome: Some("edge-response".to_owned()),
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
    let live = route(200, 200, None, false).await;
    let pack = route(200, 200, Some(TIMING), true).await;
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
    // Which path answered, and in which encoding: the pack side passed the member through.
    assert_eq!(
        evidence.server_timing.outcomes,
        [("document".to_owned(), 5)].into()
    );
    assert_eq!(
        evidence.encodings,
        [("live:identity".to_owned(), 5), ("pack:gzip".to_owned(), 5)].into()
    );
    // The no-gzip sample: the first two PNUs, uncompressed, the live route's content.
    let no_gzip = evidence.no_gzip.clone().unwrap_or_default();
    assert_eq!(
        (
            no_gzip.sent,
            no_gzip.answered,
            no_gzip.identity,
            no_gzip.mismatched
        ),
        (2, 2, 2, 0)
    );
    // No analytics: no CPU, and the gate stays shut.
    assert!(evidence.worker_cpu.is_none());
    assert!(!evidence.verdict()?);
    Ok(())
}

#[tokio::test]
async fn failures_are_counted_by_side_and_class() -> anyhow::Result<()> {
    let live = route(200, 0, None, false).await;
    let unavailable = route(
        503,
        0,
        Some(r#"outcome;desc="r2-unavailable", total;dur=6000"#),
        true,
    )
    .await;
    let evidence = latency::probe(&config(&live, &unavailable, 2), &pnus()).await?;
    assert_eq!((evidence.answered, evidence.failed), (0, 5));
    assert_eq!(evidence.failures.get("pack:http-503"), Some(&5));
    assert!(!evidence.failures.keys().any(|key| key.starts_with("live:")));
    assert_eq!(evidence.examples.len(), 5);
    Ok(())
}

/// A pack answer that does not come gzip to a client asking for gzip did not pass the member
/// through: it is a failure, and the gate stays shut however equal the content.
#[tokio::test]
async fn a_pack_answer_that_is_not_gzip_is_a_failure() -> anyhow::Result<()> {
    let live = route(200, 0, None, false).await;
    let plain = route(
        200,
        0,
        Some(r#"outcome;desc="document-decompressed""#),
        false,
    )
    .await;
    let evidence = latency::probe(&config(&live, &plain, 2), &pnus()).await?;
    assert_eq!((evidence.answered, evidence.failed), (0, 5));
    assert_eq!(evidence.failures.get("pack:not-gzip"), Some(&5));
    assert_eq!(evidence.availability, 0.0);
    Ok(())
}

/// The no-gzip sample holds the Worker's decompressing path: an answer that stays gzip, or holds
/// other content, keeps the gate shut.
#[tokio::test]
async fn the_no_gzip_sample_must_answer_uncompressed_and_equal() -> anyhow::Result<()> {
    let live = route(200, 0, None, false).await;
    let always_gzip = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(gzip(BODY.as_bytes())?)
                .insert_header("Content-Encoding", "gzip"),
        )
        .mount(&always_gzip)
        .await;
    let evidence = latency::probe(&config(&live, &always_gzip, 2), &pnus()).await?;
    let no_gzip = evidence.no_gzip.clone().unwrap_or_default();
    assert_eq!((no_gzip.answered, no_gzip.identity), (2, 0));
    let mut full = evidence;
    full.sample_size = u64::try_from(section_pack_policy()?.cutover_gate.latency_sample_size)?;
    full.answered = full.sample_size;
    full.availability = 1.0;
    full.worker_cpu = Some(gate::WorkerCpu {
        requests: 5,
        statuses: [("success".to_owned(), 5)].into(),
        ..gate::WorkerCpu::default()
    });
    full.load = Some(gate::LoadEvidence {
        sent: 5,
        answered: 5,
        availability: 1.0,
        worker_cpu: full.worker_cpu.clone(),
        ..gate::LoadEvidence::default()
    });
    assert!(!full.verdict()?, "a gzip answer to identity passed");
    full.no_gzip = Some(no_gzip_held(2));
    assert!(full.verdict()?, "{full:?}");

    let other = MockServer::start().await;
    answer_on(&other, 200, 0, BODY.as_bytes(), None, true).await?;
    Mock::given(method("GET"))
        .and(header("accept-encoding", "identity"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"pnu":"y","buildings":[]}"#))
        .with_priority(1)
        .mount(&other)
        .await;
    let evidence = latency::probe(&config(&live, &other, 2), &pnus()).await?;
    let no_gzip = evidence.no_gzip.unwrap_or_default();
    assert_eq!((no_gzip.identity, no_gzip.mismatched), (2, 2));
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
        wait: Duration::ZERO,
        poll: Duration::ZERO,
    }
}

/// The CPU the gate holds: the worst group's quantiles, every status counted, and a cut-off
/// (`exceededResources`, error 1102) refusing the gate however fast the reads were.
#[tokio::test]
async fn worker_cpu_is_read_from_analytics_and_gates() -> anyhow::Result<()> {
    let bound = section_pack_policy()?.cutover_gate.worker_cpu_p99_max_ms;
    let live = route(200, 0, None, false).await;
    let pack = route(200, 0, Some(TIMING), true).await;
    let within = analytics(json!([
        {"sum": {"requests": 5}, "dimensions": {"status": "success"},
         "quantiles": {"cpuTimeP50": 1500.0, "cpuTimeP99": bound * 1000.0, "wallTimeP50": 90000.0, "wallTimeP99": 400000.0}},
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
    full.load = Some(gate::LoadEvidence {
        sent: 5,
        answered: 5,
        availability: 1.0,
        worker_cpu: full.worker_cpu.clone(),
        ..gate::LoadEvidence::default()
    });
    assert!(full.verdict()?, "{full:?}");
    let mut unread = full.clone();
    unread.no_gzip = None;
    assert!(
        !unread.verdict()?,
        "a probe without the no-gzip sample passed"
    );

    let cut_off = analytics(json!([
        {"sum": {"requests": 4}, "dimensions": {"status": "success"},
         "quantiles": {"cpuTimeP50": 1500.0, "cpuTimeP99": 3000.0, "wallTimeP50": 90000.0, "wallTimeP99": 400000.0}},
        {"sum": {"requests": 1}, "dimensions": {"status": gate::EXCEEDED_RESOURCES},
         "quantiles": {"cpuTimeP50": 10000.0, "cpuTimeP99": 10000.0, "wallTimeP50": 90000.0, "wallTimeP99": 400000.0}},
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
    full.load = Some(gate::LoadEvidence {
        sent: 5,
        answered: 5,
        availability: 1.0,
        worker_cpu: full.worker_cpu.clone(),
        ..gate::LoadEvidence::default()
    });
    assert!(!full.verdict()?, "a cut-off invocation passed");
    Ok(())
}

/// Without the analytics variables (the unit's EnvironmentFile missing), the gate and the canary
/// check refuse before reading anything, and say which file to create.
#[test]
fn missing_analytics_credentials_are_refused_naming_the_file() -> anyhow::Result<()> {
    let names = &section_pack_policy()?.cloudflare_analytics;
    assert!(std::env::var_os(&names.api_token_env).is_none());
    let refused = AnalyticsConfig::required("gate (나)")
        .err()
        .map(|error| format!("{error:#}"))
        .unwrap_or_default();
    assert!(refused.contains(names.env_file()?), "{refused}");
    assert!(refused.contains(&names.api_token_env), "{refused}");
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
    let live = route(200, 0, None, false).await;
    let pack = route(200, 0, None, true).await;
    let mut probed = config(&live, &pack, 1);
    probed.analytics = Some(analytics_config(&server));
    let refused = latency::probe(&probed, &pnus()[..1]).await;
    assert!(refused.is_err());
    Ok(())
}

/// The load phase paces its requests and sheds what it cannot start: a route slower than the
/// in-flight bound allows loses requests, and they count against availability.
#[tokio::test]
async fn the_load_phase_paces_and_counts_what_it_sheds() -> anyhow::Result<()> {
    let fast = route(200, 0, Some(TIMING), true).await;
    let plan = LoadPlan {
        requests_per_second: 40,
        duration: Duration::from_secs(1),
        max_in_flight: 8,
    };
    let client = reqwest::Client::new();
    let url = |pnu: &str| format!("{}/buildings/by-pnu/{pnu}", fast.uri());
    let held = load::run(&client, &plan, &pnus(), url, None).await?;
    assert_eq!((held.sent, held.shed, held.answered), (40, 0, 40));
    assert_eq!(held.availability, 1.0);
    assert_eq!(held.server_timing.answers, 40);
    assert_eq!(held.server_timing.outcomes.get("document"), Some(&40));
    assert!(held.worker_cpu.is_none());

    let slow = route(200, 400, None, true).await;
    let url = |pnu: &str| format!("{}/buildings/by-pnu/{pnu}", slow.uri());
    let narrow = LoadPlan {
        max_in_flight: 2,
        ..plan
    };
    let shed = load::run(&client, &narrow, &pnus(), url, None).await?;
    assert!(shed.shed > 0, "{shed:?}");
    assert_eq!(shed.sent + shed.shed, 40);
    assert!(shed.availability < 1.0);

    let failing = route(503, 0, None, true).await;
    let url = |pnu: &str| format!("{}/buildings/by-pnu/{pnu}", failing.uri());
    let failed = load::run(&client, &plan, &pnus(), url, None).await?;
    assert_eq!(failed.failures.get("http-503"), Some(&40));
    assert_eq!(failed.availability, 0.0);

    // The load asks for gzip as browsers do; a preview that answers plain bytes fails every read.
    let plain = route(200, 0, None, false).await;
    let url = |pnu: &str| format!("{}/buildings/by-pnu/{pnu}", plain.uri());
    let not_gzip = load::run(&client, &plan, &pnus(), url, None).await?;
    assert_eq!(not_gzip.failures.get("not-gzip"), Some(&40));
    Ok(())
}

/// The draw is seeded: the same request number reads the same PNU every run, and the draw
/// repeats legal dongs (cold and warm reads mixed).
#[test]
fn the_load_draw_is_seeded_and_repeats() -> anyhow::Result<()> {
    let first = (0..200)
        .map(|n| load::drawn(n, 5))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let again = (0..200)
        .map(|n| load::drawn(n, 5))
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(first, again);
    assert!((0..5).all(|index| first.contains(&index)));
    Ok(())
}

/// A canary step's verdict: each bound breached alone is named, a healthy step names none.
#[test]
fn a_canary_step_is_judged_on_every_bound() -> anyhow::Result<()> {
    let gate = &section_pack_policy()?.cutover_gate;
    let healthy = gate::WorkerCpu {
        requests: 1000,
        cpu_p50_ms: 1.0,
        cpu_p99_ms: gate.worker_cpu_p99_max_ms,
        wall_p50_ms: 200.0,
        wall_p99_ms: 600.0,
        statuses: [("success".to_owned(), 1000)].into(),
        ..gate::WorkerCpu::default()
    };
    let old = healthy.clone();
    let ok_hosts: std::collections::BTreeMap<u16, u64> = [(200, 10_000), (503, 1)].into();
    assert!(super::health::breaches(&healthy, Some(&old), Some(&ok_hosts), 200, gate).is_empty());

    let mut few = healthy.clone();
    few.requests = 10;
    let mut cut_off = healthy.clone();
    cut_off
        .statuses
        .insert(gate::EXCEEDED_RESOURCES.to_owned(), 1);
    let mut throwing = healthy.clone();
    throwing
        .statuses
        .insert("scriptThrewException".to_owned(), 5);
    let mut costly = healthy.clone();
    costly.cpu_p99_ms += 0.1;
    let mut slower = healthy.clone();
    slower.wall_p99_ms += gate.slo.latency_max_increase_ms.warm.p99 + 1.0;
    for (label, new) in [
        ("too little traffic", few),
        ("a cut-off", cut_off),
        ("exceptions", throwing),
        ("CPU", costly),
        ("wall time", slower),
    ] {
        assert_eq!(
            super::health::breaches(&new, Some(&old), Some(&ok_hosts), 200, gate).len(),
            1,
            "{label}"
        );
    }
    let bad_hosts: std::collections::BTreeMap<u16, u64> = [(200, 1000), (503, 10)].into();
    assert_eq!(
        super::health::breaches(&healthy, Some(&old), Some(&bad_hosts), 200, gate).len(),
        1,
        "5xx"
    );
    Ok(())
}
