//! The latency probe's memory does not grow with its sample (2026-10-16: the unit price lane's gate
//! (?? was OOM-killed at 8 GB, the probe holding every sampled PNU's two decoded answers until the
//! last one came back). Its own binary, so the process's peak resident set is this probe's alone;
//! Linux, where the kernel reports and resets that peak (`/proc/self/status` `VmHWM`).
#![cfg(target_os = "linux")]

use std::io::Write as _;

use anyhow::Context as _;
use foundation_publisher_by_pnu::building_by_pnu_serving_export::section_packs::latency::{
    self, LatencyConfig,
};
use foundation_publisher_core::by_pnu_gateway_contract::ByPnuLane;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Sampled PNUs (the reserved 99999 region), ten per legal dong.
const SAMPLE: usize = 400;
/// Each answer's decoded size: the sample holding both answers of every PNU would be 400 MiB.
const DOCUMENT_BYTES: usize = 512 * 1024;
/// The peak may grow by the requests in flight, never by the sample.
const MAX_GROWTH_BYTES: u64 = 96 * 1024 * 1024;

fn peak_resident_bytes() -> anyhow::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .context("no VmHWM in /proc/self/status")?;
    let kib: u64 = line
        .trim_start_matches("VmHWM:")
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()?;
    Ok(kib * 1024)
}

/// Resets the peak to the current resident set (`clear_refs` 5), so the growth is the probe's.
fn reset_peak() -> anyhow::Result<()> {
    std::fs::write("/proc/self/clear_refs", "5")?;
    Ok(())
}

async fn serve(document: &[u8]) -> anyhow::Result<MockServer> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(document)?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(encoder.finish()?)
                .insert_header("Content-Encoding", "gzip"),
        )
        .mount(&server)
        .await;
    Ok(server)
}

#[tokio::test(flavor = "current_thread")]
async fn the_probe_holds_no_answer_past_its_read() -> anyhow::Result<()> {
    let document = format!(
        r#"{{"pnu":"x","pad":"{}"}}"#,
        "a".repeat(DOCUMENT_BYTES)
    );
    let live = serve(document.as_bytes()).await?;
    let pack = serve(document.as_bytes()).await?;
    let pnus: Vec<String> = (0..SAMPLE)
        .map(|index| format!("99999{:05}{:09}", index / 10, index))
        .collect();
    let work = std::env::temp_dir();
    let config = LatencyConfig {
        lane: ByPnuLane::Building,
        generation: 1,
        live_base_url: live.uri(),
        preview_base_url: pack.uri(),
        equality_evidence: work.join("unused.json"),
        evidence_path: work.join("unused-latency.json"),
        concurrency: 4,
        analytics: None,
        preview_script: "foundation-building-gateway-preview".to_owned(),
        load: None,
        no_gzip_sample_size: 0,
        sample_units: Vec::new(),
        baseline: None,
        run: "r".to_owned(),
    };

    reset_peak()?;
    let before = peak_resident_bytes()?;
    let evidence = latency::probe(&config, &pnus).await?;
    let growth = peak_resident_bytes()?.saturating_sub(before);

    anyhow::ensure!(
        evidence.answered == u64::try_from(SAMPLE)?,
        "{} of {SAMPLE} answered",
        evidence.answered
    );
    anyhow::ensure!(
        growth <= MAX_GROWTH_BYTES,
        "the probe's peak grew by {} MiB over {SAMPLE} reads of {} KiB (bound {} MiB): it holds answers past their read",
        growth / (1024 * 1024),
        DOCUMENT_BYTES / 1024,
        MAX_GROWTH_BYTES / (1024 * 1024)
    );
    Ok(())
}
