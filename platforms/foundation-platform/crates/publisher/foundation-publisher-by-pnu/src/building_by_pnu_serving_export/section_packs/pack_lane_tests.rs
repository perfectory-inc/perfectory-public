//! A pack-only lane (root ADR-0190) end to end on a local store: the contract's `unit-price` lane
//! bakes its Gold panel by rule, passes gate (가), and its first publish writes the lane's first
//! manifest, holding the latency evidence to the contract's baseline lane. PNUs and snapshot ids sit
//! in the repository-reserved synthetic namespaces (`scripts/guard/public-fixture-safety.py`).

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::bake::{self, BakeConfig};
use super::equality::{self, EqualityConfig};
use super::gate::{self, LatencyEvidence};
use super::publish::{self, PublishConfig};
use super::sections::{PackProvenance, Renderer};
use super::PackCommand;
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::by_pnu;

const SNAPSHOT: &str = "999990000000000001";
const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000100010000";

fn lane() -> anyhow::Result<ByPnuLane> {
    ByPnuLane::named("unit-price")?.context("the contract names the unit-price lane")
}

fn row(pnu: &str, price: u64) -> JsonMap<String, JsonValue> {
    let units = format!(
        r#"[{{"id":"00000000-0000-8000-8000-000000000001","official_price_history":[{{"base_date":"20250101","price_won":{price}}}]}}]"#
    );
    JsonMap::from_iter([
        ("pnu".to_owned(), json!(pnu)),
        ("units_json".to_owned(), json!(units)),
        ("row_digest".to_owned(), json!("a".repeat(64))),
        ("source_snapshot_id".to_owned(), json!("synthetic")),
        ("published_at_utc".to_owned(), json!("2099-01-01T00:00:00Z")),
    ])
}

struct Work {
    root: PathBuf,
    work: PathBuf,
}

impl Drop for Work {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
        let _ = std::fs::remove_dir_all(&self.work);
    }
}

/// Passing latency evidence of the equality evidence's sample, measured against `baseline`.
fn latency(
    lane: ByPnuLane,
    equality: &Path,
    baseline: Option<&str>,
    out: &Path,
) -> anyhow::Result<PathBuf> {
    let (drawn, _) = gate::read::<gate::EqualityEvidence>(equality)?;
    let policy = section_pack_policy()?;
    let size = u64::try_from(policy.cutover_gate.latency_sample_size)?;
    let timings = json!({"p50": 40.0, "p95": 90.0, "p99": 110.0, "mean": 45.0, "max": 120.0});
    let cpu = json!({"script": "preview", "from_utc": "", "to_utc": "", "requests": size,
        "cpu_p50_ms": 1.0, "cpu_p99_ms": 1.0, "statuses": {"success": size}});
    let head = json!({
        "schema_version": policy.cutover_gate.evidence_schema_version,
        "kind": gate::LATENCY_KIND,
        "lane": lane.unit(),
        "pack_generation": 1,
        "live_base_url": "https://buildings-preview.example.test",
        "preview_base_url": "https://unit-prices-preview.example.test",
        "environment": gate::PRODUCTION_ENVIRONMENT,
        "sample_size": size,
        "sample_sha256": drawn.sample_sha256,
        "answered": size, "mismatched": 0, "failed": 0,
        "live_ms": timings, "pack_ms": timings,
        "increase_p50_ms": 0.0, "increase_p95_ms": 0.0, "bound_p50_ms": 50.0, "bound_p95_ms": 50.0,
        "examples": [], "passed": true, "measured_at_utc": "2026-01-01T00:00:00Z",
    });
    let rest = json!({
        "concurrency": 8, "cold_answered": size, "warm_answered": 0,
        "worker_cpu": cpu, "availability": 1.0,
        "load": {"requests_per_second": 25, "duration_seconds": 600, "max_in_flight": 64,
            "sent": size, "answered": size, "shed": 0, "achieved_requests_per_second": 25.0,
            "availability": 1.0, "failures": {}, "latency_ms": timings,
            "server_timing": {"answers": 0, "total_ms": timings, "r2_ms": timings, "sources": {}, "outcomes": {}},
            "worker_cpu": cpu, "versions": {}},
        "no_gzip": {"sent": 200, "answered": 200, "identity": 200, "mismatched": 0, "not_compared": 0,
            "failures": {}, "outcomes": {}, "latency_ms": timings},
        "cold_read_paths": {gate::COLD_PATH_R2: size},
        "cold_live_read_paths": {gate::COLD_PATH_R2: size},
        "baseline_lane": baseline,
    });
    let mut value = head;
    for (key, field) in rest.as_object().context("an object")? {
        value[key] = field.clone();
    }
    let evidence: LatencyEvidence = serde_json::from_value(value)?;
    equality::write_evidence(out, &evidence)?;
    Ok(out.to_path_buf())
}

#[test]
fn a_contract_lane_has_every_command_and_its_names_by_rule() -> anyhow::Result<()> {
    let lane = lane()?;
    assert_eq!(lane.unit(), "unit-price-by-pnu");
    assert_eq!(
        lane.env("X"),
        "FOUNDATION_PLATFORM_UNIT_PRICE_BY_PNU_SERVING_X"
    );
    assert_eq!(
        super::sections::gold_table(lane).table_name,
        "gold.unit_price_panel"
    );
    for name in [
        "plan-unit-price-by-pnu-pack-shards",
        "export-unit-price-by-pnu-section-packs",
        "verify-unit-price-by-pnu-section-pack-equality",
        "probe-unit-price-by-pnu-section-pack-latency",
        "publish-unit-price-by-pnu-section-packs",
        "inspect-unit-price-by-pnu-section-packs",
        "check-unit-price-gateway-version-health",
        "monitor-unit-price-by-pnu-serving",
        "show-unit-price-by-pnu-serving-state",
    ] {
        assert_eq!(
            PackCommand::parse(name).map(|command| command.lane),
            Some(lane),
            "{name}"
        );
    }
    assert_eq!(
        PackCommand::parse("export-building-by-pnu-section-packs").map(|c| c.lane),
        Some(ByPnuLane::Building)
    );
    assert!(PackCommand::parse("export-nothing-by-pnu-section-packs").is_none());
    Ok(())
}

#[tokio::test]
async fn a_pack_only_lane_is_cut_over_without_objects() -> anyhow::Result<()> {
    let lane = lane()?;
    let id = uuid::Uuid::now_v7();
    let dirs = Work {
        root: std::env::temp_dir().join(format!("foundation-platform-pack-lane-{id}")),
        work: std::env::temp_dir().join(format!("foundation-platform-pack-lane-work-{id}")),
    };
    std::fs::create_dir_all(&dirs.root)?;
    std::fs::create_dir_all(&dirs.work)?;
    let output = ProfileStoreConfig::Local {
        root: dirs.root.clone(),
    };
    let store = ByPnuServingStore::open(lane, &output)?;
    let rows = vec![row(PNU_A, 100), row(PNU_B, 200)];
    let config = BakeConfig {
        lane,
        output: output.clone(),
        generation: Some(1),
        sections: lane.section_packs()?.sections.clone(),
        patch: None,
        upserts: None,
        pnu_prefix: None,
        expected_gold_snapshot: None,
        max_concurrency: 4,
        summary_path: dirs.work.join("unused.json"),
        sample_candidates_per_million: None,
    };
    let provenance = PackProvenance {
        table: "gold.unit_price_panel".to_owned(),
        iceberg_snapshot_id: SNAPSHOT.to_owned(),
        metadata_location: "s3://fixture/metadata.json".to_owned(),
        manifest_list_location: "s3://fixture/manifest.avro".to_owned(),
    };
    let summary = bake::bake(
        &config,
        &store,
        &provenance,
        &rows,
        &Renderer::load(lane).await?,
    )
    .await?;
    assert_eq!(summary.exported_row_count, 2);
    let summaries = dirs.work.join("summaries");
    std::fs::create_dir_all(&summaries)?;
    std::fs::write(summaries.join("0.json"), serde_json::to_vec(&summary)?)?;
    let equality_config = EqualityConfig {
        lane,
        summary_dir: summaries.clone(),
        generation: 1,
        expected_documents: 2,
        evidence_path: dirs.work.join("equality.json"),
    };
    let evidence = equality::verify(&equality_config)?;
    assert!(evidence.passed);
    equality::write_evidence(&equality_config.evidence_path, &evidence)?;

    let gateway = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(lane.policy()?.request_path.capabilities.clone()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "unit": lane.unit(),
            "manifest_schema_versions": [1, 2, 3, 4],
        })))
        .mount(&gateway)
        .await;
    let jobs = dirs.work.join("jobs.v1.json");
    let wanted = &lane.section_packs()?.scheduled_bake;
    std::fs::write(
        &jobs,
        serde_json::to_vec(
            &json!({"jobs": [{"id": wanted.job, "capabilities": [wanted.capability]}]}),
        )?,
    )?;
    let publish_config = |latency: PathBuf| PublishConfig {
        lane,
        output: output.clone(),
        summary_dir: Some(summaries.clone()),
        expected_gold_snapshot: SNAPSHOT.to_owned(),
        gold_record_count: Some(2),
        installed_jobs: jobs.clone(),
        equality_evidence: Some(equality_config.evidence_path.clone()),
        latency_evidence: Some(latency),
        change_set: None,
    };
    assert!(!store.exists(by_pnu::manifest_key(lane)?).await?);

    // Evidence measured against the lane's own (absent) object path is not this lane's gate.
    let own = latency(
        lane,
        &equality_config.evidence_path,
        None,
        &dirs.work.join("own.json"),
    )?;
    assert!(
        publish::publish(&publish_config(own), &store, &gateway.uri())
            .await
            .is_err()
    );
    assert!(!store.exists(by_pnu::manifest_key(lane)?).await?);

    let against = latency(
        lane,
        &equality_config.evidence_path,
        Some("building"),
        &dirs.work.join("baseline.json"),
    )?;
    // Its own p95 is also held to the contract's absolute bound: a slow baseline read cannot hide
    // a slow lane inside the increase bound.
    let (mut slow, _) = gate::read::<LatencyEvidence>(&against)?;
    assert!(slow.verdict()?);
    let bound = lane
        .pack_lane()
        .context("a pack-only lane")?
        .latency_p95_max_ms;
    slow.live_ms.p95 = bound + 100.0;
    slow.pack_ms.p95 = bound + 100.0;
    assert!(!slow.verdict()?);
    slow.lane = ByPnuLane::Building.unit().to_owned();
    assert!(
        slow.verdict()?,
        "a lane with an object path is held to the increase alone"
    );

    let state = publish::publish(&publish_config(against), &store, &gateway.uri()).await?;
    assert_eq!(state.document_count, 2);
    let live = ServedManifest::parse(lane, &store.read_manifest().await?.0)?;
    assert_eq!(live.unit, "unit-price-by-pnu");
    assert_eq!((live.object_count, live.base_object_count), (0, 0));
    assert_eq!(live.gold_table, "gold.unit_price_panel");
    assert!(live.section_packs.is_some());

    // Once cut over, the hourly monitor holds the live answers to the packs alone: the lane never
    // had an object to read.
    let live_host = MockServer::start().await;
    let prefix = lane.policy()?.request_path.prefix.clone();
    for pnu in [PNU_A, PNU_B] {
        let inspected = super::inspect::inspect(&store, pnu, None).await?;
        Mock::given(method("GET"))
            .and(path(format!("{prefix}{pnu}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(serde_json::to_vec(&inspected["document"])?),
            )
            .mount(&live_host)
            .await;
    }
    let pnus = [PNU_A.to_owned(), PNU_B.to_owned()];
    let report = super::monitor::check(&store, &live_host.uri(), &pnus, 3000.0).await?;
    assert!(report.passed, "{report:?}");
    assert_eq!(report.objects_not_compared, 2);
    // And a live answer that is not the packs' document is a breach.
    let drifted = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"units": []})))
        .mount(&drifted)
        .await;
    let report = super::monitor::check(&store, &drifted.uri(), &pnus, 3000.0).await?;
    assert!(!report.passed);
    assert_eq!(report.drifted_from_served.len(), 2);
    Ok(())
}
