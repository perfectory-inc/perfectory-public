//! The building section pack lane end to end on a local store: bake, gate (가) and (나), the
//! first publish, a patch with a changed document and a tombstone (gate 다), and a planted
//! failure for every gate. PNUs and snapshot ids sit in the repository-reserved synthetic
//! namespaces (`scripts/guard/public-fixture-safety.py`).
//!
//! The same run writes the golden packs and documents the gateway Worker's tests read
//! (`services/foundation-building-gateway/test/fixtures/section-packs/`), so the Rust writer and
//! the TypeScript reader are held to one set of bytes.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::super::building_document::{
    self, GoldSnapshotProvenance, BUILDING_DOCUMENT_SCHEMA_VERSION,
};
use super::bake::{self, BakeConfig, PackExportSummary};
use super::equality::{self, EqualityConfig};
use super::gate::{self, LatencyEvidence, Timings};
use super::latency::{self, LatencyConfig, Sample};
use super::publish::{self, ChangeSetPaths, PublishConfig};
use super::read::{self, PackView, Resolved};
use super::sections::KNOWN_SECTIONS;
use crate::building_link_evidence::ApprovedBuildingLinks;
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};
use crate::by_pnu_pack::tests::assert_golden;
use crate::by_pnu_serving_manifest::{ServedManifest, ServingManifest};
use crate::by_pnu_serving_patch_export::PatchTarget;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{by_pnu, by_pnu_packs};

const LANE: ByPnuLane = ByPnuLane::Building;
/// The Spark fixture row's PNU: one building with a floor and a priced unit, one unlinked unit.
const PNU_A: &str = "9999900000100000000";
/// A PNU with no building rows at all.
const PNU_B: &str = "9999900000100010000";
/// A PNU of another legal dong.
const PNU_C: &str = "9999900001100000000";
const UNIT: &str = "9999900000";
const SNAPSHOT: &str = "999990000000000001";
const NEXT_SNAPSHOT: &str = "999990000000000002";

struct Lane {
    root: PathBuf,
    store: ByPnuServingStore,
    gateway: MockServer,
    work: PathBuf,
}

impl Drop for Lane {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
        let _ = std::fs::remove_dir_all(&self.work);
    }
}

fn spark_row() -> anyhow::Result<JsonMap<String, JsonValue>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let path = Path::new(&manifest_dir)
        .join("../../infra/lakehouse/spark/tests/fixtures/building_panel_gold_row.json");
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn empty_row(pnu: &str) -> JsonMap<String, JsonValue> {
    JsonMap::from_iter([
        ("pnu".to_owned(), json!(pnu)),
        ("buildings_json".to_owned(), json!("[]")),
        ("unlinked_units_json".to_owned(), json!("[]")),
    ])
}

/// The Spark row with its first unit's newest price changed: one changed document.
fn changed_row() -> anyhow::Result<JsonMap<String, JsonValue>> {
    let mut row = spark_row()?;
    let buildings = row["buildings_json"]
        .as_str()
        .context("buildings_json")?
        .replace("35000000", "37000000");
    row.insert("buildings_json".to_owned(), json!(buildings));
    Ok(row)
}

fn provenance(snapshot: &str) -> GoldSnapshotProvenance {
    GoldSnapshotProvenance {
        table: "gold.building_panel".to_owned(),
        iceberg_snapshot_id: snapshot.to_owned(),
        metadata_location: "s3://fixture/metadata.json".to_owned(),
        manifest_list_location: "s3://fixture/manifest.avro".to_owned(),
    }
}

fn object(row: &JsonMap<String, JsonValue>, snapshot: &str) -> anyhow::Result<Vec<u8>> {
    let document = building_document::document_with_approvals(
        &provenance(snapshot),
        row,
        &ApprovedBuildingLinks::default(),
    )?;
    document.to_bytes()
}

impl Lane {
    /// A building lane serving objects of `rows` at base generation 1, as it does today.
    async fn serving_objects(
        label: &str,
        rows: &[JsonMap<String, JsonValue>],
    ) -> anyhow::Result<Self> {
        let id = uuid::Uuid::now_v7();
        let root =
            std::env::temp_dir().join(format!("foundation-platform-section-packs-{label}-{id}"));
        let work = std::env::temp_dir().join(format!(
            "foundation-platform-section-packs-work-{label}-{id}"
        ));
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(&work)?;
        let store =
            ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
        for row in rows {
            let pnu = row["pnu"].as_str().context("pnu")?;
            let body = object(row, SNAPSHOT)?;
            let sha = format!("{:x}", Sha256::digest(&body));
            store
                .write_object_create_only(&by_pnu::object_key(LANE, 1, pnu)?, &body, &sha)
                .await?;
        }
        let manifest = ServingManifest {
            schema_version: 2,
            unit: LANE.unit().to_owned(),
            base_generation: 1,
            base_object_count: u64::try_from(rows.len())?,
            document_schema_version: BUILDING_DOCUMENT_SCHEMA_VERSION.to_owned(),
            gold_table: "gold.building_panel".to_owned(),
            gold_iceberg_snapshot_id: SNAPSHOT.to_owned(),
            reflected_gold_iceberg_snapshot_id: SNAPSHOT.to_owned(),
            pnu_prefix_length: crate::by_pnu_gateway_contract::by_pnu_serving_patch_policy()?
                .pnu_prefix_length,
            patches: Vec::new(),
            object_count: u64::try_from(rows.len())?,
            published_at_utc: "2026-01-01T00:00:00Z".to_owned(),
            verified_rebase: None,
            reflected_gold_snapshot_tag: None,
            section_packs: None,
        };
        let body = manifest.to_bytes()?;
        let sha = format!("{:x}", Sha256::digest(&body));
        store
            .write_manifest(by_pnu::manifest_key(LANE)?, &body, &sha, None)
            .await?;
        let gateway = MockServer::start().await;
        serve_capabilities(&gateway, &[1, 2, 3]).await;
        Ok(Self {
            root,
            store,
            gateway,
            work,
        })
    }

    fn output(&self) -> ProfileStoreConfig {
        ProfileStoreConfig::Local {
            root: self.root.clone(),
        }
    }

    async fn bake(
        &self,
        rows: &[JsonMap<String, JsonValue>],
        snapshot: &str,
        patch: Option<(u64, &[&str], &[&str])>,
    ) -> anyhow::Result<PackExportSummary> {
        let config = BakeConfig {
            output: self.output(),
            generation: 1,
            sections: LANE.section_packs()?.sections.clone(),
            patch: patch.map(|(number, _, deletes)| PatchTarget {
                patch: number,
                deleted: deletes.iter().map(|pnu| (*pnu).to_owned()).collect(),
            }),
            upserts: patch
                .map(|(_, upserts, _)| upserts.iter().map(|pnu| (*pnu).to_owned()).collect()),
            pnu_prefix: None,
            expected_gold_snapshot: None,
            max_concurrency: 4,
            summary_path: self.work.join("unused.json"),
        };
        bake::bake(
            &config,
            &self.store,
            &provenance(snapshot),
            rows,
            &ApprovedBuildingLinks::default(),
        )
        .await
    }

    fn summaries(&self, name: &str, summaries: &[&PackExportSummary]) -> anyhow::Result<PathBuf> {
        let dir = self.work.join(name);
        std::fs::create_dir_all(&dir)?;
        for (index, summary) in summaries.iter().enumerate() {
            std::fs::write(
                dir.join(format!("{index}.json")),
                serde_json::to_vec(summary)?,
            )?;
        }
        Ok(dir)
    }

    async fn equality(&self) -> anyhow::Result<PathBuf> {
        let config = EqualityConfig {
            output: self.output(),
            generation: 1,
            evidence_path: self.work.join("equality.json"),
            prefixes: None,
            max_concurrency: 8,
        };
        let evidence = equality::verify(&config, &self.store).await?;
        equality::write_evidence(&config.evidence_path, &evidence)?;
        Ok(config.evidence_path)
    }

    fn latency(&self, environment: &str, increase_ms: f64) -> anyhow::Result<PathBuf> {
        let policy = section_pack_policy()?;
        let live = Timings {
            p50: 40.0,
            p95: 90.0,
            mean: 45.0,
            max: 120.0,
        };
        let pack = Timings {
            p50: live.p50 + increase_ms,
            p95: live.p95 + increase_ms,
            mean: live.mean + increase_ms,
            max: live.max + increase_ms,
        };
        let size = u64::try_from(policy.cutover_gate.latency_sample_size)?;
        let evidence = LatencyEvidence {
            schema_version: policy.cutover_gate.evidence_schema_version.clone(),
            kind: gate::LATENCY_KIND.to_owned(),
            lane: LANE.unit().to_owned(),
            pack_generation: 1,
            live_base_url: "https://buildings.example.test".to_owned(),
            preview_base_url: "https://preview.example.test".to_owned(),
            environment: environment.to_owned(),
            sample_size: size,
            answered: size,
            mismatched: 0,
            failed: 0,
            increase_p50_ms: increase_ms,
            increase_p95_ms: increase_ms,
            live_ms: live,
            pack_ms: pack,
            bound_p50_ms: policy.cutover_gate.latency_max_increase_ms.p50,
            bound_p95_ms: policy.cutover_gate.latency_max_increase_ms.p95,
            examples: Vec::new(),
            passed: true,
            measured_at_utc: "2026-01-01T00:00:00Z".to_owned(),
        };
        let path = self
            .work
            .join(format!("latency-{environment}-{increase_ms}.json"));
        equality::write_evidence(&path, &evidence)?;
        Ok(path)
    }

    async fn publish(
        &self,
        summaries: PathBuf,
        snapshot: &str,
        evidence: Option<(PathBuf, PathBuf)>,
        change_set: Option<ChangeSetPaths>,
    ) -> anyhow::Result<crate::by_pnu_section_pack_manifest::SectionPacksState> {
        let config = PublishConfig {
            output: self.output(),
            summary_dir: summaries,
            expected_gold_snapshot: snapshot.to_owned(),
            expected_document_count: Some(2),
            equality_evidence: evidence.as_ref().map(|(equality, _)| equality.clone()),
            latency_evidence: evidence.map(|(_, latency)| latency),
            change_set,
        };
        publish::publish(&config, &self.store, &self.gateway.uri()).await
    }

    async fn live(&self) -> anyhow::Result<ServedManifest> {
        ServedManifest::parse(LANE, &self.store.read_manifest().await?.0)
    }

    async fn answer(&self, pnu: &str) -> anyhow::Result<Resolved> {
        let state = self
            .live()
            .await?
            .section_packs
            .context("packs are published")?;
        let view = PackView::served(&state);
        let unit = by_pnu_packs::unit_of(pnu)?;
        let mut bases = BTreeSet::new();
        for section in &view.sections {
            if self
                .store
                .list_pack_keys(&section.name, section.generation, None)
                .await?
                .contains(&by_pnu_packs::pack_key(
                    LANE,
                    &section.name,
                    section.generation,
                    None,
                    unit,
                )?)
            {
                bases.insert(section.name.clone());
            }
        }
        let packs =
            read::load_unit(&self.store, &view, unit, &|name, _| bases.contains(name)).await?;
        read::resolve(&packs, pnu)
    }
}

async fn serve_capabilities(gateway: &MockServer, versions: &[u32]) {
    gateway.reset().await;
    Mock::given(method("GET"))
        .and(path(
            LANE.policy()
                .map(|p| p.request_path.capabilities.clone())
                .unwrap_or_default(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "unit": LANE.unit(),
            "manifest_schema_versions": versions,
        })))
        .mount(gateway)
        .await;
}

fn joined(resolved: Resolved) -> anyhow::Result<Vec<u8>> {
    match resolved {
        Resolved::Document(fragments) => read::joined_bytes(&fragments),
        other => anyhow::bail!("expected a document, got {other:?}"),
    }
}

/// The whole cut-over on one lane: packs baked beside the objects, both gates, the first publish,
/// then a patch with a changed document and a tombstone. Writes the Worker's golden fixtures.
#[tokio::test]
async fn the_building_lane_cuts_over_to_packs_and_patches_them() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("cutover", &rows).await?;
    let base = lane.bake(&rows, SNAPSHOT, None).await?;
    for section in KNOWN_SECTIONS {
        let totals = base.totals.get(section).context("every section is baked")?;
        assert_eq!(
            (totals.packs, totals.documents, totals.tombstones),
            (1, 2, 0),
            "{section}"
        );
    }
    // A re-run of the same snapshot is idempotent; another snapshot into the generation is not.
    let again = lane.bake(&rows, SNAPSHOT, None).await?;
    assert!(again.packs.iter().all(|pack| pack.outcome == "reused"));
    assert!(
        lane.bake(&rows, NEXT_SNAPSHOT, None).await.is_err(),
        "a second snapshot mixed in"
    );

    // Gate (가) passes over the objects the lane serves.
    let equality = lane.equality().await?;
    let (evidence, _) = gate::read::<gate::EqualityEvidence>(&equality)?;
    assert!(evidence.passed, "{evidence:?}");
    assert_eq!((evidence.compared, evidence.equal), (2, 2));

    let summaries = lane.summaries("base", &[&base])?;
    // No evidence, simulated latency, a slow pack route, a gateway that does not read the block:
    // each refuses, and the manifest stays as it was.
    let before = lane.store.read_manifest().await?.0;
    assert!(lane
        .publish(summaries.clone(), SNAPSHOT, None, None)
        .await
        .is_err());
    let simulated = lane.latency("local-simulation", 0.0)?;
    assert!(lane
        .publish(
            summaries.clone(),
            SNAPSHOT,
            Some((equality.clone(), simulated)),
            None
        )
        .await
        .is_err());
    let slow = lane.latency(gate::PRODUCTION_ENVIRONMENT, 80.0)?;
    assert!(lane
        .publish(
            summaries.clone(),
            SNAPSHOT,
            Some((equality.clone(), slow)),
            None
        )
        .await
        .is_err());
    let fast = lane.latency(gate::PRODUCTION_ENVIRONMENT, 10.0)?;
    serve_capabilities(&lane.gateway, &[1, 2]).await;
    assert!(lane
        .publish(
            summaries.clone(),
            SNAPSHOT,
            Some((equality.clone(), fast.clone())),
            None
        )
        .await
        .is_err());
    assert_eq!(
        lane.store.read_manifest().await?.0,
        before,
        "a refused publish wrote"
    );
    serve_capabilities(&lane.gateway, &[1, 2, 3]).await;

    let state = lane
        .publish(summaries, SNAPSHOT, Some((equality, fast)), None)
        .await?;
    assert!(state.cutover.is_some() && state.reflected_gold_snapshot_tag.is_some());
    let live = lane.live().await?;
    assert_eq!(
        (live.base_generation, live.object_count),
        (1, 2),
        "the v2 fields moved"
    );
    assert_eq!(live.section_packs.as_ref(), Some(&state));
    assert_eq!(
        joined(lane.answer(PNU_A).await?)?,
        object(&rows[0], SNAPSHOT)?
    );

    // Gate (다): patch 1 changes A and deletes B.
    let changed = changed_row()?;
    let patch = lane
        .bake(
            std::slice::from_ref(&changed),
            NEXT_SNAPSHOT,
            Some((1, &[PNU_A], &[PNU_B])),
        )
        .await?;
    let change_set = write_change_set(&lane.work, (0, 1, 1), &[PNU_A], &[PNU_B])?;
    let patch_dir = lane.summaries("patch", &[&patch])?;
    let state = lane
        .publish(patch_dir, NEXT_SNAPSHOT, None, Some(change_set))
        .await?;
    assert_eq!(state.patches.len(), 1);
    assert_eq!(state.patches[0].units, vec![UNIT.to_owned()]);
    assert_eq!(state.document_count, 1);
    let patched = joined(lane.answer(PNU_A).await?)?;
    assert_eq!(patched, object(&changed, NEXT_SNAPSHOT)?);
    assert!(matches!(lane.answer(PNU_B).await?, Resolved::Tombstone));
    assert!(matches!(lane.answer(PNU_C).await?, Resolved::Absent));

    write_worker_golden(&lane, &rows, &changed)?;
    Ok(())
}

fn write_change_set(
    work: &Path,
    (new, upserts, deletes): (u64, u64, u64),
    upsert_pnus: &[&str],
    delete_pnus: &[&str],
) -> anyhow::Result<ChangeSetPaths> {
    let summary = work.join("change-set.json");
    std::fs::write(
        &summary,
        serde_json::to_vec(&json!({
            "quality_metrics": {"new_count": new, "upsert_count": upserts, "delete_count": deletes},
            "input": {"baseline_snapshot_id": SNAPSHOT, "current_snapshot_id": NEXT_SNAPSHOT},
        }))?,
    )?;
    let upsert_path = work.join("upserts.txt");
    std::fs::write(&upsert_path, upsert_pnus.join("\n"))?;
    let delete_path = work.join("deletes.txt");
    std::fs::write(&delete_path, delete_pnus.join("\n"))?;
    Ok(ChangeSetPaths {
        summary,
        upserts: upsert_path,
        deletes: delete_path,
    })
}

/// The packs, manifest and documents the Worker's tests serve and expect.
fn write_worker_golden(
    lane: &Lane,
    rows: &[JsonMap<String, JsonValue>],
    changed: &JsonMap<String, JsonValue>,
) -> anyhow::Result<()> {
    for section in KNOWN_SECTIONS {
        for (patch, name) in [
            (None, format!("g1-{section}.pack")),
            (Some(1), format!("g1-p1-{section}.pack")),
        ] {
            let key = by_pnu_packs::pack_key(LANE, section, 1, patch, UNIT)?;
            assert_golden(&name, &std::fs::read(lane.root.join(key))?)?;
        }
    }
    // The served documents are kept as strings, exactly as the object lane served them; the
    // marker names the fixture's namespace for scripts/guard/public-fixture-safety.py.
    let documents = json!({
        "namespace": "synthetic: reserved 99999 PNUs, written by the section pack tests",
        "base": {
            PNU_A: String::from_utf8(object(&rows[0], SNAPSHOT)?)?,
            PNU_B: String::from_utf8(object(&rows[1], SNAPSHOT)?)?,
        },
        "patched": { PNU_A: String::from_utf8(object(changed, NEXT_SNAPSHOT)?)? },
    });
    let mut body = serde_json::to_vec_pretty(&documents)?;
    body.push(b'\n');
    assert_golden("documents.json", &body)
}

/// Gate (가) refuses: a document that differs, an object the packs lack, a pack the objects lack.
#[tokio::test]
async fn the_equality_gate_finds_every_kind_of_difference() -> anyhow::Result<()> {
    let served = vec![spark_row()?, empty_row(PNU_B), empty_row(PNU_C)];
    let lane = Lane::serving_objects("equality", &served).await?;
    // The packs: A changed, B as served, C missing, and a PNU the objects never had.
    let extra = "9999900002100000000";
    lane.bake(
        &[changed_row()?, empty_row(PNU_B), empty_row(extra)],
        SNAPSHOT,
        None,
    )
    .await?;
    let (evidence, _) = gate::read::<gate::EqualityEvidence>(&lane.equality().await?)?;
    assert!(!evidence.passed);
    assert_eq!(
        (
            evidence.equal,
            evidence.different,
            evidence.only_served,
            evidence.only_packs
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(evidence.examples.different, vec![PNU_A.to_owned()]);
    assert_eq!(evidence.examples.only_served, vec![PNU_C.to_owned()]);
    assert_eq!(evidence.examples.only_packs, vec![extra.to_owned()]);

    // Evidence of another served state does not open the gate either.
    let mut stale = evidence.clone();
    stale.different = 0;
    stale.only_served = 0;
    stale.only_packs = 0;
    stale.equal = stale.compared;
    stale.passed = true;
    stale.served.object_count = stale.compared;
    stale.served.newest_patch = 7;
    assert!(gate::require_equality(&stale, 1, &lane.live().await?).is_err());
    Ok(())
}

/// Gate (다) refuses a patch that holds more or less than its change set, and a pack publish
/// keeps every v2 field the object lane reads.
#[tokio::test]
async fn a_patch_must_hold_exactly_its_change_set() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("patch-gate", &rows).await?;
    let base = lane.bake(&rows, SNAPSHOT, None).await?;
    let equality = lane.equality().await?;
    let latency = lane.latency(gate::PRODUCTION_ENVIRONMENT, 0.0)?;
    lane.publish(
        lane.summaries("base", &[&base])?,
        SNAPSHOT,
        Some((equality, latency)),
        None,
    )
    .await?;
    // The patch holds A, but the change set also deletes B: B's tombstone is missing.
    let patch = lane
        .bake(&[changed_row()?], NEXT_SNAPSHOT, Some((1, &[PNU_A], &[])))
        .await?;
    let change_set = write_change_set(&lane.work, (0, 1, 1), &[PNU_A], &[PNU_B])?;
    let before = lane.store.read_manifest().await?.0;
    assert!(lane
        .publish(
            lane.summaries("patch", &[&patch])?,
            NEXT_SNAPSHOT,
            None,
            Some(change_set)
        )
        .await
        .is_err());
    assert_eq!(lane.store.read_manifest().await?.0, before);
    Ok(())
}

/// Gate (나) measures, and refuses a slow or a different pack route. Both routes are local
/// stand-ins here, so the evidence says `local-simulation` and could never open the gate.
#[tokio::test]
async fn the_latency_probe_measures_and_refuses_a_slow_route() -> anyhow::Result<()> {
    let body = String::from_utf8(object(&spark_row()?, SNAPSHOT)?)?;
    // The Worker's join prints integral floats as integers; that is the same content.
    let joined = body.replace("50.0", "50");
    assert_ne!(body, joined);
    let prefix = LANE.policy()?.request_path.prefix.clone();
    let route = |delay_ms: u64, answer: String| async move {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(answer)
                    .set_delay(std::time::Duration::from_millis(delay_ms)),
            )
            .mount(&server)
            .await;
        server
    };
    let pnus = vec![PNU_A.to_owned(); 20];
    let work = std::env::temp_dir().join(format!(
        "foundation-platform-latency-{}",
        uuid::Uuid::now_v7()
    ));
    let config = |live: &MockServer, pack: &MockServer| LatencyConfig {
        generation: 1,
        live_base_url: live.uri(),
        preview_base_url: pack.uri(),
        sample: Sample::File(work.join("unused")),
        sample_size: 20,
        evidence_path: work.join("latency.json"),
    };
    let live = route(5, body.clone()).await;
    let close = route(15, joined.clone()).await;
    let evidence = latency::probe(&config(&live, &close), &pnus).await?;
    assert_eq!((evidence.answered, evidence.mismatched), (20, 0));
    assert_eq!(evidence.environment, "local-simulation");
    assert!(evidence.increase_p50_ms >= 5.0, "{evidence:?}");
    assert!(
        gate::require_latency(&evidence, 1).is_err(),
        "a simulation opened the gate"
    );

    let slow = route(5 + 200, joined).await;
    let evidence = latency::probe(&config(&live, &slow), &pnus).await?;
    assert!(evidence.increase_p95_ms > evidence.bound_p95_ms);
    assert!(!evidence.verdict()?, "a slow pack route passed");

    let different = route(5, body.replace("101호", "999호")).await;
    let evidence = latency::probe(&config(&live, &different), &pnus).await?;
    assert_eq!(evidence.mismatched, 20);
    assert!(!evidence.verdict()?, "a different answer passed");
    assert!(prefix.starts_with('/'));
    let _ = std::fs::remove_dir_all(work);
    Ok(())
}

/// The probe's sample comes from the anchor packs of the generation, every PNU at most once, and
/// a generation too small for the asked size is refused rather than measured on fewer.
#[tokio::test]
async fn the_latency_sample_spreads_over_the_anchor_packs() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B), empty_row(PNU_C)];
    let lane = Lane::serving_objects("sample", &rows).await?;
    lane.bake(&rows, SNAPSHOT, None).await?;
    let config = |size: usize| LatencyConfig {
        generation: 1,
        live_base_url: "https://live.example.test".to_owned(),
        preview_base_url: "https://preview.example.test".to_owned(),
        sample: Sample::Packs(lane.output()),
        sample_size: size,
        evidence_path: lane.work.join("latency.json"),
    };
    let mut drawn = latency::sample(&config(3)).await?;
    drawn.sort_unstable();
    assert_eq!(
        drawn,
        vec![PNU_A.to_owned(), PNU_B.to_owned(), PNU_C.to_owned()]
    );
    assert_eq!(latency::sample(&config(1)).await?.len(), 1);
    assert!(
        latency::sample(&config(4)).await.is_err(),
        "a short sample was measured"
    );
    Ok(())
}

#[test]
fn percentiles_are_nearest_rank() {
    let mut samples = (1..=100).map(f64::from).collect::<Vec<_>>();
    let timings = latency::timings(&mut samples);
    assert_eq!((timings.p50, timings.p95, timings.max), (50.0, 95.0, 100.0));
    let numbers = latency::normalized_digest(br#"{"a":50.0,"source":{"x":1}}"#).ok();
    assert_eq!(numbers, latency::normalized_digest(br#"{"a":50}"#).ok());
}
