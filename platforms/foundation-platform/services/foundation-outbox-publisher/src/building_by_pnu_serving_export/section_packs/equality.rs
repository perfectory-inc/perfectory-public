//! `verify-building-by-pnu-section-pack-equality`: gate (가) of the cut-over (root ADR-0147 §6).
//!
//! Reads every PNU the manifest's objects serve (the newest patch's object, else the base's) and
//! every PNU an unpublished pack generation holds, and compares each answer by content: the
//! document with `source` removed, keys sorted, compact (the digest the verified re-base uses,
//! root ADR-0146 §1). The pack answer is joined the way the gateway joins it (`read.rs`), so a
//! section that disagrees with its anchor counts as unreadable, not as equal.
//!
//! Read-only. The evidence file it writes is what the first pack publish demands; it names the
//! object state it compared against, so a publish over a newer object patch needs a new check.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};

use super::super::{optional_env, parse_max_concurrency, LANE};
use super::gate::{self, EqualityEvidence, Examples, ServedObjects};
use super::read::{self, PackView, Resolved};
use crate::by_pnu_gateway_contract::section_pack_policy;
use crate::by_pnu_serving_manifest::{read_tombstone, ServedManifest};
use crate::by_pnu_serving_rebase::content_digest;
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{by_pnu, by_pnu_packs};

const DEFAULT_MAX_CONCURRENCY: usize = 64;
/// Object reads in flight inside one legal dong.
const READS_PER_UNIT: usize = 8;
const READ_ATTEMPTS: usize = 3;

#[derive(Clone, Debug)]
pub(crate) struct EqualityConfig {
    pub(crate) output: ProfileStoreConfig,
    pub(crate) generation: u64,
    pub(crate) evidence_path: PathBuf,
    /// `None`: every PNU, the only run that can open the gate. `Some`: these PNU prefixes.
    pub(crate) prefixes: Option<Vec<String>>,
    pub(crate) max_concurrency: usize,
}

impl EqualityConfig {
    fn from_env() -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&LANE.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
        };
        Ok(Self {
            output: ProfileStoreConfig::parse(
                env("OUTPUT_STORAGE_DRIVER")?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(env("OUTPUT_ROOT")?),
            )?,
            generation: required("PACK_GENERATION")?
                .parse()
                .context("the pack generation must be a number")?,
            evidence_path: PathBuf::from(required("PACK_EQUALITY_EVIDENCE_PATH")?),
            prefixes: env("PACK_EQUALITY_PREFIXES")?.map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|prefix| !prefix.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            }),
            max_concurrency: env("MAX_CONCURRENCY")?
                .map(|raw| parse_max_concurrency(&raw))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
        })
    }
}

/// Runs the check and writes the evidence; fails when the evidence does not pass.
///
/// # Errors
/// Returns an error when reading fails or the evidence does not pass.
pub(crate) async fn run() -> anyhow::Result<()> {
    super::sections::check_contract_sections()?;
    let config = EqualityConfig::from_env()?;
    let store = ByPnuServingStore::open(LANE, &config.output)?;
    let evidence = verify(&config, &store).await?;
    write_evidence(&config.evidence_path, &evidence)?;
    tracing::info!(
        compared = evidence.compared,
        equal = evidence.equal,
        different = evidence.different,
        only_served = evidence.only_served,
        only_packs = evidence.only_packs,
        unreadable = evidence.unreadable,
        passed = evidence.passed,
        "building section pack equality checked"
    );
    ensure!(
        evidence.passed || !evidence.complete,
        "the packs do not serve what the objects serve; see {}",
        config.evidence_path.display()
    );
    Ok(())
}

pub(crate) fn write_evidence(
    path: &std::path::Path,
    evidence: &impl serde::Serialize,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut body = serde_json::to_vec_pretty(evidence)?;
    body.push(b'\n');
    std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

#[derive(Default)]
struct Tally {
    compared: u64,
    equal: u64,
    different: u64,
    only_served: u64,
    only_packs: u64,
    unreadable: u64,
    examples: Examples,
}

impl Tally {
    fn add(&mut self, other: Self) {
        self.compared += other.compared;
        self.equal += other.equal;
        self.different += other.different;
        self.only_served += other.only_served;
        self.only_packs += other.only_packs;
        self.unreadable += other.unreadable;
        for (into, from) in [
            (&mut self.examples.different, other.examples.different),
            (&mut self.examples.only_served, other.examples.only_served),
            (&mut self.examples.only_packs, other.examples.only_packs),
            (&mut self.examples.unreadable, other.examples.unreadable),
        ] {
            into.extend(from);
            into.sort_unstable();
            into.truncate(gate::MAX_EXAMPLES);
        }
    }

    fn note(list: &mut Vec<String>, pnu: &str) {
        if list.len() < gate::MAX_EXAMPLES {
            list.push(pnu.to_owned());
        }
    }
}

/// Compares and returns the evidence; does not write it.
///
/// # Errors
/// Returns an error when the live manifest or a listing cannot be read.
pub(crate) async fn verify(
    config: &EqualityConfig,
    store: &ByPnuServingStore,
) -> anyhow::Result<EqualityEvidence> {
    let started = Instant::now();
    let started_at_utc = crate::by_pnu_serving_manifest_publish::now();
    let (bytes, _) = store.read_manifest().await?;
    let served = ServedManifest::parse(LANE, &bytes)?;
    let view = PackView::unpublished(config.generation)?;
    let sections = LANE.section_packs()?.sections.clone();
    let mut base_units: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for section in &sections {
        let units = store
            .list_pack_keys(section, config.generation, None)
            .await?
            .iter()
            .filter_map(|key| by_pnu_packs::parse_pack_key(LANE, key))
            .map(|parsed| parsed.unit)
            .collect();
        base_units.insert(section.clone(), units);
    }
    let shards = match &config.prefixes {
        Some(prefixes) => prefixes.clone(),
        None => (0..100).map(|n| format!("{n:02}")).collect(),
    };
    let mut tally = Tally::default();
    for shard in &shards {
        tally.add(compare_shard(config, store, &served, &view, &base_units, shard).await?);
    }
    let complete = config.prefixes.is_none();
    let mut evidence = EqualityEvidence {
        schema_version: section_pack_policy()?
            .cutover_gate
            .evidence_schema_version
            .clone(),
        kind: gate::EQUALITY_KIND.to_owned(),
        lane: LANE.unit().to_owned(),
        pack_generation: config.generation,
        sections,
        served: ServedObjects::of(&served),
        complete,
        compared: tally.compared,
        equal: tally.equal,
        different: tally.different,
        only_served: tally.only_served,
        only_packs: tally.only_packs,
        unreadable: tally.unreadable,
        examples: tally.examples,
        passed: false,
        started_at_utc,
        finished_at_utc: crate::by_pnu_serving_manifest_publish::now(),
        elapsed_seconds: started.elapsed().as_secs_f64(),
    };
    evidence.passed = evidence.verdict();
    Ok(evidence)
}

/// The served object key of every PNU under `shard`: newest patch first, else the base.
async fn served_keys(
    store: &ByPnuServingStore,
    served: &ServedManifest,
    shard: &str,
) -> anyhow::Result<HashMap<String, String>> {
    let mut keys = HashMap::new();
    for key in store
        .list_existing_generation_keys(served.base_generation, Some(shard))
        .await?
    {
        let pnu = key
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".json"))
            .with_context(|| format!("{key} is not an object key"))?
            .to_owned();
        keys.insert(pnu, key);
    }
    for patch in served.patches.iter().rev() {
        for key in store
            .list_patch_keys(served.base_generation, patch.generation, Some(shard))
            .await?
        {
            let (_, _, pnu) = by_pnu::parse_patch_object_key(LANE, &key)
                .with_context(|| format!("{key} is not a patch object key"))?;
            keys.insert(pnu, key);
        }
    }
    Ok(keys)
}

async fn compare_shard(
    config: &EqualityConfig,
    store: &ByPnuServingStore,
    served: &ServedManifest,
    view: &PackView,
    base_units: &BTreeMap<String, BTreeSet<String>>,
    shard: &str,
) -> anyhow::Result<Tally> {
    let served_keys = served_keys(store, served, shard).await?;
    let mut by_unit: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (pnu, key) in served_keys {
        by_unit
            .entry(by_pnu_packs::unit_of(&pnu)?.to_owned())
            .or_default()
            .push((pnu, key));
    }
    for units in base_units.values() {
        for unit in units.iter().filter(|unit| unit.starts_with(shard)) {
            by_unit.entry(unit.clone()).or_default();
        }
    }
    let has_base = |section: &str, unit: &str| {
        base_units
            .get(section)
            .is_some_and(|units| units.contains(unit))
    };
    let mut jobs = Vec::with_capacity(by_unit.len());
    for (unit, objects) in by_unit {
        jobs.push(compare_unit(store, view, unit, objects, &has_base));
    }
    let tallies = stream::iter(jobs)
        .buffer_unordered(config.max_concurrency.div_ceil(READS_PER_UNIT).max(1))
        .try_collect::<Vec<_>>()
        .await?;
    let mut tally = Tally::default();
    for one in tallies {
        tally.add(one);
    }
    Ok(tally)
}

async fn compare_unit(
    store: &ByPnuServingStore,
    view: &PackView,
    unit: String,
    mut objects: Vec<(String, String)>,
    has_base: &(dyn Fn(&str, &str) -> bool + Sync),
) -> anyhow::Result<Tally> {
    objects.sort_unstable();
    let packs = read::load_unit(store, view, &unit, has_base).await?;
    let mut reads = Vec::with_capacity(objects.len());
    for (pnu, key) in &objects {
        reads.push(read_served_of(store, key, pnu));
    }
    let read_back = stream::iter(reads)
        .buffer_unordered(READS_PER_UNIT)
        .try_collect::<HashMap<_, _>>()
        .await?;
    let mut pnus = read::anchor_pnus(&packs)?
        .into_iter()
        .chain(objects.iter().map(|(pnu, _)| pnu.clone()))
        .collect::<Vec<_>>();
    pnus.sort_unstable();
    pnus.dedup();
    let mut tally = Tally::default();
    for pnu in &pnus {
        let served = read_back.get(pnu).cloned().flatten();
        let pack = match read::resolve(&packs, pnu).and_then(|resolved| match resolved {
            Resolved::Document(fragments) => {
                content_digest(&read::joined_bytes(&fragments)?).map(Some)
            }
            Resolved::Tombstone | Resolved::Absent => Ok(None),
        }) {
            Ok(pack) => pack,
            Err(error) => {
                tracing::warn!(pnu = %pnu, error = %format!("{error:#}"), "pack answer unreadable");
                tally.compared += 1;
                tally.unreadable += 1;
                Tally::note(&mut tally.examples.unreadable, pnu);
                continue;
            }
        };
        match (served, pack) {
            (None, None) => {}
            (Some(served), Some(pack)) => {
                tally.compared += 1;
                if served == pack {
                    tally.equal += 1;
                } else {
                    tally.different += 1;
                    Tally::note(&mut tally.examples.different, pnu);
                }
            }
            (Some(_), None) => {
                tally.compared += 1;
                tally.only_served += 1;
                Tally::note(&mut tally.examples.only_served, pnu);
            }
            (None, Some(_)) => {
                tally.compared += 1;
                tally.only_packs += 1;
                Tally::note(&mut tally.examples.only_packs, pnu);
            }
        }
    }
    Ok(tally)
}

async fn read_served_of(
    store: &ByPnuServingStore,
    key: &str,
    pnu: &str,
) -> anyhow::Result<(String, Option<crate::by_pnu_serving_rebase::ContentDigest>)> {
    Ok((pnu.to_owned(), read_served(store, key, pnu).await?))
}

/// The content digest of a served object; `None` for a tombstone of its own PNU.
async fn read_served(
    store: &ByPnuServingStore,
    key: &str,
    pnu: &str,
) -> anyhow::Result<Option<crate::by_pnu_serving_rebase::ContentDigest>> {
    let mut last = None;
    for _ in 0..READ_ATTEMPTS {
        match store.read_bytes(key).await {
            Ok(bytes) => {
                if let Some(tombstone) = read_tombstone(&bytes) {
                    ensure!(tombstone.pnu == pnu, "{key} is a tombstone of another PNU");
                    return Ok(None);
                }
                return content_digest(&bytes).map(Some);
            }
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no read was attempted")))
        .with_context(|| format!("{key} could not be read in {READ_ATTEMPTS} attempts"))
}
