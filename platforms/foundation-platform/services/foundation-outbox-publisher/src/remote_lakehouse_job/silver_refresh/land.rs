//! The VWorld land lanes (root ADR-0169 step 3): a release is the newest vintage whose objects
//! cover every region the lane's contract requires, read from the ZIP member names measured beside
//! the ledger (`catalog.bronze_object_member`, step 1).
//!
//! The ledger's own fields cannot say this: a VWorld object's `provider_file_name` is the dataset
//! title, and which 시도 and which harvest it holds is written only in the CSV inside
//! (`AL_D151_<시도>_<YYYYMMDD>.csv`). The contract states the rule that reads them
//! (`silver_refresh.member_name`) and how many regions make a release (`completeness`); the
//! objects are the ledger's. An object nobody has measured yet is refused, never guessed.
//!
//! A release replaces the table (gold.parcel_panel reads one `source_snapshot_id` per land table):
//! each object is exported to its own handoff by the lane's existing export, then the handoffs are
//! loaded in batches, the first overwriting and the rest appending. The table's ingest record
//! names each Bronze object (`source_record_id`), so a run that stopped part way resumes at the
//! first batch it does not hold, and a run of a loaded release writes nothing.
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{bail, ensure, Context};
use chrono::NaiveDate;
use lakehouse_infrastructure::IcebergRestCatalog;
use regex::Regex;
use sqlx::{PgPool, Row};

use super::{
    execute::{self, Runtime, SparkLoad},
    lane::{ExportKind, LaneContract, WriteMode},
    release::{self, LedgerObject},
    Decision,
};

/// A read bound: the largest land source holds about a thousand objects of a few members each.
const MAX_MEMBER_ROWS: i64 = 200_000;
/// The command that measures what the ledger holds and nobody has read yet (root ADR-0169 §1).
pub(in crate::remote_lakehouse_job) const MEASURE_COMMAND: &str =
    "scripts/ops/bronze-object-members.sh";
/// The region of a national lane's one object.
const NATIONAL: &str = "national";

/// How many regions make a release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) enum Completeness {
    /// One object for each of `count` 시도, all of one vintage.
    Sido { count: usize },
    /// One national object; its vintage is its ledger snapshot date.
    National,
}

/// A land lane's rule, from its contract (`lane.rs` checks it).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct LandRule {
    /// The one Bronze source slug the lane reads.
    pub source: String,
    /// The member the export reads; for 시도 lanes, with the named groups `region` and `vintage`.
    pub member_name: String,
    pub completeness: Completeness,
    /// `source_snapshot_id` is this label, `:` and the release: the source slug in hyphens, the
    /// label the hand loads wrote (`lane.rs` derives it).
    pub snapshot_label: String,
    pub handoff_prefix: String,
    pub handoff_suffix: String,
}

impl LandRule {
    /// The member rule, refused unless it is anchored and names exactly the groups its
    /// completeness reads.
    pub(in crate::remote_lakehouse_job) fn member_regex(&self) -> anyhow::Result<Regex> {
        ensure!(
            self.member_name.starts_with('^') && self.member_name.ends_with('$'),
            "member_name must match a whole member name (^…$)"
        );
        let regex = Regex::new(&self.member_name)
            .with_context(|| format!("member_name {:?} is not a regex", self.member_name))?;
        let names: BTreeSet<&str> = regex.capture_names().flatten().collect();
        let wanted: BTreeSet<&str> = match self.completeness {
            Completeness::Sido { .. } => ["region", "vintage"].into(),
            Completeness::National => BTreeSet::new(),
        };
        ensure!(
            names == wanted,
            "member_name names the groups {names:?}; a {:?} lane reads {wanted:?}",
            self.completeness
        );
        Ok(regex)
    }

    /// Where the export puts one object's handoff: the prefix, the object's file stem and the
    /// suffix (the names the hand loads used, so a handoff already there is found).
    pub(in crate::remote_lakehouse_job) fn handoff_key(
        &self,
        object: &LedgerObject,
    ) -> anyhow::Result<String> {
        let stem = object
            .file_name()
            .strip_suffix(".zip")
            .with_context(|| format!("{} is not a ZIP", object.object_key))?;
        Ok(format!(
            "{}/{stem}{}",
            self.handoff_prefix, self.handoff_suffix
        ))
    }
}

/// What the settled reading of one ledger object found (`catalog.bronze_object_measurement`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) enum Reading {
    /// No settled reading yet (none, or only `failed` ones).
    Unmeasured,
    /// A ZIP, with its member names in directory order.
    Zip(Vec<String>),
    /// Read and settled as not a ZIP or not readable: no member to load.
    NotLoadable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct MeasuredObject {
    pub object: LedgerObject,
    pub reading: Reading,
}

/// One object the lane could load: the region and vintage its member names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct Candidate {
    pub region: String,
    pub vintage: String,
    pub member: String,
    pub object: LedgerObject,
}

/// The objects of the source the lane could load, and why any were left out.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct Candidates {
    pub found: Vec<Candidate>,
    /// Objects whose members the export would refuse (two members it reads, a vintage that is
    /// no date), for the log; they are never loaded.
    pub refused: Vec<String>,
}

/// Reads the members of every object of the source. An object without a settled reading refuses
/// the run: it may hold the newest vintage, and the release cannot be told without it.
/// `converts` is the export's own member test: an object the export would refuse (more than one
/// member it converts) is left out here rather than failing the run part way.
pub(in crate::remote_lakehouse_job) fn candidates(
    rule: &LandRule,
    measured: &[MeasuredObject],
    converts: impl Fn(&str) -> bool,
) -> anyhow::Result<Candidates> {
    let regex = rule.member_regex()?;
    let unmeasured: Vec<&str> = measured
        .iter()
        .filter(|object| object.reading == Reading::Unmeasured)
        .map(|object| object.object.object_key.as_str())
        .collect();
    ensure!(
        unmeasured.is_empty(),
        "{} Bronze objects of {} have no ZIP member reading yet, so the release cannot be told \
         (first: {}). Measure them, then run again: {MEASURE_COMMAND} {} (root ADR-0169 §1)",
        unmeasured.len(),
        rule.source,
        unmeasured
            .iter()
            .take(3)
            .copied()
            .collect::<Vec<_>>()
            .join(", "),
        rule.source
    );
    let mut result = Candidates::default();
    for measured in measured {
        let Reading::Zip(members) = &measured.reading else {
            continue;
        };
        let object = &measured.object;
        let matching: Vec<&String> = members
            .iter()
            .filter(|name| regex.is_match(name.as_str()))
            .collect();
        let converted = members
            .iter()
            .filter(|name| converts(name.as_str()))
            .count();
        let name = match matching.as_slice() {
            [] => continue,
            [name] if converted == 1 && converts(name.as_str()) => *name,
            many => {
                result.refused.push(format!(
                    "{} holds {} members the lane's rule reads and {converted} its export converts",
                    object.object_key,
                    many.len()
                ));
                continue;
            }
        };
        ensure!(
            object
                .object_key
                .strip_prefix(&format!("bronze/source={}/", rule.source))
                .is_some_and(|file| !file.contains('/') && file.ends_with(".zip")),
            "{} is not a ZIP of {}",
            object.object_key,
            rule.source
        );
        let (region, vintage) = match rule.completeness {
            Completeness::Sido { .. } => {
                let captures = regex
                    .captures(name)
                    .context("a matched name has captures")?;
                (
                    captures
                        .name("region")
                        .context("the rule names a region")?
                        .as_str()
                        .to_owned(),
                    captures
                        .name("vintage")
                        .context("the rule names a vintage")?
                        .as_str()
                        .to_owned(),
                )
            }
            Completeness::National => (
                NATIONAL.to_owned(),
                object.snapshot_date.format("%Y%m%d").to_string(),
            ),
        };
        if vintage.len() != 8 || NaiveDate::parse_from_str(&vintage, "%Y%m%d").is_err() {
            result.refused.push(format!(
                "{} names the vintage {vintage:?}, which is no date",
                object.object_key
            ));
            continue;
        }
        result.found.push(Candidate {
            region,
            vintage,
            member: name.clone(),
            object: object.clone(),
        });
    }
    Ok(result)
}

/// The release a land run loads: one object per region, all of one vintage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct LandRelease {
    /// `YYYYMMDD`: the vintage in the member names, or a national object's ledger date.
    pub vintage: String,
    /// Region to the object loaded for it, in region order (the load batches follow it).
    pub objects: Vec<(String, LedgerObject)>,
    /// Newer vintages that did not cover the regions, for the log; they are not loaded.
    pub skipped_incomplete: Vec<String>,
}

impl LandRelease {
    /// The rows' `source_snapshot_id`: the label and the vintage, or for a national lane the
    /// object's file stem (the identities the hand loads wrote, so a loaded release reads as one).
    pub(in crate::remote_lakehouse_job) fn source_snapshot_id(
        &self,
        rule: &LandRule,
    ) -> anyhow::Result<String> {
        Ok(match rule.completeness {
            Completeness::Sido { .. } => format!("{}:{}", rule.snapshot_label, self.vintage),
            Completeness::National => {
                let [(_, object)] = self.objects.as_slice() else {
                    bail!("a national release is one object");
                };
                let stem = object
                    .file_name()
                    .strip_suffix(".zip")
                    .context("a land object is a ZIP")?;
                format!("{}:{stem}", rule.snapshot_label)
            }
        })
    }

    /// The Bronze keys the release loads, which the table's ingest record names once loaded.
    pub(in crate::remote_lakehouse_job) fn object_keys(&self) -> Vec<String> {
        self.objects
            .iter()
            .map(|(_, object)| object.object_key.clone())
            .collect()
    }
}

/// Picks the newest complete release.
///
/// Vintages are tried newest first. A vintage whose objects do not name exactly the contract's
/// count of regions is skipped (named in `skipped_incomplete`). In the first complete vintage, a
/// region with two objects keeps the one the provider updated later; two with the same or no
/// update date are refused (`release::newest`, the hub lanes' rule), never decided by key order.
pub(in crate::remote_lakehouse_job) fn select(
    rule: &LandRule,
    candidates: &[Candidate],
) -> anyhow::Result<LandRelease> {
    let required = match rule.completeness {
        Completeness::Sido { count } => count,
        Completeness::National => 1,
    };
    let vintages: BTreeSet<&str> = candidates
        .iter()
        .map(|candidate| candidate.vintage.as_str())
        .collect();
    let mut skipped_incomplete = Vec::new();
    for vintage in vintages.into_iter().rev() {
        let mut by_region: BTreeMap<&str, Vec<&LedgerObject>> = BTreeMap::new();
        for candidate in candidates.iter().filter(|c| c.vintage == vintage) {
            by_region
                .entry(candidate.region.as_str())
                .or_default()
                .push(&candidate.object);
        }
        if by_region.len() != required {
            skipped_incomplete.push(format!("{vintage} regions={}", by_region.len()));
            continue;
        }
        let mut objects = Vec::new();
        for (region, held) in by_region {
            let chosen = release::newest(&held)
                .with_context(|| format!("region {region} of vintage {vintage}"))?
                .context("a region with objects has a newest one")?;
            objects.push((region.to_owned(), chosen.clone()));
        }
        return Ok(LandRelease {
            vintage: vintage.to_owned(),
            objects,
            skipped_incomplete,
        });
    }
    bail!(
        "the ledger holds no vintage of {} covering {required} regions (incomplete: {})",
        rule.source,
        if skipped_incomplete.is_empty() {
            "none; no object matched the member rule".to_owned()
        } else {
            skipped_incomplete.join(", ")
        }
    )
}

/// Whether the table already holds the release: every object of it recorded, or an object of a
/// newer vintage recorded (never reverted to an older release).
pub(in crate::remote_lakehouse_job) fn decide(
    release: &LandRelease,
    candidates: &[Candidate],
    ingested: &BTreeSet<String>,
) -> Decision {
    if release
        .objects
        .iter()
        .all(|(_, object)| ingested.contains(&object.object_key))
    {
        return Decision::Unchanged("already_loaded");
    }
    if candidates.iter().any(|candidate| {
        candidate.vintage > release.vintage && ingested.contains(&candidate.object.object_key)
    }) {
        return Decision::Unchanged("newer_release_loaded");
    }
    Decision::ExportAndLoad
}

/// The write of load batch `index`: the first replaces the table's previous release.
pub(in crate::remote_lakehouse_job) const fn batch_write_mode(index: usize) -> WriteMode {
    if index == 0 {
        WriteMode::Overwrite
    } else {
        WriteMode::Append
    }
}

/// The environment of one object's export (`land_use_silver_export`'s `<prefix>_*` names).
pub(in crate::remote_lakehouse_job) fn export_environment(
    env_prefix: &str,
    object: &LedgerObject,
    handoff_key: &str,
    source_snapshot_id: &str,
    summary_path: &Path,
) -> Vec<(String, String)> {
    [
        ("INPUT_OBJECT_KEY", object.object_key.clone()),
        ("OUTPUT_OBJECT_KEY", handoff_key.to_owned()),
        ("SOURCE_SNAPSHOT_ID", source_snapshot_id.to_owned()),
        ("SUMMARY_PATH", summary_path.to_string_lossy().into_owned()),
    ]
    .into_iter()
    .map(|(name, value)| (format!("{env_prefix}_{name}"), value))
    .collect()
}

/// The rows one export reports: `Some` for a conversion, `None` when the handoff was already in
/// the bucket (the summary then has no counts). Refuses a summary of another snapshot id.
pub(in crate::remote_lakehouse_job) fn export_rows(
    raw: &str,
    source_snapshot_id: &str,
) -> anyhow::Result<Option<u64>> {
    let summary: serde_json::Value =
        serde_json::from_str(raw).context("the export summary is not JSON")?;
    ensure!(
        summary["source"]["source_snapshot_id"].as_str() == Some(source_snapshot_id),
        "the export summary names another source_snapshot_id"
    );
    match summary["outcome"].as_str() {
        Some("converted") => Ok(Some(
            summary["output"]["row_count"]
                .as_u64()
                .context("a conversion summary counts its rows")?,
        )),
        Some("already_present") => Ok(None),
        other => bail!("the export summary's outcome is {other:?}"),
    }
}

/// Reads every object of the source with its settled reading's member names (bounded).
pub(in crate::remote_lakehouse_job) async fn read_measured(
    pool: &PgPool,
    source: &str,
) -> anyhow::Result<Vec<MeasuredObject>> {
    let rows = sqlx::query(
        "SELECT b.id::text AS id, s.slug, b.object_key, b.snapshot_date, b.provider_updated_at,
                b.size_bytes, b.checksum_sha256, m.outcome, member.member_name
         FROM catalog.bronze_object b
         JOIN catalog.source_catalog s ON s.id = b.source_catalog_id
         LEFT JOIN catalog.bronze_object_measurement m
                ON m.bronze_object_id = b.id AND m.outcome <> 'failed'
         LEFT JOIN catalog.bronze_object_member member ON member.measurement_id = m.id
         WHERE s.slug = $1
         ORDER BY b.object_key, b.id, member.member_index
         LIMIT $2",
    )
    .bind(source)
    .bind(MAX_MEMBER_ROWS + 1)
    .fetch_all(pool)
    .await
    .context("cannot read the Bronze ledger and its ZIP members")?;
    ensure!(
        i64::try_from(rows.len())? <= MAX_MEMBER_ROWS,
        "{source} holds more than {MAX_MEMBER_ROWS} ledger member rows"
    );
    let mut objects: BTreeMap<String, MeasuredObject> = BTreeMap::new();
    for row in &rows {
        let id: String = row.try_get("id")?;
        let member: Option<String> = row.try_get("member_name")?;
        let measured = match objects.entry(id) {
            Entry::Occupied(found) => found.into_mut(),
            Entry::Vacant(slot) => {
                let outcome: Option<String> = row.try_get("outcome")?;
                let reading = match outcome.as_deref() {
                    None => Reading::Unmeasured,
                    Some("zip") => Reading::Zip(Vec::new()),
                    Some("not_zip" | "unreadable") => Reading::NotLoadable,
                    Some(other) => bail!("unknown settled measurement outcome {other:?}"),
                };
                let object = LedgerObject {
                    slug: row.try_get("slug")?,
                    object_key: row.try_get("object_key")?,
                    snapshot_date: row.try_get("snapshot_date")?,
                    provider_updated_at: row.try_get("provider_updated_at")?,
                    size_bytes: u64::try_from(row.try_get::<i64, _>("size_bytes")?)?,
                    checksum_sha256: row.try_get("checksum_sha256")?,
                };
                slot.insert(MeasuredObject { object, reading })
            }
        };
        if let (Some(member), Reading::Zip(members)) = (member, &mut measured.reading) {
            members.push(member);
        }
    }
    Ok(objects.into_values().collect())
}

/// Exports and loads a release `decide` found the table lacks; returns the rows the loads hold.
pub(in crate::remote_lakehouse_job) async fn load(
    contract: &LaneContract,
    rule: &LandRule,
    release: &LandRelease,
    runtime: &Runtime,
    catalog: &IcebergRestCatalog,
) -> anyhow::Result<u64> {
    let ExportKind::Land { env_prefix } = contract.kind else {
        bail!("a land release runs a land export");
    };
    let identity = release.source_snapshot_id(rule)?;
    let lane = contract.lane.id();
    runtime.reset_work()?;
    let work = runtime.work();
    let mut handoffs = Vec::new();
    for (region, object) in &release.objects {
        let handoff = rule.handoff_key(object)?;
        let summary = work.join(format!("export-summary-{region}.json"));
        println!(
            "silver-refresh lane={lane} release={} region={region}: exporting {} to {handoff}",
            release.vintage, object.object_key
        );
        execute::export(
            runtime,
            contract,
            &export_environment(env_prefix, object, &handoff, &identity, &summary),
        )?;
        let raw = std::fs::read_to_string(&summary)
            .with_context(|| format!("the export left no {}", summary.display()))?;
        handoffs.push((handoff, export_rows(&raw, &identity)?));
    }
    let bucket = super::super::required_lookup(
        &mut |name: &str| std::env::var(name).ok(),
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
    )?;
    let evidence_name = format!("{lane}-{}", release.vintage);
    let batch_size = usize::try_from(contract.spark.input_file_batch_size.max(1)).unwrap_or(1);
    let mut rows = 0;
    for (index, batch) in handoffs.chunks(batch_size).enumerate() {
        let load = SparkLoad {
            input: batch
                .iter()
                .map(|(key, _)| format!("s3a://{bucket}/{key}"))
                .collect::<Vec<_>>()
                .join(","),
            input_format: "jsonl",
            // Known only when every handoff of the batch was converted by this run.
            expected_count: batch.iter().map(|(_, converted)| *converted).sum(),
            reads_r2: true,
            write_mode: batch_write_mode(index),
        };
        // A batch the table already holds (a resumed run) is reported and skipped by
        // append_batch_once, the overwrite of the first included.
        let summary = execute::spark(runtime, contract, &load)?;
        ensure!(
            !summary.source_snapshot_truncated
                && summary.source_snapshot_ids.as_slice() == std::slice::from_ref(&identity),
            "load batch {index} holds the source_snapshot_ids {:?}, not {identity}: a handoff \
             already in the bucket was written under another id, and gold.parcel_panel reads one",
            summary.source_snapshot_ids
        );
        execute::keep_evidence(runtime, &evidence_name, index)?;
        rows += summary.persisted_row_count.unwrap_or(summary.row_count);
    }
    let summaries: Vec<String> = release
        .objects
        .iter()
        .map(|(region, _)| format!("export-summary-{region}.json"))
        .collect();
    execute::keep_files(runtime, &evidence_name, &summaries)?;
    super::confirm_recorded(catalog, &contract.table, &release.object_keys()).await?;
    runtime.reset_work()?;
    Ok(rows)
}
