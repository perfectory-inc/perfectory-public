//! The 시군구 crosswalk the hub exports compose PNUs through (root ADR-0103, ADR-0142, ADR-0143,
//! ADR-0145).
//!
//! The HUB building-register feed carries the authority-current merged 시군구 code (12xxx,
//! 전남광주통합특별시) while the cadastral map still keys parcels by the superseded codes (29xxx 광주
//! / 46xxx 전남).
//!
//! **The crosswalk is a view of the code change table, not a table of its own (root ADR-0145).**
//! `reference.legal_dong_code_change` is the one record of region code changes; the legal-dong
//! pairing job (`infra/lakehouse/spark/jobs/legal_dong_code_change_pairs.py`) appends to it and
//! writes the 시군구 view of it as a projection file, old → new: the pairs, the merged 시도 they
//! govern, the full-table snapshot it was built from and the change table snapshot it was read at.
//! [`hub_sigungu_crosswalk`] reads that file from [`PROJECTION_ENV`] and refuses
//!
//! - no path, an unreadable file, or another schema;
//! - a stale projection: the latest-snapshot marker the snapshot loader writes beside it
//!   ([`LATEST_MARKER_FILE`]) names a newer full table than the one the projection was built from,
//!   or the collector's state beside it ([`COLLECTION_STATE_FILE`]) handed off a table the
//!   projection was not built from;
//! - a projection no collection has confirmed lately: the collector's last successful check is
//!   older than `projection.max_age_days` of `code-go-kr-legal-dong.contract.json`;
//! - a projection the change table has moved past: the [`CHANGE_TABLE`] snapshot it names is not
//!   the table's current snapshot in the Iceberg catalog;
//! - while `projection.baseline_comparison.required` holds, a projection that disagrees with the 27
//!   hand pairs of `sigungu-crosswalk-baseline.json` in a 시도 they govern. Those pairs are
//!   a test fixture; the comparison stays until a live pairing run reproduces them, and that run's
//!   evidence in the contract turns it off ([`BaselineComparison`]).
//!
//! The hub's placeholder codes and the absent-시도 row bound are facts about the HUB feed, not about
//! code changes; they live in `hub-register-feed.contract.json`.
//!
//! The kernel stays pure: it never reads infra files; every export layer loads the crosswalk here
//! and passes it in. A 시도 the crosswalk governs is not commentary: every hub 시군구 code under it
//! must have a mapping and an unmapped one stops the export by name (ADR-0142). Codes outside the
//! governed 시도 pass through composition unchanged (identity), except the declared placeholder
//! codes, which compose no PNU.
//!
//! An ungoverned code whose 시도 the cadastral parcel set does not carry composes a PNU no parcel
//! has — an orphan, invisible to every NULL-share check. The [`SidoTally`] counts rows by 시도
//! against the cadastral set the parcel source contract (`vworld-parcel-source-objects.json`)
//! records, and refuses an export whose rows under one such 시도 exceed the hub feed contract's
//! `absent_sido_row_bound` (ADR-0142).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::pnu::SigunguCrosswalk;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Deserialize;
use serde_json::{json, Value};

const BASELINE_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/sigungu-crosswalk-baseline.json");
const HUB_FEED_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/hub-register-feed.contract.json");
const PARCEL_SOURCE_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");
const BASELINE_FILE: &str = "sigungu-crosswalk-baseline.json";
const HUB_FEED_FILE: &str = "hub-register-feed.contract.json";

use crate::code_go_kr_legal_dong_contract::CONTRACT_JSON;

/// The 27 hand pairs, the comparison baseline while it is required. Old → new in the projection's
/// shape, so the repository holds code pairs in one direction (root ADR-0145).
#[derive(Deserialize)]
struct Baseline {
    sido: Vec<ProjectionSido>,
    sigungu: Vec<ProjectionPair>,
}

/// A merged 시도 and the 시도 it supersedes, internal direction (current → superseded).
struct Governed {
    current_code: String,
    supersedes: Vec<String>,
}

/// One 시군구 pair, internal direction (current → superseded).
struct Pair {
    current_code: String,
    superseded_code: String,
}

#[derive(Deserialize)]
struct HubFeed {
    placeholder_sigungu: HubPlaceholders,
    absent_sido_row_bound: HubBound,
}

#[derive(Deserialize)]
struct HubPlaceholders {
    codes: Vec<String>,
}

#[derive(Deserialize)]
struct HubBound {
    rows: u64,
}

#[derive(Deserialize)]
struct ParcelSource {
    objects: Vec<ParcelSourceObject>,
}

#[derive(Deserialize)]
struct ParcelSourceObject {
    region_code: String,
    granularity: String,
}

/// The environment variable naming the crosswalk projection the pairing job wrote.
pub const PROJECTION_ENV: &str = "FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION";
/// The marker the snapshot loader writes beside the projection after each code.go.kr snapshot.
pub const LATEST_MARKER_FILE: &str = "latest-legal-dong-snapshot.json";
/// The collector's state beside the projection: the table it last handed off, and when a
/// collection last confirmed it (`code_go_kr_legal_dong.py stage-handoff`).
pub const COLLECTION_STATE_FILE: &str = "accepted.json";
/// The Iceberg table the projection is a view of, and whose snapshot it names (root ADR-0145).
pub const CHANGE_TABLE: &str = "reference.legal_dong_code_change";
const PROJECTION_SCHEMA: &str = "foundation-platform.sigungu_crosswalk_projection.v2";
const RUNBOOK: &str = "docs/runbooks/legal-dong-code-changes.md";

/// The projection file: the 시군구 view of the change table, old → new like the table.
#[derive(Deserialize)]
struct Projection {
    schema_version: String,
    legal_dong_snapshot_date: String,
    legal_dong_snapshot_record: String,
    change_table: String,
    change_table_snapshot_id: String,
    sido: Vec<ProjectionSido>,
    sigungu: Vec<ProjectionPair>,
}

#[derive(Deserialize)]
struct ProjectionSido {
    new_code: String,
    old_codes: Vec<String>,
}

#[derive(Deserialize)]
struct ProjectionPair {
    old_code: String,
    new_code: String,
}

#[derive(Deserialize)]
struct LatestMarker {
    snapshot_date: String,
    source_record_id: String,
}

#[derive(Deserialize)]
struct CollectionState {
    table_object_key: String,
    checked_at_utc: String,
}

#[derive(Deserialize)]
struct SourceContract {
    projection: ProjectionPolicy,
}

#[derive(Deserialize)]
struct ProjectionPolicy {
    max_age_days: i64,
    baseline_comparison: BaselineComparison,
}

/// `projection.baseline_comparison` of `code-go-kr-legal-dong.contract.json` (root ADR-0145 §3).
///
/// `required: true, retired_by: null` holds the derived crosswalk to the 27 hand pairs. A live
/// pairing run that reproduces them retires the comparison: `required: false` and `retired_by`
/// naming that run. Either half without the other is refused, so the comparison cannot be switched
/// off without saying which run earned it.
#[derive(Debug, Deserialize)]
pub struct BaselineComparison {
    required: bool,
    retired_by: Option<RetirementEvidence>,
}

/// The live run whose projection reproduced the baseline pairs.
#[derive(Debug, Deserialize)]
pub struct RetirementEvidence {
    change_table_snapshot_id: String,
    legal_dong_snapshot_record: String,
    projection_sha256: String,
}

impl BaselineComparison {
    fn from_contract(contract_json: &str) -> anyhow::Result<Self> {
        let comparison = serde_json::from_str::<SourceContract>(contract_json)
            .context(
                "code-go-kr-legal-dong.contract.json has no readable projection.baseline_comparison",
            )?
            .projection
            .baseline_comparison;
        match (comparison.required, comparison.retired_by.as_ref()) {
            (true, None) => {}
            (true, Some(_)) => bail!(
                "code-go-kr-legal-dong.contract.json: projection.baseline_comparison names the run \
                 that retired it but is still required; set required to false or drop retired_by"
            ),
            (false, None) => bail!(
                "code-go-kr-legal-dong.contract.json: projection.baseline_comparison is off without \
                 retired_by; name the live pairing run that reproduced {BASELINE_FILE} \
                 ({RUNBOOK}, 'Retiring the baseline comparison')"
            ),
            (false, Some(evidence)) => ensure!(
                !evidence.change_table_snapshot_id.trim().is_empty()
                    && !evidence.legal_dong_snapshot_record.trim().is_empty()
                    && evidence.projection_sha256.len() == 64
                    && evidence
                        .projection_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit()),
                "code-go-kr-legal-dong.contract.json: projection.baseline_comparison.retired_by must \
                 name the change table snapshot, the full-table record and the projection's sha256"
            ),
        }
        Ok(comparison)
    }
}

fn five_digits(code: &str) -> bool {
    code.len() == 5 && code.bytes().all(|b| b.is_ascii_digit())
}

/// Loads the 시군구 crosswalk the hub exports compose through (root ADR-0143 §5, ADR-0145).
///
/// It is the change table's 시군구 view named by [`PROJECTION_ENV`], checked against the baseline
/// while the contract requires it, and shown current against the collector's state and the Iceberg
/// catalog.
///
/// # Errors
/// Refuses when the variable is unset, the projection, its latest-snapshot marker or the
/// collector's state is missing or unreadable, the projection is stale or no collection confirmed
/// it within the contract's age, the catalog cannot be asked or holds another change table
/// snapshot, it is not a consistent crosswalk, or it disagrees with a required baseline. Every
/// refusal names the runbook step that fixes it.
pub async fn hub_sigungu_crosswalk() -> anyhow::Result<SigunguCrosswalk> {
    let crosswalk = crosswalk_at(std::env::var_os(PROJECTION_ENV), Utc::now())?;
    let config = LakehouseCatalogConfig::from_env().with_context(|| {
        format!(
            "Refusing the export: the Iceberg catalog is not configured, so the crosswalk \
             projection cannot be shown to name the current {CHANGE_TABLE} snapshot \
             ({RUNBOOK}, 'Before a hub export')"
        )
    })?;
    let catalog =
        IcebergRestCatalog::new(config).context("failed to build the Iceberg catalog client")?;
    let current = catalog
        .load_current_snapshot_manifest_list(CHANGE_TABLE)
        .await
        .with_context(|| format!("failed to resolve the {CHANGE_TABLE} snapshot"))?
        .map(|snapshot| snapshot.snapshot_id.to_string());
    confirm_catalog_snapshot(&crosswalk, current.as_deref())?;
    Ok(crosswalk)
}

fn crosswalk_at(
    path: Option<std::ffi::OsString>,
    now: DateTime<Utc>,
) -> anyhow::Result<SigunguCrosswalk> {
    let path = path
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
        .with_context(|| {
            format!(
                "Refusing the export: {PROJECTION_ENV} is not set. The hub exports compose PNUs \
                 through the 시군구 view of {CHANGE_TABLE} the legal-dong pairing job writes (root \
                 ADR-0143 §5, ADR-0145). Run the collector and the pairing job, then point \
                 {PROJECTION_ENV} at its projection ({RUNBOOK}, 'Before a hub export')."
            )
        })?;
    let projection = std::fs::read(&path).with_context(|| {
        format!(
            "Refusing the export: cannot read the crosswalk projection {} ({RUNBOOK}, 'Before a \
             hub export')",
            path.display()
        )
    })?;
    let marker_path = path.with_file_name(LATEST_MARKER_FILE);
    let marker = std::fs::read(&marker_path).with_context(|| {
        format!(
            "Refusing the export: the latest-snapshot marker {} is missing, so the projection \
             cannot be shown to be current ({RUNBOOK}, 'Before a hub export')",
            marker_path.display()
        )
    })?;
    let state_path = path.with_file_name(COLLECTION_STATE_FILE);
    let state = std::fs::read(&state_path).with_context(|| {
        format!(
            "Refusing the export: the collector's state {} is missing, so no collection can be \
             shown to have confirmed the projection's table ({RUNBOOK}, 'Before a hub export')",
            state_path.display()
        )
    })?;
    let origin = path.display().to_string();
    let crosswalk =
        crosswalk_from_projection(&projection, &marker, CONTRACT_JSON, BASELINE_JSON, &origin)?;
    confirm_collection(&crosswalk, &state, now, CONTRACT_JSON, &origin)?;
    Ok(crosswalk)
}

fn provenance<'a>(crosswalk: &'a SigunguCrosswalk, key: &str) -> &'a str {
    crosswalk.provenance().get(key).map_or("", String::as_str)
}

/// Refuses a projection whose table is not the collector's last handoff, or whose table no
/// collection has confirmed within the contract's `projection.max_age_days`.
fn confirm_collection(
    crosswalk: &SigunguCrosswalk,
    state_bytes: &[u8],
    now: DateTime<Utc>,
    contract_json: &str,
    origin: &str,
) -> anyhow::Result<()> {
    let state: CollectionState = serde_json::from_slice(state_bytes).with_context(|| {
        format!("Refusing the export: the collector's state beside {origin} is unreadable")
    })?;
    let max_age_days = serde_json::from_str::<SourceContract>(contract_json)
        .context("code-go-kr-legal-dong.contract.json has no projection.max_age_days")?
        .projection
        .max_age_days;
    let record = provenance(crosswalk, "legal_dong_snapshot_record");
    ensure!(
        state.table_object_key == record,
        "Refusing the export: {origin} is stale. It was built from {record}, but the collector \
         has since handed off {} and the pairing has not run on it. Let lineage_stewardship load \
         it ({RUNBOOK}, 'Before a hub export').",
        state.table_object_key
    );
    let checked = DateTime::parse_from_rfc3339(&state.checked_at_utc)
        .with_context(|| {
            format!(
                "Refusing the export: the collector's state beside {origin} has no readable \
                 checked_at_utc: {:?}",
                state.checked_at_utc
            )
        })?
        .with_timezone(&Utc);
    let age = now.signed_duration_since(checked);
    ensure!(
        age <= chrono::Duration::days(max_age_days),
        "Refusing the export: no collection has confirmed the code.go.kr table of {origin} for {} \
         days (last at {}), more than the {max_age_days} days code-go-kr-legal-dong.contract.json \
         allows (projection.max_age_days). Find out why legal_dong_code_changes stopped \
         succeeding ({RUNBOOK}, 'Before a hub export').",
        age.num_days(),
        state.checked_at_utc
    );
    Ok(())
}

/// Refuses a projection that names another [`CHANGE_TABLE`] snapshot than the catalog's current
/// one (`None`: the table has none, which the projection writes as "").
fn confirm_catalog_snapshot(
    crosswalk: &SigunguCrosswalk,
    catalog_current: Option<&str>,
) -> anyhow::Result<()> {
    let named = provenance(crosswalk, "change_table_snapshot_id");
    let current = catalog_current.unwrap_or_default();
    ensure!(
        named == current,
        "Refusing the export: the crosswalk projection names {CHANGE_TABLE} snapshot {named:?}, \
         but the catalog's current snapshot is {current:?}. The table moved after the projection \
         was written; run the pairing again ({RUNBOOK}, 'Before a hub export')."
    );
    Ok(())
}

/// The baseline fixture: the 27 hand pairs (embedded), for tests that compose through them.
#[must_use]
pub const fn baseline_fixture_json() -> &'static str {
    BASELINE_JSON
}

/// [`hub_sigungu_crosswalk`] over projection and marker bytes already read, under its checks of
/// the projection itself; whether it is current (the collector's state, the catalog) is not asked.
///
/// # Errors
/// As [`hub_sigungu_crosswalk`], minus the file reads and the currency checks.
pub fn hub_sigungu_crosswalk_from(
    projection_bytes: &[u8],
    marker_bytes: &[u8],
    origin: &str,
) -> anyhow::Result<SigunguCrosswalk> {
    crosswalk_from_projection(
        projection_bytes,
        marker_bytes,
        CONTRACT_JSON,
        BASELINE_JSON,
        origin,
    )
}

fn crosswalk_from_projection(
    projection_bytes: &[u8],
    marker_bytes: &[u8],
    contract_json: &str,
    baseline_json: &str,
    origin: &str,
) -> anyhow::Result<SigunguCrosswalk> {
    use sha2::{Digest, Sha256};

    let projection: Projection = serde_json::from_slice(projection_bytes).with_context(|| {
        format!("Refusing the export: {origin} is not a sigungu crosswalk projection")
    })?;
    ensure!(
        projection.schema_version == PROJECTION_SCHEMA,
        "Refusing the export: {origin} has schema {:?}, expected {PROJECTION_SCHEMA:?}",
        projection.schema_version
    );
    ensure!(
        projection.change_table == CHANGE_TABLE,
        "Refusing the export: {origin} is a view of {:?}, not of {CHANGE_TABLE} (root ADR-0145)",
        projection.change_table
    );
    let marker: LatestMarker = serde_json::from_slice(marker_bytes).with_context(|| {
        format!("Refusing the export: the latest-snapshot marker beside {origin} is unreadable")
    })?;
    ensure!(
        marker.snapshot_date == projection.legal_dong_snapshot_date
            && marker.source_record_id == projection.legal_dong_snapshot_record,
        "Refusing the export: {origin} is stale. It was built from the code.go.kr snapshot {} ({}), \
         but the latest loaded snapshot is {} ({}). Run the pairing job on the latest snapshot \
         ({RUNBOOK}, 'Before a hub export').",
        projection.legal_dong_snapshot_date,
        projection.legal_dong_snapshot_record,
        marker.snapshot_date,
        marker.source_record_id
    );
    ensure!(
        projection.sigungu.is_empty() || !projection.change_table_snapshot_id.trim().is_empty(),
        "Refusing the export: {origin} names no {CHANGE_TABLE} snapshot for its pairs"
    );
    let (governed, pairs) = composing_direction(&projection.sido, &projection.sigungu);
    let derived = crosswalk_pairs(&governed, &pairs, origin)?;
    let comparison = BaselineComparison::from_contract(contract_json)?;
    if comparison.required {
        compare_with_baseline(&governed, &derived, &parse_baseline(baseline_json)?, origin)?;
    }
    let hub_feed = parse_hub_feed(HUB_FEED_JSON)?;
    let provenance = BTreeMap::from([
        ("projection".to_owned(), origin.to_owned()),
        (
            "projection_sha256".to_owned(),
            format!("{:x}", Sha256::digest(projection_bytes)),
        ),
        (
            "legal_dong_snapshot_date".to_owned(),
            projection.legal_dong_snapshot_date.clone(),
        ),
        (
            "legal_dong_snapshot_record".to_owned(),
            projection.legal_dong_snapshot_record.clone(),
        ),
        (
            "change_table_snapshot_id".to_owned(),
            projection.change_table_snapshot_id,
        ),
        (
            "baseline_comparison".to_owned(),
            if comparison.required {
                "required".to_owned()
            } else {
                "retired".to_owned()
            },
        ),
    ]);
    SigunguCrosswalk::new(derived, governed.into_iter().map(|sido| sido.current_code))
        .and_then(|crosswalk| crosswalk.with_placeholders(hub_feed.placeholder_sigungu.codes))
        .map(|crosswalk| crosswalk.with_provenance(provenance))
        .with_context(|| format!("Refusing the export: {origin} is not a consistent crosswalk"))
}

/// The table, the projection and the baseline run old → new; the kernel composes current →
/// superseded. This is the one place the direction turns.
fn composing_direction(
    sido: &[ProjectionSido],
    sigungu: &[ProjectionPair],
) -> (Vec<Governed>, Vec<Pair>) {
    let governed = sido
        .iter()
        .map(|sido| Governed {
            current_code: sido.new_code.clone(),
            supersedes: sido.old_codes.clone(),
        })
        .collect();
    let pairs = sigungu
        .iter()
        .map(|pair| Pair {
            current_code: pair.new_code.clone(),
            superseded_code: pair.old_code.clone(),
        })
        .collect();
    (governed, pairs)
}

/// `current → superseded` from one set of 시도 declarations and 시군구 pairs, refusing a non-5-digit
/// code, a repeated current code, and a pair no 시도 declaration's `supersedes` allows.
fn crosswalk_pairs(
    sido: &[Governed],
    sigungu: &[Pair],
    origin: &str,
) -> anyhow::Result<HashMap<String, String>> {
    let supersedes = sido
        .iter()
        .map(|sido| {
            (
                sido.current_code.as_str(),
                sido.supersedes
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut crosswalk = HashMap::with_capacity(sigungu.len());
    for entry in sigungu {
        ensure!(
            five_digits(&entry.current_code) && five_digits(&entry.superseded_code),
            "{origin}: sigungu crosswalk codes must be 5 digits: {} -> {}",
            entry.superseded_code,
            entry.current_code
        );
        ensure!(
            supersedes
                .get(&entry.current_code[..2])
                .is_some_and(|targets| targets.contains(&entry.superseded_code[..2])),
            "{origin}: sigungu crosswalk maps {} -> {}, which no sido entry's old codes allow",
            entry.superseded_code,
            entry.current_code
        );
        let previous = crosswalk.insert(entry.current_code.clone(), entry.superseded_code.clone());
        ensure!(
            previous.is_none(),
            "{origin}: sigungu crosswalk repeats the new code {}",
            entry.current_code
        );
    }
    Ok(crosswalk)
}

/// Refuses a derived crosswalk that differs from the baseline's hand pairs in a 시도 the baseline
/// governs: the same governed 시도 with the same superseded 시도, and exactly the baseline's pairs
/// under it. A 시도 the baseline does not govern is the derivation's alone (a later merger).
fn compare_with_baseline(
    derived_sido: &[Governed],
    derived: &HashMap<String, String>,
    baseline: &Baseline,
    origin: &str,
) -> anyhow::Result<()> {
    let (baseline_sido, baseline_sigungu) = composing_direction(&baseline.sido, &baseline.sigungu);
    let mut differences = Vec::new();
    for sido in &baseline_sido {
        let expected = sido.supersedes.iter().collect::<BTreeSet<_>>();
        match derived_sido
            .iter()
            .find(|candidate| candidate.current_code == sido.current_code)
        {
            Some(found) if found.supersedes.iter().collect::<BTreeSet<_>>() == expected => {}
            Some(found) => differences.push(format!(
                "sido {} supersedes {:?} in the projection but {:?} in the baseline",
                sido.current_code, found.supersedes, sido.supersedes
            )),
            None => differences.push(format!(
                "the projection does not govern sido {}",
                sido.current_code
            )),
        }
    }
    let baseline_pairs = baseline_sigungu
        .iter()
        .map(|entry| (entry.current_code.as_str(), entry.superseded_code.as_str()))
        .collect::<BTreeMap<_, _>>();
    for (current, superseded) in &baseline_pairs {
        match derived.get(*current) {
            Some(found) if found == superseded => {}
            Some(found) => differences.push(format!(
                "{found} -> {current} in the projection but {superseded} -> {current} in the baseline"
            )),
            None => differences.push(format!(
                "{superseded} -> {current} is in the baseline but not in the projection"
            )),
        }
    }
    let governed = baseline_sido
        .iter()
        .map(|sido| sido.current_code.as_str())
        .collect::<BTreeSet<_>>();
    let mut extra = derived
        .iter()
        .filter(|(current, _)| {
            governed.contains(&current[..2]) && !baseline_pairs.contains_key(current.as_str())
        })
        .map(|(current, superseded)| format!("{superseded} -> {current} is not in the baseline"))
        .collect::<Vec<_>>();
    extra.sort();
    differences.extend(extra);
    if differences.is_empty() {
        return Ok(());
    }
    bail!(
        "Refusing the export: the code.go.kr crosswalk {origin} disagrees with the hand pairs of \
         {BASELINE_FILE} in a 시도 they govern (root ADR-0143 §5, ADR-0145 §3): {}. A steward \
         decides which is wrong ({RUNBOOK}, 'When the crosswalk disagrees with the baseline').",
        differences.join("; ")
    )
}

/// The per-export 시도 tally for `crosswalk` over the hub feed's placeholders and bound and the
/// cadastral parcel set.
///
/// # Errors
/// Fails when either contract is unreadable, the cadastral set is empty or inconsistent, or the
/// crosswalk maps onto a 시도 the cadastral set does not carry.
pub fn hub_sido_tally(crosswalk: &SigunguCrosswalk) -> anyhow::Result<SidoTally> {
    SidoTally::from_contracts(crosswalk, HUB_FEED_JSON, PARCEL_SOURCE_JSON)
}

fn parse_baseline(raw: &str) -> anyhow::Result<Baseline> {
    serde_json::from_str(raw).with_context(|| format!("{BASELINE_FILE} is not valid baseline JSON"))
}

fn parse_hub_feed(raw: &str) -> anyhow::Result<HubFeed> {
    serde_json::from_str(raw).with_context(|| format!("{HUB_FEED_FILE} is not valid JSON"))
}

/// The baseline's own hand pairs as a crosswalk, with the hub feed's placeholders. Only tests read
/// it: the exports read the change table's view, and the baseline is at most its comparison.
#[cfg(test)]
pub(crate) fn baseline_crosswalk() -> anyhow::Result<SigunguCrosswalk> {
    parse_crosswalk(BASELINE_JSON, HUB_FEED_JSON)
}

#[cfg(test)]
fn parse_crosswalk(baseline: &str, hub_feed: &str) -> anyhow::Result<SigunguCrosswalk> {
    let baseline = parse_baseline(baseline)?;
    ensure!(
        !baseline.sigungu.is_empty(),
        "{BASELINE_FILE} has no sigungu entries"
    );
    let (sido, sigungu) = composing_direction(&baseline.sido, &baseline.sigungu);
    let crosswalk = crosswalk_pairs(&sido, &sigungu, BASELINE_FILE)?;
    let placeholders = parse_hub_feed(hub_feed)?.placeholder_sigungu.codes;
    SigunguCrosswalk::new(crosswalk, sido.into_iter().map(|sido| sido.current_code))
        .and_then(|crosswalk| crosswalk.with_placeholders(placeholders))
        .with_context(|| format!("{BASELINE_FILE} is not a consistent crosswalk"))
}

/// Hub rows counted by the 시도 of their raw 시군구 code, judged against the cadastral parcel set.
///
/// Feed it each row's `register_parcel_key` (whose first five characters are the raw hub 시군구
/// code, zero-padded) and call [`SidoTally::finish`] before the export counts as done.
#[derive(Debug)]
pub struct SidoTally {
    cadastral_sido: BTreeSet<String>,
    governed_sido: BTreeSet<String>,
    placeholders: BTreeSet<String>,
    absent_sido_row_bound: u64,
    crosswalk_provenance: BTreeMap<String, String>,
    rows_by_sido: BTreeMap<String, u64>,
    placeholder_rows: BTreeMap<String, u64>,
}

impl SidoTally {
    fn from_contracts(
        crosswalk: &SigunguCrosswalk,
        hub_feed: &str,
        parcel_source: &str,
    ) -> anyhow::Result<Self> {
        let hub_feed = parse_hub_feed(hub_feed)?;
        let parcel_source: ParcelSource = serde_json::from_str(parcel_source)
            .context("vworld-parcel-source-objects.json is not valid JSON")?;
        let region_prefixes = |granularity: &str| {
            parcel_source
                .objects
                .iter()
                .filter(|object| object.granularity == granularity)
                .map(|object| object.region_code.get(..2).unwrap_or_default().to_owned())
                .collect::<BTreeSet<_>>()
        };
        let cadastral_sido = region_prefixes("sido");
        ensure!(
            !cadastral_sido.is_empty() && cadastral_sido == region_prefixes("sigungu"),
            "vworld-parcel-source-objects.json must list its sido objects and cover the same \
             sido with its sigungu objects"
        );
        for superseded in crosswalk.superseded_codes() {
            let sido = superseded.get(..2).unwrap_or(superseded);
            ensure!(
                cadastral_sido.contains(sido),
                "the sigungu crosswalk maps a code onto {superseded}, whose sido {sido} the \
                 cadastral parcel set no longer carries"
            );
        }
        Ok(Self {
            cadastral_sido,
            governed_sido: crosswalk.governed_sido().map(str::to_owned).collect(),
            placeholders: hub_feed
                .placeholder_sigungu
                .codes
                .iter()
                .map(|code| format!("{:0>5}", code.trim()))
                .collect(),
            absent_sido_row_bound: hub_feed.absent_sido_row_bound.rows,
            crosswalk_provenance: crosswalk.provenance().clone(),
            rows_by_sido: BTreeMap::new(),
            placeholder_rows: BTreeMap::new(),
        })
    }

    /// Counts one row by the raw 시군구 code at the head of its register parcel key.
    pub fn observe(&mut self, register_parcel_key: &str) {
        let code = register_parcel_key.get(..5).unwrap_or(register_parcel_key);
        if self.placeholders.contains(code) {
            *self.placeholder_rows.entry(code.to_owned()).or_insert(0) += 1;
            return;
        }
        let sido = code.get(..2).unwrap_or(code).to_owned();
        *self.rows_by_sido.entry(sido).or_insert(0) += 1;
    }

    /// Refuses when one 시도 the cadastral parcel set does not carry, and the crosswalk does not
    /// govern, holds more rows than the bound; otherwise returns the counts for the summary.
    ///
    /// # Errors
    /// Fails naming the 시도, its row count, and the bound.
    pub fn finish(&self) -> anyhow::Result<Value> {
        let absent = self
            .rows_by_sido
            .iter()
            .filter(|(sido, _)| {
                !self.cadastral_sido.contains(*sido) && !self.governed_sido.contains(*sido)
            })
            .map(|(sido, rows)| (sido.clone(), *rows))
            .collect::<BTreeMap<_, _>>();
        if let Some((sido, rows)) = absent
            .iter()
            .find(|(_, rows)| **rows > self.absent_sido_row_bound)
        {
            bail!(
                "Refusing the export: {rows} hub rows carry 시도 {sido}, which the cadastral parcel \
                 set does not carry and no merged 시도 governs (bound {}). Their PNUs would be \
                 orphans. If a merger created it, the code.go.kr pairing (root ADR-0143) must \
                 derive its pairs; if the hub uses it for no 시군구, declare it a placeholder in \
                 {HUB_FEED_FILE}.",
                self.absent_sido_row_bound
            );
        }
        Ok(json!({
            "rows_by_sido": self.rows_by_sido,
            "placeholder_rows": self.placeholder_rows,
            "absent_from_cadastre_rows": absent,
            "absent_sido_row_bound": self.absent_sido_row_bound,
            "crosswalk": self.crosswalk_provenance,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        confirm_catalog_snapshot, confirm_collection, crosswalk_at, crosswalk_from_projection,
        parse_crosswalk, BaselineComparison, SidoTally, BASELINE_JSON, COLLECTION_STATE_FILE,
        CONTRACT_JSON, HUB_FEED_JSON, LATEST_MARKER_FILE, PARCEL_SOURCE_JSON, PROJECTION_ENV,
    };
    use foundation_shared_kernel::pnu::{
        standard_pnu_from_hub_register_codes_via, SigunguCrosswalkError,
    };
    use serde_json::{json, Value};

    /// A synthetic cadastral parcel set over 시도 97 and 98.
    const PARCEL_SOURCE: &str = r#"{"objects":[
        {"region_code":"97","granularity":"sido"},{"region_code":"98","granularity":"sido"},
        {"region_code":"97110","granularity":"sigungu"},{"region_code":"98110","granularity":"sigungu"}]}"#;

    /// A register parcel key headed by the raw hub 시군구 `code`, zero-padded like the real one.
    fn key(code: &str) -> String {
        format!("{code:0>5}{}", "00101000010000")
    }

    /// Merged 시도 99 superseding 98.
    const BASELINE: &str = r#"{"sido":[{"new_code":"99","old_codes":["98"]}],
        "sigungu":[{"old_code":"98110","new_code":"99110"}]}"#;
    /// Placeholders 99999 and the malformed `0`; bound 2 rows.
    const HUB_FEED: &str = r#"{"placeholder_sigungu":{"codes":["99999","0"]},
        "absent_sido_row_bound":{"rows":2}}"#;

    const SNAPSHOT_DATE: &str = "2099-01-01";
    const SNAPSHOT_RECORD: &str =
        "bronze/source=codegokr__legal_dong_code_table/regcode_20990101t000000z.html";

    /// The projection a pairing run that reproduced the real baseline would write: the change
    /// table's view, old → new like the fixture, whose 시도 and pairs it carries as they are.
    fn projection_from_baseline() -> anyhow::Result<Value> {
        let baseline: Value = serde_json::from_str(BASELINE_JSON)?;
        let (sido, sigungu) = (baseline["sido"].clone(), baseline["sigungu"].clone());
        Ok(json!({
            "schema_version": "foundation-platform.sigungu_crosswalk_projection.v2",
            "legal_dong_snapshot_date": SNAPSHOT_DATE,
            "legal_dong_snapshot_record": SNAPSHOT_RECORD,
            "change_table": "reference.legal_dong_code_change",
            "change_table_snapshot_id": "1",
            "sido": sido,
            "sigungu": sigungu,
        }))
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2099-01-05T00:00:00Z")
            .map(|value| value.with_timezone(&chrono::Utc))
            .unwrap_or_default()
    }

    /// The collector's state after a check at `checked_at` that handed off `record`.
    fn state(record: &str, checked_at: &str) -> Vec<u8> {
        json!({"table_object_key": record, "row_count": 1, "table_digest": "d",
               "handoff": "h", "checked_at_utc": checked_at})
        .to_string()
        .into_bytes()
    }

    fn marker(date: &str) -> Vec<u8> {
        json!({"snapshot_date": date, "source_record_id": SNAPSHOT_RECORD})
            .to_string()
            .into_bytes()
    }

    fn load(projection: &Value, marker: &[u8]) -> anyhow::Result<super::SigunguCrosswalk> {
        crosswalk_from_projection(
            projection.to_string().as_bytes(),
            marker,
            CONTRACT_JSON,
            BASELINE_JSON,
            "projection.json",
        )
    }

    /// The source contract with `projection.baseline_comparison` replaced.
    fn contract_with(comparison: &Value) -> anyhow::Result<String> {
        let mut contract: Value = serde_json::from_str(CONTRACT_JSON)?;
        contract["projection"]["baseline_comparison"] = comparison.clone();
        Ok(contract.to_string())
    }

    #[test]
    fn a_projection_that_reproduces_the_baseline_pairs_is_the_crosswalk() -> anyhow::Result<()> {
        let projection = projection_from_baseline()?;
        let crosswalk = load(&projection, &marker(SNAPSHOT_DATE))?;
        let baseline: Value = serde_json::from_str(BASELINE_JSON)?;
        let pairs = baseline["sigungu"]
            .as_array()
            .map_or(&[][..], Vec::as_slice);
        assert_eq!(pairs.len(), 27);
        assert_eq!(crosswalk.len(), pairs.len());
        for pair in pairs {
            let current = pair["new_code"].as_str().unwrap_or_default();
            assert_eq!(
                crosswalk.superseded_code(current),
                pair["old_code"].as_str(),
                "{current}"
            );
        }
        assert!(crosswalk.governs_sido("12"));
        assert_eq!(
            crosswalk
                .provenance()
                .get("legal_dong_snapshot_date")
                .map(String::as_str),
            Some(SNAPSHOT_DATE)
        );
        assert_eq!(
            crosswalk
                .provenance()
                .get("projection_sha256")
                .map(String::len),
            Some(64)
        );
        // ADR-0142 의 거부는 그대로다: 다스리는 시도의 짝 없는 코드, 자리표시자.
        assert_eq!(
            crosswalk.resolve("12999"),
            Err(SigunguCrosswalkError::UnmappedGovernedSigungu(
                "12999".to_owned()
            ))
        );
        for code in ["99999", "99990", "0"] {
            assert_eq!(
                standard_pnu_from_hub_register_codes_via(
                    &crosswalk, code, "00101", "0", "0001", "0000"
                )?,
                None,
                "a placeholder must compose no PNU: {code}"
            );
        }
        // 실물 계약 둘과 함께 시도 집계도 일관되고, 요약에 크로스워크 출처가 실린다.
        let mut tally = SidoTally::from_contracts(&crosswalk, HUB_FEED_JSON, PARCEL_SOURCE_JSON)?;
        tally.observe(&key("12210"));
        let summary = tally.finish()?;
        assert_eq!(summary["crosswalk"]["change_table_snapshot_id"], "1");
        assert_eq!(summary["crosswalk"]["baseline_comparison"], "required");
        Ok(())
    }

    #[test]
    fn a_view_of_another_table_or_an_old_schema_is_refused() -> anyhow::Result<()> {
        // 심은 위반: 폐기된 저장 대응표를 가리키는 투영, 옛 스키마(새 → 옛 방향)의 투영.
        let mut other = projection_from_baseline()?;
        other["change_table"] = json!("reference.some_crosswalk");
        let refused = load(&other, &marker(SNAPSHOT_DATE));
        assert!(
            refused
                .as_ref()
                .is_err_and(|error| format!("{error:#}").contains("ADR-0145")),
            "got {refused:?}"
        );
        let mut old = projection_from_baseline()?;
        old["schema_version"] = json!("foundation-platform.sigungu_crosswalk_projection.v1");
        assert!(load(&old, &marker(SNAPSHOT_DATE)).is_err());
        Ok(())
    }

    #[test]
    fn no_projection_path_is_refused() {
        let refused = crosswalk_at(None, now());
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = format!("{error:#}");
                message.contains(PROJECTION_ENV) && message.contains("Before a hub export")
            }),
            "got {refused:?}"
        );
        assert!(crosswalk_at(Some(std::ffi::OsString::new()), now()).is_err());
    }

    #[test]
    fn a_projection_without_its_latest_marker_is_refused() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("crosswalk-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("sigungu-crosswalk.projection.json");
        std::fs::write(&path, projection_from_baseline()?.to_string())?;
        let refused = crosswalk_at(Some(path.clone().into_os_string()), now());
        assert!(
            refused
                .as_ref()
                .is_err_and(|error| format!("{error:#}").contains(LATEST_MARKER_FILE)),
            "got {refused:?}"
        );
        std::fs::write(dir.join(LATEST_MARKER_FILE), marker(SNAPSHOT_DATE))?;
        let refused = crosswalk_at(Some(path.clone().into_os_string()), now());
        assert!(
            refused
                .as_ref()
                .is_err_and(|error| format!("{error:#}").contains(COLLECTION_STATE_FILE)),
            "got {refused:?}"
        );
        std::fs::write(
            dir.join(COLLECTION_STATE_FILE),
            state(SNAPSHOT_RECORD, "2099-01-04T05:15:00+09:00"),
        )?;
        assert!(crosswalk_at(Some(path.into_os_string()), now()).is_ok());
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_stale_projection_is_refused() -> anyhow::Result<()> {
        // 심은 위반: 더 새 전체 표가 적재됐는데 짝 맞추기는 옛 표로 만든 투영이다.
        let refused = load(&projection_from_baseline()?, &marker("2099-01-02"));
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = format!("{error:#}");
                message.contains("stale") && message.contains("2099-01-02")
            }),
            "got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_projection_no_collection_confirmed_lately_is_refused() -> anyhow::Result<()> {
        let crosswalk = load(&projection_from_baseline()?, &marker(SNAPSHOT_DATE))?;
        let max: i64 = serde_json::from_str::<Value>(CONTRACT_JSON)?["projection"]["max_age_days"]
            .as_i64()
            .unwrap_or_default();
        assert!(max > 0, "the contract must bound the age");
        let at = |days: i64| (now() - chrono::Duration::days(days)).to_rfc3339();
        confirm_collection(
            &crosswalk,
            &state(SNAPSHOT_RECORD, &at(max)),
            now(),
            CONTRACT_JSON,
            "p",
        )?;
        // 심은 위반: 마지막으로 확인된 수집이 계약의 한계보다 하루 오래됐다.
        let refused = confirm_collection(
            &crosswalk,
            &state(SNAPSHOT_RECORD, &at(max + 1)),
            now(),
            CONTRACT_JSON,
            "p",
        );
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = format!("{error:#}");
                message.contains("max_age_days") && message.contains(&format!("{} days", max + 1))
            }),
            "got {refused:?}"
        );
        // 심은 위반: 수집이 새 표를 넘겼는데 짝 맞추기가 아직 그 표로 돌지 않았다.
        let refused = confirm_collection(
            &crosswalk,
            &state(
                "bronze/source=codegokr__legal_dong_code_table/regcode-newer.html",
                &at(0),
            ),
            now(),
            CONTRACT_JSON,
            "p",
        );
        assert!(
            refused
                .as_ref()
                .is_err_and(|error| format!("{error:#}").contains("stale")),
            "got {refused:?}"
        );
        assert!(confirm_collection(&crosswalk, b"{}", now(), CONTRACT_JSON, "p").is_err());
        Ok(())
    }

    #[test]
    fn a_projection_the_change_table_moved_past_is_refused() -> anyhow::Result<()> {
        // The projection names snapshot "1" (projection_from_baseline).
        let crosswalk = load(&projection_from_baseline()?, &marker(SNAPSHOT_DATE))?;
        confirm_catalog_snapshot(&crosswalk, Some("1"))?;
        for current in [Some("2"), None] {
            let refused = confirm_catalog_snapshot(&crosswalk, current);
            assert!(
                refused.as_ref().is_err_and(|error| {
                    let message = format!("{error:#}");
                    message.contains("reference.legal_dong_code_change")
                        && message.contains("current snapshot")
                }),
                "{current:?}: got {refused:?}"
            );
        }
        // A table nothing was written to: the projection says "" and the catalog has none.
        let mut empty = projection_from_baseline()?;
        empty["change_table_snapshot_id"] = json!("");
        empty["sigungu"] = json!([]);
        empty["sido"] = json!([]);
        let crosswalk = crosswalk_from_projection(
            empty.to_string().as_bytes(),
            &marker(SNAPSHOT_DATE),
            CONTRACT_JSON,
            r#"{"sido":[],"sigungu":[]}"#,
            "p",
        )?;
        confirm_catalog_snapshot(&crosswalk, None)?;
        assert!(confirm_catalog_snapshot(&crosswalk, Some("3")).is_err());
        Ok(())
    }

    /// The four ways a projection can disagree with the baseline, each planted on a copy of the
    /// projection that reproduces it.
    fn disagreeing_projections() -> anyhow::Result<Vec<(&'static str, Value)>> {
        let baseline: Value = serde_json::from_str(BASELINE_JSON)?;
        let new_code = baseline["sigungu"][0]["new_code"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let other_old = baseline["sigungu"][1]["old_code"].clone();
        let old_sido = baseline["sido"][0]["old_codes"][0]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let mut changed = projection_from_baseline()?;
        changed["sigungu"][0]["old_code"] = other_old;
        let mut missing = projection_from_baseline()?;
        if let Some(pairs) = missing["sigungu"].as_array_mut() {
            pairs.remove(0);
        }
        let mut extra = projection_from_baseline()?;
        if let Some(pairs) = extra["sigungu"].as_array_mut() {
            pairs.push(json!({"old_code": format!("{old_sido}999"),
                              "new_code": format!("{}999", &new_code[..2])}));
        }
        let mut ungoverned = projection_from_baseline()?;
        ungoverned["sido"] = json!([]);
        ungoverned["sigungu"] = json!([]);
        Ok(vec![
            ("changed", changed),
            ("missing", missing),
            ("extra", extra),
            ("ungoverned", ungoverned),
        ])
    }

    #[test]
    fn a_projection_that_disagrees_with_a_required_baseline_is_refused() -> anyhow::Result<()> {
        for (label, projection) in disagreeing_projections()? {
            let refused = load(&projection, &marker(SNAPSHOT_DATE));
            assert!(
                refused.as_ref().is_err_and(|error| {
                    let message = format!("{error:#}");
                    message.contains("sigungu-crosswalk-baseline.json")
                        && (message.contains("disagrees") || message.contains("old codes"))
                }),
                "{label}: got {refused:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_retired_baseline_needs_the_run_that_retired_it() -> anyhow::Result<()> {
        let evidence = json!({"change_table_snapshot_id": "1",
                              "legal_dong_snapshot_record": SNAPSHOT_RECORD,
                              "projection_sha256": "a".repeat(64)});
        // 심은 위반 셋: 증거 없이 끔, 증거를 둔 채 켬, 해시가 아닌 증거.
        for comparison in [
            json!({"required": false, "retired_by": null}),
            json!({"required": true, "retired_by": evidence}),
            json!({"required": false, "retired_by": {"change_table_snapshot_id": "1",
                   "legal_dong_snapshot_record": SNAPSHOT_RECORD, "projection_sha256": "abc"}}),
            json!({"required": false}),
        ] {
            let contract = contract_with(&comparison)?;
            assert!(
                BaselineComparison::from_contract(&contract).is_err(),
                "must refuse: {comparison}"
            );
        }
        assert!(BaselineComparison::from_contract(CONTRACT_JSON)?.required);
        // 증거를 갖춰 끄면 비교는 돌지 않는다: 기준과 다른 짝도 변경표의 몫이다.
        let retired = contract_with(&json!({"required": false, "retired_by": evidence}))?;
        for (label, projection) in disagreeing_projections()? {
            if label == "ungoverned" {
                continue;
            }
            let crosswalk = crosswalk_from_projection(
                projection.to_string().as_bytes(),
                &marker(SNAPSHOT_DATE),
                &retired,
                BASELINE_JSON,
                "p",
            );
            assert!(
                crosswalk.as_ref().is_ok_and(|crosswalk| crosswalk
                    .provenance()
                    .get("baseline_comparison")
                    .is_some_and(|value| value == "retired")),
                "{label}: got {crosswalk:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_merger_the_baseline_does_not_know_is_the_derivations_to_govern() -> anyhow::Result<()> {
        let mut projection = projection_from_baseline()?;
        if let Some(sido) = projection["sido"].as_array_mut() {
            sido.push(json!({"new_code": "98", "old_codes": ["97"]}));
        }
        if let Some(pairs) = projection["sigungu"].as_array_mut() {
            pairs.push(json!({"old_code": "97110", "new_code": "98110"}));
        }
        let crosswalk = load(&projection, &marker(SNAPSHOT_DATE))?;
        assert!(crosswalk.governs_sido("98"));
        assert_eq!(crosswalk.superseded_code("98110"), Some("97110"));
        Ok(())
    }

    #[test]
    fn the_baseline_fixture_is_itself_a_consistent_crosswalk() -> anyhow::Result<()> {
        let crosswalk = parse_crosswalk(BASELINE_JSON, HUB_FEED_JSON)?;
        assert!(!crosswalk.is_empty());
        assert!(crosswalk.governs_sido("12"));
        SidoTally::from_contracts(&crosswalk, HUB_FEED_JSON, PARCEL_SOURCE_JSON)?.finish()?;
        Ok(())
    }

    #[test]
    fn a_governed_code_missing_from_the_crosswalk_stops_composition() -> anyhow::Result<()> {
        // 심은 위반: 시도 99 를 통합으로 선언하고 짝은 99110 하나만 둔다.
        let crosswalk = parse_crosswalk(BASELINE, HUB_FEED)?;
        let refused = standard_pnu_from_hub_register_codes_via(
            &crosswalk, "99990", "00101", "0", "0001", "0000",
        );
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = error.to_string();
                message.contains("99990") && message.contains("reference.legal_dong_code_change")
            }),
            "an unmapped merged code must be refused by name, got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_crosswalk_that_maps_outside_its_declared_sido_is_refused() {
        for raw in [
            // 99 를 다스리는 선언이 없는데 99110 을 매핑
            r#"{"sido":[{"new_code":"97","old_codes":["98"]}],
                "sigungu":[{"old_code":"98110","new_code":"99110"}]}"#,
            // 대상 96110 은 99 가 대체한 시도(98)가 아니다
            r#"{"sido":[{"new_code":"99","old_codes":["98"]}],
                "sigungu":[{"old_code":"96110","new_code":"99110"}]}"#,
        ] {
            assert!(
                parse_crosswalk(raw, HUB_FEED).is_err(),
                "must refuse: {raw}"
            );
        }
        // 짝이 있는 코드를 자리표시자로도 선언
        let hub_feed = HUB_FEED.replace(r#"["99999","0"]"#, r#"["99110"]"#);
        assert!(
            parse_crosswalk(BASELINE, &hub_feed).is_err(),
            "must refuse: {hub_feed}"
        );
    }

    #[test]
    fn a_sido_absent_from_the_cadastre_over_the_bound_is_refused() -> anyhow::Result<()> {
        // 심은 위반: 지적도에도 통합 선언에도 없는 시도 96 이 한계(2행)를 넘는다.
        let mut tally = SidoTally::from_contracts(
            &parse_crosswalk(BASELINE, HUB_FEED)?,
            HUB_FEED,
            PARCEL_SOURCE,
        )?;
        for _ in 0..3 {
            tally.observe(&key("96110"));
        }
        let refused = tally.finish();
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = error.to_string();
                message.contains("시도 96")
                    && message.contains("3 hub rows")
                    && message.contains("hub-register-feed.contract.json")
            }),
            "got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_small_unknown_sido_passes_and_is_reported() -> anyhow::Result<()> {
        let mut tally = SidoTally::from_contracts(
            &parse_crosswalk(BASELINE, HUB_FEED)?,
            HUB_FEED,
            PARCEL_SOURCE,
        )?;
        for code in [
            "96110", // 지적도에 없는 시도 96, 한계 이하
            "96120", "97110", // 지적도의 시도
            "99110", // 통합 시도: 지적도에 없어도 크로스워크가 다스린다
            "99999", // 자리표시자
            "0",     // 자리표시자 `0` (키에서는 0 채움)
        ] {
            tally.observe(&key(code));
        }
        let summary = tally.finish()?;
        assert_eq!(summary["absent_from_cadastre_rows"]["96"], 2);
        assert!(summary["absent_from_cadastre_rows"].get("99").is_none());
        assert_eq!(summary["rows_by_sido"]["97"], 1);
        assert_eq!(summary["placeholder_rows"]["99999"], 1);
        assert_eq!(summary["placeholder_rows"]["00000"], 1);
        Ok(())
    }

    #[test]
    fn a_crosswalk_onto_a_sido_the_cadastre_dropped_is_refused() -> anyhow::Result<()> {
        // 지적도가 시도 98 을 더 이상 싣지 않으면, 99 → 98 짝은 거짓이 된다.
        let parcel_source = r#"{"objects":[{"region_code":"97","granularity":"sido"},
            {"region_code":"97110","granularity":"sigungu"}]}"#;
        assert!(SidoTally::from_contracts(
            &parse_crosswalk(BASELINE, HUB_FEED)?,
            HUB_FEED,
            parcel_source
        )
        .is_err());
        assert!(HUB_FEED_JSON.contains("absent_sido_row_bound"));
        assert!(!BASELINE_JSON.contains("placeholder_sigungu"));
        Ok(())
    }
}
