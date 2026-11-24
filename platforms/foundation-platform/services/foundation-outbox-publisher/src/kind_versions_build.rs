//! The publisher's one build of a parcel kind's Gold as a table of appended row versions (root
//! ADR-0564 §3, §6): `build-parcel-register-masters-versions`, `build-parcel-demolitions-versions`,
//! `build-parcel-energy-versions`, `build-parcel-sub-parcels-versions`,
//! `build-parcel-building-permits-versions`, `build-parcel-panel-versions`,
//! `build-parcel-ownership-versions`, `build-parcel-factories-versions`,
//! `build-parcel-public-auction-versions`. `DuckDB` inside the publisher computes the kind
//! (`foundation_publisher_silver::kind_versions_stage`, one stage per kind) and the publisher appends
//! the keys that changed (`gold_versions_commit`). A new lane is a stage, a [`KindBuild`], a kind line
//! and a job.
//!
//! While a lane serves the Spark table its versions table replaces, the versions table is its shadow:
//! its inputs are pinned to that table's build-tag commit (`planned_inputs`), so the takeover verdict
//! compares two engines over one set of inputs, and its first build holds every key. One run: create
//! the table if it is not there, plan, mend the tags, stop when the inputs, the served edition and the
//! code are what the base commit was built from; otherwise rebuild the PNUs the stage names
//! (every PNU for a first build, a new code version or edition, or a moved whole-table input), diff
//! them against the head's current rows, refuse a build that loses or changes more than its bounds,
//! and append.

use std::{collections::BTreeSet, path::Path, time::Instant};

use anyhow::{bail, ensure, Context};
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    building_energy_stage::ENERGY,
    building_permit_stage::BUILDING_PERMITS,
    building_sub_parcel_stage::SUB_PARCELS,
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    factory_kind_stage::FACTORIES,
    kind_versions_stage::{
        build_in_parts, ensure_no_commit_tables, load_region, spool, spool_base,
        targets_from_spools, KindVersionsStage, PARCEL_SOURCE,
    },
    land_ownership_stage::OWNERSHIP,
    onbid_kind_stage::PUBLIC_AUCTION,
    parcel_nearby_stage::{NearbyPins, Source},
    parcel_panel_stage::PANEL,
    process_memory,
    record_kind_stage::{DEMOLITIONS, REGISTER_MASTERS},
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_versions_commit::{
        self as versions, Commit, Diff, Digests, AS_OF_PROPERTY, CODE_VERSION_PROPERTY,
        CURRENT_RECORDS_PROPERTY, FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY,
        RECORDED_EXPLANATION_PROPERTY, SERVED_EDITION_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY,
        VERSION_SEQ_PROPERTY,
    },
    parcel_nearby_versions_build::{
        contract, lakehouse_read, max_row_loss_fraction_of, served_edition, write_summary,
        MAX_CHANGED_KEY_FRACTION, MEMORY_SHARE,
    },
    public_data_control_support::{optional_env_value, required_env_value},
};

pub(crate) const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
/// The 시도 a run is asked for, comma-separated (root ADR-0615 §2: the partition keys the
/// orchestrator hands a run). Absent, the run brings every 시도 that is not at the wanted version
/// up to it.
pub(crate) const PARTITIONS_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_PARTITIONS";

/// The 시도 [`PARTITIONS_ENV`] names.
///
/// # Errors
/// Refuses a key that is not two digits, and an empty list: a run asked for nothing was asked wrongly.
pub(crate) fn requested_partitions(text: Option<&str>) -> anyhow::Result<Option<BTreeSet<String>>> {
    let Some(text) = text else {
        return Ok(None);
    };
    let keys: BTreeSet<String> = text
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .collect();
    ensure!(!keys.is_empty(), "{PARTITIONS_ENV} names no 시도");
    for key in &keys {
        ensure!(
            key.len() == 2 && key.bytes().all(|byte| byte.is_ascii_digit()),
            "{PARTITIONS_ENV} names {key:?}, not a 시도"
        );
    }
    Ok(Some(keys))
}

/// One lane's build: the versions table, its by-PNU lane (whose served Gold says whether this run
/// is a shadow, root ADR-0564 §6), the table whose columns it carries and whose build a shadow
/// follows, and the stage that computes it.
pub(crate) struct KindBuild {
    pub table: &'static str,
    pub lane: &'static str,
    pub replaced: &'static str,
    pub job: &'static str,
    pub stage: &'static (dyn KindVersionsStage + Sync),
    /// Whether each 시도 is its own commit (root ADR-0615 §2-4): the head then records every
    /// 시도's state, a run that died keeps the 시도 it had committed and the next run builds the
    /// rest. A lane is switched on its own; `false` is the one commit of the whole table.
    // one-off: until every kind lane commits a 시도 at a time (ADR-0615 §6): the whole-table
    // branches of `run` go with it.
    pub per_sido_commits: bool,
}

/// Lane 1: the 총괄표제부 (root ADR-0450).
pub(crate) const REGISTER_MASTERS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_register_masters_versions",
    lane: "parcel-register-masters",
    replaced: "gold.parcel_register_masters",
    job: "build-parcel-register-masters-versions",
    stage: &REGISTER_MASTERS,
    per_sido_commits: false,
};

/// Lane 2: the 철거·멸실 reports (root ADR-0399).
pub(crate) const DEMOLITIONS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_demolitions_versions",
    lane: "parcel-demolitions",
    replaced: "gold.parcel_demolitions",
    job: "build-parcel-demolitions-versions",
    stage: &DEMOLITIONS,
    per_sido_commits: false,
};

/// Lane 3: the building energy (root ADR-0398).
pub(crate) const ENERGY_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_energy_versions",
    lane: "parcel-energy",
    replaced: "gold.parcel_energy",
    job: "build-parcel-energy-versions",
    stage: &ENERGY,
    per_sido_commits: false,
};

/// Lane 4: the 부속지번, read from both of a link's parcels (root ADR-0399).
pub(crate) const SUB_PARCELS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_sub_parcels_versions",
    lane: "parcel-sub-parcels",
    replaced: "gold.parcel_sub_parcels",
    job: "build-parcel-sub-parcels-versions",
    stage: &SUB_PARCELS,
    per_sido_commits: false,
};

/// Lane 5: the 건축인허가 and the other files of the permit family (root ADR-0451, ADR-0464).
pub(crate) const BUILDING_PERMITS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_building_permits_versions",
    lane: "parcel-building-permits",
    replaced: "gold.parcel_building_permits",
    job: "build-parcel-building-permits-versions",
    stage: &BUILDING_PERMITS,
    per_sido_commits: false,
};

/// The parcel lane's host panel (root ADR-0096, ADR-0564 §9 차례 5): its lane is the host lane, whose
/// Gold is the panel's constant (`by_pnu_serving_manifest_publish::gold_table`), so while that lane
/// bakes `gold.parcel_panel` this table is its shadow.
pub(crate) const PARCEL_PANEL_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_panel_versions",
    lane: "parcel",
    replaced: "gold.parcel_panel",
    job: "build-parcel-panel-versions",
    stage: &PANEL,
    per_sido_commits: false,
};

/// Lane 6: the owners (root ADR-0361).
pub(crate) const OWNERSHIP_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_ownership_versions",
    lane: "parcel-ownership",
    replaced: "gold.parcel_ownership",
    job: "build-parcel-ownership-versions",
    stage: &OWNERSHIP,
    per_sido_commits: true,
};

/// Lane 7: the registered factories (root ADR-0449).
pub(crate) const FACTORIES_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_factories_versions",
    lane: "parcel-factories",
    replaced: "gold.parcel_factories",
    job: "build-parcel-factories-versions",
    stage: &FACTORIES,
    per_sido_commits: false,
};

/// Lane 8: the 공매 rounds (root ADR-0287, ADR-0340), built as of an instant.
pub(crate) const PUBLIC_AUCTION_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_public_auction_versions",
    lane: "parcel-public-auction",
    replaced: "gold.parcel_public_auction",
    job: "build-parcel-public-auction-versions",
    stage: &PUBLIC_AUCTION,
    per_sido_commits: false,
};

/// Builds and appends `kind`'s changed keys.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run(kind: &KindBuild) -> anyhow::Result<()> {
    let (table, stage) = (kind.table, kind.stage);
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(table)?;
    let content_columns: Vec<&'static str> = contract(kind.replaced)?
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    // The kind line's `place_identity` gave the table its place id; the stage must carry it.
    ensure!(
        content_columns.contains(&"place_id") == stage.carries_place_id(),
        "{} has {}a place_id column but its stage {} it",
        kind.replaced,
        if stage.carries_place_id() { "no " } else { "" },
        if stage.carries_place_id() {
            "carries"
        } else {
            "does not carry"
        }
    );
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let created = committer.create_table_if_absent(versions_contract).await?;

    // Plan.
    let history = catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist after its creation"))?;
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let lane = ByPnuLane::named(kind.lane)?
        .with_context(|| format!("the contract names no {} lane", kind.lane))?;
    let versions::Planned {
        pins,
        served_edition: edition,
        shadow,
    } = versions::planned_inputs(
        &catalog,
        table,
        by_pnu_serving_manifest_publish::gold_table(lane),
        &stage.inputs(),
        served_edition()?,
    )
    .await?;
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = stage.code_version();
    if let Some(base) = &base {
        versions::pin(&catalog, table, base.snapshot_id, &base.pins).await?;
    }
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // A time-dependent kind is built as of an instant: a shadow the one its replaced build was, so
    // the two tables answer one question; otherwise now.
    let as_of = if stage.as_of_dependent() {
        Some(match &shadow {
            Some(shadow) => replaced_as_of(work, &shadow.replaced, shadow.build)?,
            None => published_at_utc.clone(),
        })
    } else {
        None
    };
    let stamp = as_of.clone().unwrap_or_else(|| published_at_utc.clone());
    let moved = base
        .as_ref()
        .map_or_else(BTreeSet::new, |base| versions::moved(base, &pins));
    // A table that commits a 시도 at a time (root ADR-0615): the head says what each 시도 was built
    // from, and a 시도 already at the version this run would build is not built again.
    let requested = requested_partitions(optional_env_value(PARTITIONS_ENV)?.as_deref())?;
    ensure!(
        kind.per_sido_commits || requested.is_none(),
        "{table} is committed whole: a run of it cannot be asked for some 시도 ({PARTITIONS_ENV})"
    );
    let mut states = if kind.per_sido_commits {
        versions::PartitionStates::of(&history)?
    } else {
        versions::PartitionStates::default()
    };
    let built_from = versions::BuiltFrom {
        pins: pins.clone(),
        code_version: code.clone(),
        served_edition: edition.clone(),
    };
    let wanted = versions::data_version(&built_from, as_of.as_deref());
    let unchanged = if states.sidos.is_empty() {
        base.as_ref().is_some_and(|base| {
            moved.is_empty()
                && base.code_version == code
                && base.served_edition.as_deref() == Some(edition.as_str())
                && base.as_of == as_of
        })
    } else {
        states.sidos.values().all(|state| state.version == wanted)
    };
    if unchanged {
        write_summary(
            &summary_path,
            &json!({
                "job": kind.job, "engine": "duckdb", "table": table, "outcome": "unchanged",
                "pins": pins, "served_edition": edition, "shadow_of": shadow_build,
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {table} nothing to do: its inputs, edition and code are the base commit's");
        return Ok(());
    }
    let whole = stage.whole_table_inputs();
    let every_pnu = base.as_ref().is_none_or(|base| {
        base.code_version != code
            || base.served_edition.as_deref() != Some(edition.as_str())
            || moved.iter().any(|input| whole.contains(&input.as_str()))
            || !base.pins.keys().all(|input| pins.contains_key(input))
            // Time moves a time-dependent kind's documents with no input moving: its every run is
            // whole, from one read of its inputs (root ADR-0326: finding the change would read them
            // twice), and appends only the keys whose digest changed.
            || as_of.is_some()
    });
    // A moved code version or served edition explains a change of any size (root ADR-0346).
    let moved_code_or_edition = base.as_ref().and_then(|base| {
        if base.code_version != code {
            Some(format!("the code version moved to {code}"))
        } else if base.served_edition.as_deref() != Some(edition.as_str()) {
            Some(format!(
                "the served parcel edition moved {} -> {edition}",
                base.served_edition.as_deref().unwrap_or("-")
            ))
        } else {
            None
        }
    });

    // Compute, one 시도 at a time (rules.txt "DuckDB ports are regional, one pass"): every input is
    // read once into a local spool split by 시도, then each 시도 is loaded, built, diffed and staged
    // before the next replaces it; one append commits them all.
    // Asked before the DuckDB session opens: the session is not `Send` across an await.
    let shape = committer.table_shape(table).await?;
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = duck.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    duck.require_room(0)?;
    let connection = duck.connection();
    // The instant a time-dependent stage serves as of, in microseconds since the epoch: compared
    // with the rows' instants by `epoch_us`, whatever the session's time zone.
    if let Some(instant) = &as_of {
        let micros = chrono::DateTime::parse_from_rfc3339(instant)
            .with_context(|| format!("{instant:?} is not an RFC 3339 instant"))?
            .timestamp_micros();
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE as_of AS SELECT CAST({micros} AS BIGINT) AS instant_us;"
        ))?;
    }
    let spool_dir = Spool::new(work.join("spool"))?;
    let new_pins = NearbyPins(pins.clone());
    let read = spool(
        stage,
        connection,
        &Source::Pinned {
            catalog: LAKEHOUSE_CATALOG,
            pins: &new_pins,
        },
        &edition,
        "",
        spool_dir.path(),
    )?;
    let mut sidos = read.sidos.clone();
    // What finding the changed keys took: once, nationally (root ADR-0615 §7).
    let mut targets_millis = 0_u64;
    if !every_pnu {
        let base = base.as_ref().context("a partial run has a base")?;
        let old_pins = NearbyPins(base.pins.clone());
        let old_edition = base
            .served_edition
            .clone()
            .context("a partial run's base names its served edition")?;
        // The base side: only what moved is read again, and the changed keys are found once, from
        // the two sides' spools; a 시도 with none is not loaded below.
        let old = spool_base(
            stage,
            connection,
            &Source::Pinned {
                catalog: LAKEHOUSE_CATALOG,
                pins: &old_pins,
            },
            &old_edition,
            spool_dir.path(),
            &moved,
        )?;
        let at = Instant::now();
        targets_from_spools(stage, connection, spool_dir.path(), &old.compared)?;
        targets_millis = u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX);
        sidos.extend(old.sidos);
    }
    // A 시도 the head holds and the served edition no longer does (a merged 시도) is diffed too: its
    // keys get removal markers.
    if let Some(head) = head {
        sidos.extend(head_sidos(connection, table, head)?);
    }
    duck.require_room(0)?;
    let parcels_snapshot = pins
        .get(PARCEL_SOURCE)
        .context("the parcels are pinned")?
        .to_string();
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let mut commit = Commit {
        table,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: parcels_snapshot.clone(),
        published_at_utc: stamp.clone(),
    };
    let run = versions::run_id(table, &pins, &code, &edition);
    let (mut diff, mut digests, mut files) = (Diff::default(), Digests::default(), Vec::new());
    let (mut rows, mut targets) = (0, (!every_pnu).then_some(0));
    // Of a table that commits a 시도 at a time: the 시도 this run committed, the ones it found at
    // the wanted version, the newest commit, and whether a 시도 with nothing to append still waits
    // for a commit to record its state.
    let (mut built_sidos, mut current_sidos) = (Vec::new(), Vec::new());
    let (mut newest, mut state_owed) = (None, false);
    // Every 시도 is diffed against the head the run read (a commit of one 시도 changes no other's
    // rows); each commit is appended at the head the one before it made.
    let mut append_head = head;
    let max_loss = max_row_loss_fraction_of(kind.replaced)?;
    let mut phases = Phases::default();
    *phases.0.entry("targets").or_default() += targets_millis;
    let mut checks = Vec::new();
    for sido in &sidos {
        if requested.as_ref().is_some_and(|keys| !keys.contains(sido)) {
            continue;
        }
        let recorded = states.sidos.get(sido).cloned();
        if recorded
            .as_ref()
            .is_some_and(|state| state.version == wanted)
        {
            current_sidos.push(sido.clone());
            continue;
        }
        // A 시도 is built from its changed PNUs only when it stands where the base commit does: one
        // a run that died left behind is built whole.
        let from = states.built_from(sido).cloned();
        let at_base = from.as_ref().is_none_or(|from| {
            base.as_ref().is_some_and(|base| {
                from.pins == base.pins
                    && from.code_version == base.code_version
                    && Some(&from.served_edition) == base.served_edition.as_ref()
            })
        });
        let whole_sido = every_pnu || !at_base || (kind.per_sido_commits && recorded.is_none());
        // The session's connection is borrowed for the 시도's work only: a commit below awaits.
        let named = {
            let connection = duck.connection();
            // Asked before the 시도 is loaded: one with no changed key is not read at all.
            let named = if whole_sido {
                None
            } else {
                let count: i64 = connection.query_row(
                    &format!("SELECT count(*) FROM targets WHERE {}", sido_range(sido)?),
                    [],
                    |row| row.get(0),
                )?;
                Some(u64::try_from(count)?)
            };
            if named != Some(0) {
                let at = Instant::now();
                rows += load_region(stage, connection, spool_dir.path(), "", sido)?;
                phases.add("load", at);
            }
            named
        };
        if named == Some(0) {
            // No key of this 시도 changed: nothing is built, read from the head or staged. A table
            // that commits a 시도 at a time still records that the 시도 stands at this version.
            if let Some(state) = &recorded {
                states.record(sido, &wanted, &built_from, state.current_records);
                built_sidos.push(sido.clone());
                state_owed = true;
            }
            continue;
        }
        let (sido_diff, sido_digests, sido_files) = {
            let connection = duck.connection();
            let at = Instant::now();
            // One 시군구 at a time (rules.txt "per-sigungu is the shared default"): no statement holds a 시도.
            build_in_parts(
                stage,
                connection,
                named.is_none(),
                &parcels_snapshot,
                &stamp,
            )?;
            phases.add("build", at);
            let at = Instant::now();
            ensure_no_commit_tables(connection)?;
            let invalid: i64 = connection.query_row(
                &format!(
                    "SELECT count(*) FROM built WHERE NOT regexp_full_match(pnu, '[0-9]{{19}}') \
                     OR substr(pnu, 1, 2) <> '{sido}' OR {} IS NULL",
                    stage.section_column()
                ),
                [],
                |row| row.get(0),
            )?;
            ensure!(
                invalid == 0,
                "{invalid} built rows of 시도 {sido} have a malformed PNU, another 시도's or no section: \
                 nothing was appended"
            );
            let range = match named {
                None => sido_range(sido)?,
                // The head is read by the literal ranges of the 시군구 that hold a changed key, so
                // the scan skips every other row group; `pnu IN (subquery)` alone is a semi-join
                // the scan does not see, and read the whole 시도 from the lakehouse.
                Some(_) => format!(
                    "({}) AND pnu IN (SELECT pnu FROM targets)",
                    part_ranges(&target_parts(connection, sido)?)?
                ),
            };
            let sido_diff = versions::diff(connection, &commit, &range)?;
            let sido_digests = versions::digests(connection, &commit)?;
            phases.add("diff", at);
            let at = Instant::now();
            let refused = run_result_checks(stage, connection, table, sido, &mut checks)?;
            phases.add("checks", at);
            ensure!(
                refused.is_empty(),
                "시도 {sido} of {table} breaks {}: nothing of it was appended",
                refused.join(", ")
            );
            let at = Instant::now();
            let sido_files = versions::stage(&duck, &shape, &run, versions_contract)?;
            phases.add("stage", at);
            (sido_diff, sido_digests, sido_files)
        };
        if let (Some(total), Some(count)) = (targets.as_mut(), named) {
            *total += count;
        }
        if !kind.per_sido_commits {
            diff += sido_diff;
            digests += sido_digests;
            files.extend(sido_files);
            continue;
        }
        // The 시도's own commit. Its bounds are the table's (root ADR-0564 §3-4), held to the 시도:
        // the rows it had are the head's record, or what a whole diff read.
        let rows_before = recorded
            .as_ref()
            .map(|state| state.current_records)
            .or_else(|| named.is_none().then_some(sido_diff.current_in_range));
        let rows_after = rows_before
            .map(|rows_before| {
                (rows_before + sido_diff.added)
                    .checked_sub(sido_diff.removed)
                    .with_context(|| format!("시도 {sido} removes more keys than it holds"))
            })
            .transpose()?;
        let explained = from.as_ref().and_then(|from| {
            if from.code_version != code {
                Some(format!("the code version moved to {code}"))
            } else if from.served_edition != edition {
                Some(format!(
                    "the served parcel edition moved {} -> {edition}",
                    from.served_edition
                ))
            } else {
                None
            }
        });
        let explained = if from.is_none() {
            moved_code_or_edition.clone()
        } else {
            explained
        };
        if let (Some(rows_before), Some(rows_after)) = (
            rows_before.filter(|rows_before| *rows_before > 0),
            rows_after,
        ) {
            #[allow(clippy::cast_precision_loss)]
            let floor = (1.0 - max_loss) * rows_before as f64;
            #[allow(clippy::cast_precision_loss)]
            if (rows_after as f64) < floor {
                bail!(
                    "시도 {sido} of {table} would hold {rows_after} current rows, fewer than the {floor:.0} its \
                     {rows_before} allow (gold-panel-rebuild.contract.json max_row_loss_fraction): \
                     nothing of it was appended"
                );
            }
            #[allow(clippy::cast_precision_loss)]
            let fraction = sido_diff.keys() as f64 / rows_before as f64;
            if explained.is_none() && fraction > MAX_CHANGED_KEY_FRACTION {
                bail!(
                    "{} of 시도 {sido}'s {rows_before} current rows of {table} changed ({fraction:.3}, over \
                     {MAX_CHANGED_KEY_FRACTION}) with neither the code nor the served edition moved: \
                     nothing of it was appended (root ADR-0564 §3.4)",
                    sido_diff.keys()
                );
            }
        }
        let rows_after = rows_after.context(
            "a 시도 built from its changed PNUs has its rows recorded at the head (root ADR-0615 §2-4)",
        )?;
        states.record(sido, &wanted, &built_from, rows_after);
        built_sidos.push(sido.clone());
        diff += sido_diff.clone();
        if sido_files.is_empty() {
            // Nothing to append: the next commit records the 시도's state with its own.
            state_owed = true;
            continue;
        }
        let (fingerprint, current_records) =
            versions::after_digests(&commit, &sido_diff, sido_digests)?;
        let mut properties = commit_properties(
            &commit,
            (fingerprint, current_records),
            &built_from,
            sido_diff.keys(),
            as_of.as_deref(),
        )?;
        properties.insert(
            versions::PARTITION_STATE_PROPERTY.to_owned(),
            states.property()?,
        );
        if let Some(why) = &explained {
            properties.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
        }
        let batch = format!("{run}-{sido}-{wanted}");
        let at = Instant::now();
        let read_head = std::mem::replace(&mut commit.head, append_head);
        let snapshot = versions::append(
            &committer,
            &catalog,
            &commit,
            &sido_files,
            &batch,
            properties,
        )
        .await?;
        commit.head = read_head;
        append_head = Some(snapshot);
        phases.add("commit", at);
        println!(
            "committed {table} 시도 {sido}: snapshot {snapshot} version_seq={} keys_changed={}",
            commit.version_seq,
            sido_diff.keys()
        );
        // The table's totals and number go on from this commit.
        commit.base = Some(versions::Base {
            snapshot_id: snapshot,
            version_seq: commit.version_seq,
            pins: pins.clone(),
            code_version: code.clone(),
            served_edition: Some(edition.clone()),
            current_records,
            fingerprint,
            as_of: as_of.clone(),
        });
        commit.version_seq += 1;
        newest = Some(snapshot);
        state_owed = false;
    }
    drop(spool_dir);
    println!(
        "gold-versions {table} built {} PNUs of {} served ({rows} rows) in {} 시도 in {:.1}s (memory peak {:?} bytes)",
        targets.map_or_else(|| "every".to_owned(), |count: u64| count.to_string()),
        read.served_parcels,
        sidos.len(),
        started.elapsed().as_secs_f64(),
        process_memory::cgroup_peak_bytes()
    );
    if kind.per_sido_commits {
        if state_owed {
            // The 시도 that had nothing to append: one commit of no file records their state.
            let totals = commit
                .base
                .as_ref()
                .map_or(((0, 0), 0), |base| (base.fingerprint, base.current_records));
            let mut properties =
                commit_properties(&commit, totals, &built_from, 0, as_of.as_deref())?;
            properties.insert(
                versions::PARTITION_STATE_PROPERTY.to_owned(),
                states.property()?,
            );
            let batch = format!("{run}-state-{wanted}");
            commit.head = append_head;
            newest = Some(
                versions::append(&committer, &catalog, &commit, &[], &batch, properties).await?,
            );
        }
        drop(duck);
        let tags = match newest {
            Some(snapshot) => Some(versions::pin(&catalog, table, snapshot, &pins).await?),
            None => None,
        };
        write_summary(
            &summary_path,
            &json!({
                "job": kind.job, "engine": "duckdb", "table": table,
                "outcome": if newest.is_some() { "committed" } else { "unchanged" },
                "created_table": created, "snapshot_id": newest, "parent_snapshot_id": head,
                "run": run, "pins": pins, "moved": moved, "served_edition": edition,
                "shadow_of": shadow_build, "code_version": code, "as_of": as_of,
                "data_version": wanted,
                "version_seq": commit.base.as_ref().map(|base| base.version_seq),
                "current_records": commit.base.as_ref().map(|base| base.current_records),
                "content_fingerprint": commit.base.as_ref().map(|base| versions::fingerprint_text(base.fingerprint)),
                "partitions": {"built": built_sidos, "found_current": current_sidos, "requested": requested},
                "every_pnu": every_pnu, "targets": targets,
                "served_parcels": read.served_parcels, "rows": rows, "read_millis": read.read_millis,
            "phase_millis": phases.0, "checks": checks,
                "sidos": sidos, "diff": diff, "tags": tags,
                "duckdb_memory_limit_bytes": memory_limit,
                "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!(
            "committed {table} a 시도 at a time: {} built, {} already at {wanted}, keys_changed={} in {:.0}s",
            built_sidos.len(),
            current_sidos.len(),
            diff.keys(),
            started.elapsed().as_secs_f64()
        );
        return Ok(());
    }
    if as_of.is_some() && diff.keys() == 0 && moved.is_empty() && moved_code_or_edition.is_none() {
        write_summary(
            &summary_path,
            &json!({
                "job": kind.job, "engine": "duckdb", "table": table, "outcome": "unchanged",
                "pins": pins, "served_edition": edition, "shadow_of": shadow_build, "as_of": as_of,
                "base_as_of": base.as_ref().and_then(|base| base.as_of.clone()),
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {table} nothing to do: no input moved and no document changed since its base's instant");
        return Ok(());
    }
    let (fingerprint, current_records) = versions::after_digests(&commit, &diff, digests)?;
    let mut recorded_explanation = None;
    if let Some(base) = &base {
        #[allow(clippy::cast_precision_loss)]
        let floor = (1.0 - max_loss) * base.current_records as f64;
        #[allow(clippy::cast_precision_loss)]
        if (current_records as f64) < floor {
            bail!(
                "{table} would hold {current_records} current rows, fewer than the {floor:.0} its \
                 {} allow (gold-panel-rebuild.contract.json max_row_loss_fraction): \
                 nothing was appended",
                base.current_records
            );
        }
        #[allow(clippy::cast_precision_loss)]
        let fraction = diff.keys() as f64 / (base.current_records.max(1)) as f64;
        match moved_code_or_edition {
            Some(why) => recorded_explanation = Some(why),
            None if fraction > MAX_CHANGED_KEY_FRACTION => bail!(
                "{} of {table}'s {} current rows changed ({fraction:.3}, over \
                 {MAX_CHANGED_KEY_FRACTION}) with neither the code nor the served edition moved: \
                 nothing was appended (root ADR-0564 §3.4)",
                diff.keys(),
                base.current_records
            ),
            None => {}
        }
    }

    // Commit.
    let mut properties = commit_properties(
        &commit,
        (fingerprint, current_records),
        &built_from,
        diff.keys(),
        None,
    )?;
    if let Some(why) = &recorded_explanation {
        properties.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
    }
    if let Some(as_of) = &as_of {
        properties.insert(AS_OF_PROPERTY.to_owned(), as_of.clone());
    }
    let snapshot =
        versions::append(&committer, &catalog, &commit, &files, &run, properties).await?;
    drop(duck);
    let tags = versions::pin(&catalog, table, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": kind.job, "engine": "duckdb", "table": table, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "moved": moved,
            "served_edition": edition, "shadow_of": shadow_build, "code_version": code, "as_of": as_of,
            "every_pnu": every_pnu, "targets": targets, "recorded_explanation": recorded_explanation,
            "served_parcels": read.served_parcels, "rows": rows, "read_millis": read.read_millis,
            "phase_millis": phases.0, "checks": checks,
            "sidos": sidos,
            "diff": diff, "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": files.len(), "tags": tags, "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "committed {table}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records} in {:.0}s",
        diff.keys(),
        diff.added,
        diff.changed,
        diff.removed,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The milliseconds a run spent in each phase of its 시도 (`load`, `targets`, `build`, `diff`,
/// `stage`, `commit`), summed over them: what the summary shows of where a run's time went.
#[derive(Debug, Default)]
struct Phases(std::collections::BTreeMap<&'static str, u64>);

impl Phases {
    fn add(&mut self, phase: &'static str, since: Instant) {
        *self.0.entry(phase).or_default() +=
            u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX);
    }
}

/// Runs the kind's result checks on the 시도 the connection holds, prints each as a
/// `gold-versions-check` line (a run a check refuses writes no summary), adds them to `results` and
/// returns the blocking ones that counted a row.
fn run_result_checks(
    stage: &dyn KindVersionsStage,
    connection: &foundation_publisher_silver::duckdb::Connection,
    table: &str,
    sido: &str,
    results: &mut Vec<serde_json::Value>,
) -> anyhow::Result<Vec<&'static str>> {
    let mut refused = Vec::new();
    for check in stage.result_checks() {
        let breaches: i64 = connection
            .query_row(&check.sql, [], |row| row.get(0))
            .with_context(|| format!("the check {} of {table} did not run", check.name))?;
        let result = json!({
            "table": table, "sido": sido, "check": check.name, "breaches": breaches,
            "blocking": check.blocking, "passed": breaches == 0,
        });
        println!("gold-versions-check {result}");
        results.push(result);
        if check.blocking && breaches != 0 {
            refused.push(check.name);
        }
    }
    Ok(refused)
}

/// The 시군구 (the first five digits) of `sido`'s changed keys in `targets`.
fn target_parts(
    connection: &foundation_publisher_silver::duckdb::Connection,
    sido: &str,
) -> anyhow::Result<Vec<String>> {
    Ok(connection
        .prepare(&format!(
            "SELECT DISTINCT substr(pnu, 1, 5) FROM targets WHERE {} ORDER BY 1",
            sido_range(sido)?
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

/// The key ranges of `parts` (시군구), as a predicate on `pnu` of literals a scan can skip by.
///
/// # Errors
/// Refuses a part that is not five digits, and no part at all.
pub(crate) fn part_ranges(parts: &[String]) -> anyhow::Result<String> {
    ensure!(!parts.is_empty(), "no 시군구 holds a changed key");
    let ranges = parts
        .iter()
        .map(|part| {
            let number: u32 = part
                .parse()
                .ok()
                .filter(|_| part.len() == 5 && part.bytes().all(|byte| byte.is_ascii_digit()))
                .with_context(|| format!("the 시군구 {part:?} is not five digits"))?;
            Ok(if number == 99_999 {
                format!("(pnu >= '{part}')")
            } else {
                format!("(pnu >= '{part}' AND pnu < '{:05}')", number + 1)
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ranges.join(" OR "))
}

/// The properties every commit of a kind's table records: its number, the table's current rows and
/// fingerprint after it, what it was built from and the keys it changed.
fn commit_properties(
    commit: &Commit<'_>,
    (fingerprint, current_records): ((u64, u128), u64),
    from: &versions::BuiltFrom,
    keys_changed: u64,
    as_of: Option<&str>,
) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    let mut properties = std::collections::BTreeMap::from([
        (
            VERSION_SEQ_PROPERTY.to_owned(),
            commit.version_seq.to_string(),
        ),
        (
            CURRENT_RECORDS_PROPERTY.to_owned(),
            current_records.to_string(),
        ),
        (
            FINGERPRINT_PROPERTY.to_owned(),
            versions::fingerprint_text(fingerprint),
        ),
        (
            SOURCE_SNAPSHOTS_PROPERTY.to_owned(),
            versions::pins_property(&from.pins)?,
        ),
        (KEYS_CHANGED_PROPERTY.to_owned(), keys_changed.to_string()),
        (CODE_VERSION_PROPERTY.to_owned(), from.code_version.clone()),
        (
            SERVED_EDITION_PROPERTY.to_owned(),
            from.served_edition.clone(),
        ),
    ]);
    if let Some(as_of) = as_of {
        properties.insert(AS_OF_PROPERTY.to_owned(), as_of.to_owned());
    }
    Ok(properties)
}

/// Whether `text` is a UTC instant as the builds write them (`YYYY-MM-DDTHH:MM:SSZ`, with an
/// optional fraction): safe to write into SQL as a literal.
fn is_instant(text: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(text).is_ok()
        && text.ends_with('Z')
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'-' | b':' | b'T' | b'Z' | b'.'))
}

/// The instant the replaced table's build commit was built as of: its rows' newest
/// `published_at_utc` (`onbid_public_auction_silver_to_gold.previous_build_time`; a merge that
/// rebuilt some PNUs leaves its own instant on those and the older one on the rest).
///
/// # Errors
/// Returns an error when the table cannot be read at `build`, holds no row, or its instant is not one.
fn replaced_as_of(work: &Path, replaced: &str, build: i64) -> anyhow::Result<String> {
    ensure!(
        replaced
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || matches!(byte, b'_' | b'.')),
        "{replaced} is not a plain table name"
    );
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let instant: Option<String> = duck.connection().query_row(
        &format!(
            "SELECT CAST(max(published_at_utc) AS VARCHAR) FROM {LAKEHOUSE_CATALOG}.{replaced} \
             AT (VERSION => {build})"
        ),
        [],
        |row| row.get(0),
    )?;
    let instant = instant.with_context(|| format!("{replaced} holds no row at {build}"))?;
    ensure!(
        is_instant(&instant),
        "{replaced} at {build} was built as of {instant:?}, not a UTC instant"
    );
    Ok(instant)
}

/// The key range of one 시도, as a predicate on `pnu` that skips the files outside it.
pub(crate) fn sido_range(sido: &str) -> anyhow::Result<String> {
    let number: u8 = sido
        .parse()
        .ok()
        .filter(|_| sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()))
        .with_context(|| format!("the 시도 {sido:?} is not two digits"))?;
    Ok(if number == 99 {
        format!("(pnu >= '{sido}')")
    } else {
        format!("(pnu >= '{sido}' AND pnu < '{:02}')", number + 1)
    })
}

/// The 시도 the head's rows lie in (a removal marker's too: a superset of its current rows').
pub(crate) fn head_sidos(
    connection: &foundation_publisher_silver::duckdb::Connection,
    table: &str,
    head: i64,
) -> anyhow::Result<BTreeSet<String>> {
    connection
        .prepare(&format!(
            "SELECT DISTINCT substr(pnu, 1, 2) FROM {LAKEHOUSE_CATALOG}.{table} AT (VERSION => {head})"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()
        .context("DuckDB failed to read the head's 시도")
}

/// The run's spool directory, removed when the run ends, failed or not (the runs a disk keeps hold
/// summaries and logs, not spools).
pub(crate) struct Spool(std::path::PathBuf);

impl Spool {
    pub(crate) fn new(path: std::path::PathBuf) -> anyhow::Result<Self> {
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("cannot clear {}", path.display()))?;
        }
        std::fs::create_dir_all(&path)
            .with_context(|| format!("cannot create {}", path.display()))?;
        Ok(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_head_is_read_by_the_literal_ranges_of_the_changed_sigungu() -> anyhow::Result<()> {
        assert_eq!(
            part_ranges(&["41110".to_owned(), "41199".to_owned(), "99999".to_owned()])?,
            "(pnu >= '41110' AND pnu < '41111') OR (pnu >= '41199' AND pnu < '41200') OR (pnu >= '99999')"
        );
        assert_eq!(
            part_ranges(&["09999".to_owned()])?,
            "(pnu >= '09999' AND pnu < '10000')"
        );
        assert!(part_ranges(&[]).is_err());
        for bad in ["4111", "411100", "4111a", ""] {
            assert!(part_ranges(&[bad.to_owned()]).is_err(), "{bad:?} was taken");
        }
        Ok(())
    }

    #[test]
    fn a_run_is_asked_for_sido_by_their_two_digits_or_for_none() -> anyhow::Result<()> {
        assert_eq!(requested_partitions(None)?, None);
        assert_eq!(
            requested_partitions(Some("41, 11,41"))?,
            Some(BTreeSet::from(["11".to_owned(), "41".to_owned()]))
        );
        for bad in ["", " , ", "4", "411", "4a", "11,national"] {
            assert!(
                requested_partitions(Some(bad)).is_err(),
                "{bad:?} was taken"
            );
        }
        Ok(())
    }

    /// A kind line that names `place_identity` gives its table a `place_id` column (by_pnu_kind
    /// derive); a stage that did not carry it would write every row's id as null, and the takeover
    /// verdict would fail on every row (the factories are the first lane that carries it).
    #[test]
    fn every_lane_s_stage_carries_the_place_id_its_table_has() -> anyhow::Result<()> {
        for kind in [
            &REGISTER_MASTERS_BUILD,
            &DEMOLITIONS_BUILD,
            &ENERGY_BUILD,
            &SUB_PARCELS_BUILD,
            &BUILDING_PERMITS_BUILD,
            &PARCEL_PANEL_BUILD,
            &OWNERSHIP_BUILD,
            &FACTORIES_BUILD,
            &PUBLIC_AUCTION_BUILD,
        ] {
            let has = contract(kind.replaced)?
                .columns
                .iter()
                .any(|column| column.name == "place_id");
            assert_eq!(has, kind.stage.carries_place_id(), "{}", kind.table);
        }
        Ok(())
    }

    #[test]
    fn an_instant_is_a_utc_rfc3339_time_and_nothing_else() {
        for good in ["2099-01-02T03:04:05Z", "2099-01-02T03:04:05.123Z"] {
            assert!(is_instant(good), "{good}");
        }
        for bad in [
            "2099-01-02 03:04:05",
            "2099-01-02T03:04:05+09:00",
            "2099-01-02T03:04:05Z'",
            "",
        ] {
            assert!(!is_instant(bad), "{bad}");
        }
    }

    #[test]
    fn a_sido_s_range_bounds_its_keys_and_the_last_one_is_open() -> anyhow::Result<()> {
        assert_eq!(sido_range("41")?, "(pnu >= '41' AND pnu < '42')");
        assert_eq!(sido_range("09")?, "(pnu >= '09' AND pnu < '10')");
        assert_eq!(sido_range("99")?, "(pnu >= '99')");
        for bad in ["4", "411", "4'", "+1"] {
            assert!(sido_range(bad).is_err(), "{bad} was taken");
        }
        Ok(())
    }

    #[test]
    fn a_spool_is_removed_when_its_run_ends() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let path = work.path().join("spool");
        std::fs::create_dir_all(path.join("stale"))?;
        {
            let spool = Spool::new(path.clone())?;
            assert!(!spool.path().join("stale").exists());
            std::fs::write(spool.path().join("part.parquet"), b"x")?;
        }
        assert!(!path.exists());
        Ok(())
    }
}
