//! The publisher's one build of a parcel kind's Gold as a table of appended row versions (root
//! ADR-0564 §3, §6): `build-parcel-register-masters-versions`, `build-parcel-demolitions-versions`,
//! `build-parcel-energy-versions`, `build-parcel-sub-parcels-versions`,
//! `build-parcel-building-permits-versions`, `build-parcel-panel-versions`,
//! `build-parcel-ownership-versions`, `build-parcel-factories-versions`,
//! `build-parcel-public-auction-versions`, `build-parcel-real-transaction-versions`,
//! `build-unit-price-versions`. `DuckDB` inside the publisher computes the kind
//! (`foundation_publisher_silver::kind_versions_stage`, one stage per kind) and the publisher appends
//! the keys that changed (`gold_versions_commit`). A new lane is a stage, a [`KindBuild`], a kind line
//! and a job. What a lane needs beyond its SQL is declared, never a driver of its own (root ADR-0615
//! §7): inputs pinned to another Gold's build (the rebuild contract's `pins_from`), an input read by
//! one batch (`batch_input`), and checks of the whole build held before anything is appended
//! (`national_checks`).
//!
//! While a lane serves the Spark table its versions table replaces, the versions table is its shadow:
//! its inputs are pinned to that table's build-tag commit (`planned_inputs`), so the takeover verdict
//! compares two engines over one set of inputs, and its first build holds every key. One run: create
//! the table if it is not there, plan, mend the tags, stop when the inputs, the served edition and the
//! code are what the base commit was built from; otherwise rebuild the PNUs the stage names
//! (every PNU for a first build, a new code version or edition, or a moved whole-table input), diff
//! them against the head's current rows, refuse a build that loses or changes more than its bounds,
//! and append.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    building_energy_stage::ENERGY,
    building_permit_stage::BUILDING_PERMITS,
    building_sub_parcel_stage::SUB_PARCELS,
    court_kind_stage::COURT_AUCTION,
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    factory_kind_stage::FACTORIES,
    kind_versions_stage::{
        build_in_parts, ensure_no_commit_tables, leave_input_batch, load_ranged, load_region,
        spool, spool_base, targets_from_spools, KindVersionsStage, PARCEL_SOURCE,
    },
    land_ownership_stage::OWNERSHIP,
    onbid_kind_stage::PUBLIC_AUCTION,
    parcel_boundary_served_stage::SERVED_BOUNDARIES,
    parcel_nearby_stage::{NearbyPins, Source, NEARBY},
    parcel_panel_stage::PANEL,
    process_memory,
    real_transaction_versions_stage::REAL_TRANSACTION,
    record_kind_stage::{DEMOLITIONS, REGISTER_MASTERS},
    unit_price_stage::UNIT_PRICE,
};
use lakehouse_domain::{GOLD_VERSIONS_REMOVAL_COLUMN, GOLD_VERSION_SEQ_COLUMN};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_documents_check::DocumentRules,
    gold_versions_commit::{
        self as versions, Commit, Diff, Digests, AS_OF_PROPERTY, CODE_VERSION_PROPERTY,
        CURRENT_RECORDS_PROPERTY, FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY,
        RECORDED_EXPLANATION_PROPERTY, SERVED_EDITION_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY,
        VERSION_SEQ_PROPERTY,
    },
    gold_versions_support::{
        contract, lakehouse_read, lakehouse_reader_read, max_row_loss_fraction_of, served_edition,
        write_summary, MAX_CHANGED_KEY_FRACTION, MEMORY_SHARE, REBUILD_CONTRACT,
    },
    public_data_control_support::{optional_env_value, required_env_value},
};

mod checks;
mod rehearsal;
mod replaced;

use checks::{explaining, run_national_checks, run_result_checks};
use rehearsal::Rehearsal;
use replaced::{replaced_as_of, replaced_edition};

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

/// What a lane's reader reads, which says whether a run of the lane is a shadow (root ADR-0564 §6).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Serves {
    /// A by-PNU lane: the lane contract names the Gold it bakes.
    ByPnuLane(&'static str),
    /// A Gold outside the by-PNU lanes (`gold_versions_tables_outside_the_lanes`): its reader reads
    /// this table.
    Table(&'static str),
}

/// One lane's build: the versions table, what its reader reads (whether this run is a shadow, root
/// ADR-0564 §6), the table whose columns it carries and whose build a shadow follows, and the stage
/// that computes it.
pub(crate) struct KindBuild {
    pub table: &'static str,
    pub lane: Serves,
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
    lane: Serves::ByPnuLane("parcel-register-masters"),
    replaced: "gold.parcel_register_masters",
    job: "build-parcel-register-masters-versions",
    stage: &REGISTER_MASTERS,
    per_sido_commits: false,
};

/// Lane 2: the 철거·멸실 reports (root ADR-0399).
pub(crate) const DEMOLITIONS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_demolitions_versions",
    lane: Serves::ByPnuLane("parcel-demolitions"),
    replaced: "gold.parcel_demolitions",
    job: "build-parcel-demolitions-versions",
    stage: &DEMOLITIONS,
    per_sido_commits: false,
};

/// Lane 3: the building energy (root ADR-0398).
pub(crate) const ENERGY_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_energy_versions",
    lane: Serves::ByPnuLane("parcel-energy"),
    replaced: "gold.parcel_energy",
    job: "build-parcel-energy-versions",
    stage: &ENERGY,
    per_sido_commits: false,
};

/// Lane 4: the 부속지번, read from both of a link's parcels (root ADR-0399).
pub(crate) const SUB_PARCELS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_sub_parcels_versions",
    lane: Serves::ByPnuLane("parcel-sub-parcels"),
    replaced: "gold.parcel_sub_parcels",
    job: "build-parcel-sub-parcels-versions",
    stage: &SUB_PARCELS,
    per_sido_commits: false,
};

/// Lane 5: the 건축인허가 and the other files of the permit family (root ADR-0451, ADR-0464).
pub(crate) const BUILDING_PERMITS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_building_permits_versions",
    lane: Serves::ByPnuLane("parcel-building-permits"),
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
    lane: Serves::ByPnuLane("parcel"),
    replaced: "gold.parcel_panel",
    job: "build-parcel-panel-versions",
    stage: &PANEL,
    per_sido_commits: false,
};

/// Lane 6: the owners (root ADR-0361).
pub(crate) const OWNERSHIP_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_ownership_versions",
    lane: Serves::ByPnuLane("parcel-ownership"),
    replaced: "gold.parcel_ownership",
    job: "build-parcel-ownership-versions",
    stage: &OWNERSHIP,
    per_sido_commits: true,
};

/// Lane 7: the registered factories (root ADR-0449).
pub(crate) const FACTORIES_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_factories_versions",
    lane: Serves::ByPnuLane("parcel-factories"),
    replaced: "gold.parcel_factories",
    job: "build-parcel-factories-versions",
    stage: &FACTORIES,
    per_sido_commits: false,
};

/// Lane 8: the 공매 rounds (root ADR-0287, ADR-0340), built as of an instant.
pub(crate) const PUBLIC_AUCTION_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_public_auction_versions",
    lane: Serves::ByPnuLane("parcel-public-auction"),
    replaced: "gold.parcel_public_auction",
    job: "build-parcel-public-auction-versions",
    stage: &PUBLIC_AUCTION,
    per_sido_commits: false,
};

/// Lane 9: the 실거래 deals (root ADR-0350, ADR-0371): a deal is placed by its address, so the stage
/// reads its own inputs by the part that reads each row (root ADR-0615 §7). A 시도 is its own
/// commit: the first build is longer than one run of its unit, and the table keeps the 시도 a run
/// committed.
pub(crate) const REAL_TRANSACTION_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_real_transaction_versions",
    lane: Serves::ByPnuLane("parcel-real-transaction"),
    replaced: "gold.parcel_real_transaction",
    job: "build-parcel-real-transaction-versions",
    stage: &REAL_TRANSACTION,
    per_sido_commits: true,
};

/// The 호 공시가격 (root ADR-0190, ADR-0204): the newest price batch's rows (the stage's
/// `batch_input`), the registers pinned to the building Gold's build once the lane is no shadow
/// (`pins_from`), and a build that shrinks the price histories of more of the current units than
/// the contract allows refused before anything is appended (`national_checks`). Committed whole: that
/// check judges every 시도 together.
/// The parcel nearby kind (root ADR-0465): the nearest facilities, the store count and the highway
/// list of each served parcel. A commit that only the stores moved records why (the stage's
/// `change_explanation`, root ADR-0596). Committed whole: the explanation judges every 시도's diff.
pub(crate) const NEARBY_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_nearby_versions",
    lane: Serves::ByPnuLane("parcel-nearby"),
    replaced: "gold.parcel_nearby",
    job: "build-parcel-nearby-versions",
    stage: &NEARBY,
    per_sido_commits: false,
};

/// The served parcel boundaries (root ADR-0133 §2, ADR-0602): one edition as the source holds it
/// (parcel geometry takes no edits, root ADR-0621 §1), read a 시도 at a time by its key ranges (the
/// stage's ranged read). A 시도 is its own commit: the tile bake's inputs are each 시도's state (root
/// ADR-0618). The tile bake reads the replaced table until its switch names this one.
pub(crate) const BOUNDARY_SERVED_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_boundary_served_versions",
    lane: Serves::Table(lakehouse_domain::lakehouse::GOLD_PARCEL_BOUNDARY_SERVED.table_name),
    replaced: lakehouse_domain::lakehouse::GOLD_PARCEL_BOUNDARY_SERVED.table_name,
    job: "build-parcel-boundary-served-versions",
    stage: &SERVED_BOUNDARIES,
    per_sido_commits: true,
};

/// The candidate edition a rehearsal builds (root ADR-0618 §9).
pub(crate) const REHEARSAL_EDITION_ENV: &str =
    "FOUNDATION_PLATFORM_GOLD_VERSIONS_REHEARSAL_EDITION";
/// Where a rehearsal writes what it built (create-only: a rehearsal's files are evidence).
pub(crate) const REHEARSAL_DIR_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_REHEARSAL_DIR";

pub(crate) const UNIT_PRICE_BUILD: KindBuild = KindBuild {
    table: "gold.unit_price_versions",
    lane: Serves::ByPnuLane("unit-price"),
    replaced: "gold.unit_price",
    job: "build-unit-price-versions",
    stage: &UNIT_PRICE,
    per_sido_commits: false,
};

/// Lane 9: the 경매 items in progress and sale results (root ADR-0287, ADR-0353, ADR-0540), built as
/// of an instant.
pub(crate) const COURT_AUCTION_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_court_auction_versions",
    lane: Serves::ByPnuLane("parcel-court-auction"),
    replaced: "gold.parcel_court_auction",
    job: "build-parcel-court-auction-versions",
    stage: &COURT_AUCTION,
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
    // The versions table's columns but the two each commit adds: the replaced table's, and the
    // digest a replaced table whose producer wrote none gains (the served boundaries).
    let content_columns: Vec<&'static str> = versions_contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| ![GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(name))
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
    // A rehearsal reads with the reader's catalog token and writes nothing (root ADR-0618 §9).
    let rehearsal = Rehearsal::from_env()?;
    let catalog_config = if rehearsal.is_some() {
        LakehouseCatalogConfig::reader_from_env()?
    } else {
        LakehouseCatalogConfig::from_env()?
    };
    let catalog = IcebergRestCatalog::new(catalog_config.clone())?;
    let committer = match rehearsal {
        Some(_) => None,
        None => Some(LakehouseDataCommitter::from_env().await?),
    };
    let created = match &committer {
        Some(committer) => committer.create_table_if_absent(versions_contract).await?,
        None => false,
    };

    // Plan.
    // A rehearsal of a table that does not exist yet rehearses its first build.
    let history = catalog.load_main_snapshot_history(table).await?;
    ensure!(
        rehearsal.is_some() || history.is_some(),
        "{table} does not exist after its creation"
    );
    let history = history.unwrap_or_default();
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let served_table = match kind.lane {
        Serves::ByPnuLane(name) => by_pnu_serving_manifest_publish::gold_table(
            ByPnuLane::named(name)?
                .with_context(|| format!("the contract names no {name} lane"))?,
        ),
        Serves::Table(served) => served,
    };
    // A rehearsal builds its candidate from the inputs' heads, never as a shadow.
    let versions::Planned {
        mut pins,
        served_edition: edition,
        shadow,
    } = versions::planned_inputs(
        &catalog,
        table,
        if rehearsal.is_some() {
            table
        } else {
            served_table
        },
        &stage.inputs(),
        served_edition()?,
    )
    .await?;
    let edition = rehearsal
        .as_ref()
        .map_or(edition, |rehearsal| rehearsal.edition.clone());
    let ranged = stage.ranged_reads();
    let whole = stage.whole_table_inputs();
    ensure!(
        ranged.iter().all(|read| whole.contains(&read.input)),
        "{table} reads an input by its ranges that is not whole-table: a partial run has no spool \
         of it to compare (root ADR-0615 §7)"
    );
    // Once the lane is no shadow, the inputs its rebuild contract entry names in `pins_from` are
    // read at the snapshots the host Gold's build read (root ADR-0190: a unit's price lies under the
    // PNU the building Gold places the unit at); a shadow reads its replaced build's pins.
    if shadow.is_none() {
        pins.extend(host_pins(&catalog, kind.replaced).await?);
    }
    // What the driver holds only for a table committed whole: a check that judges every 시도
    // together, the batch a commit records, and an explanation judged over every 시도's diff (root
    // ADR-0615 §7).
    let national = stage.national_checks();
    let batch_input = stage.batch_input();
    let explanation = stage.change_explanation();
    ensure!(
        !kind.per_sido_commits
            || (national.is_empty() && batch_input.is_none() && explanation.is_none()),
        "{table} commits a 시도 at a time: a check of the whole build, a batch read or an explanation \
         of a change holds only for a table committed whole (root ADR-0615 §7)"
    );
    let base_batch = batch_input.and_then(|batch| {
        let base = base.as_ref()?;
        history
            .iter()
            .find(|commit| commit.snapshot_id == base.snapshot_id)?
            .summary
            .get(batch.property)
            .cloned()
    });
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = stage.code_version();
    if let (Some(base), None) = (&base, &rehearsal) {
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
    // A shadow whose replaced build records no edition builds the one that build's rows carry.
    let edition = match (&shadow, stage.replaced_edition_sql()) {
        (Some(shadow), Some(sql)) if shadow.served_edition.is_none() => {
            replaced_edition(work, &shadow.replaced, shadow.build, sql)?
        }
        _ => edition,
    };
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
    if unchanged && rehearsal.is_none() {
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
    // A rehearsal builds every row of its candidate.
    let every_pnu = rehearsal.is_some()
        || base.as_ref().is_none_or(|base| {
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
    let shape = match &committer {
        Some(committer) => Some(committer.table_shape(table).await?),
        None => None,
    };
    // The ingest log a batch is chosen by, read here for the same reason.
    let ingest_log = match &batch_input {
        Some(batch) => Some(
            catalog
                .load_ingest_commit_log(batch.input)
                .await?
                .with_context(|| format!("{} does not exist", batch.input))?,
        ),
        None => None,
    };
    // The renderer and schema every changed document is held to (root ADR-0615 §7), loaded here for
    // the same reason. The lane's renderer is found by the table its lane bakes: the replaced
    // table while this one shadows it, this one once the lane took it over.
    let document_rules = match DocumentRules::load(kind.replaced).await? {
        Ok(rules) => Ok(rules),
        Err(_) => DocumentRules::load(table).await?,
    };
    let read = if rehearsal.is_some() {
        lakehouse_reader_read(&catalog_config)?
    } else {
        lakehouse_read()?
    };
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &read)?;
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
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &new_pins,
    };
    // The batch the lane reads, chosen at the pins and left for its read.
    let chosen_batch = match (batch_input, &ingest_log) {
        (Some(batch), Some(log)) => {
            let id = (batch.choose)(connection, &source, log)?;
            leave_input_batch(connection, &id)?;
            Some(id)
        }
        _ => None,
    };
    // A moved batch explains a change of any size too, as a moved code version does.
    let moved_code_or_edition = moved_code_or_edition.or_else(|| {
        let (batch, id) = (batch_input?, chosen_batch.as_ref()?);
        (base.is_some() && base_batch.as_deref() != Some(id.as_str())).then(|| {
            format!(
                "the batch of {} moved {} -> {id}",
                batch.input,
                base_batch.as_deref().unwrap_or("-")
            )
        })
    });
    let read = spool(stage, connection, &source, &edition, "", spool_dir.path())?;
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
    // Every row's `source_snapshot_id`: the served parcels' pin, or a batch read's, whose rows they
    // are.
    let stamp_input = batch_input.map_or(PARCEL_SOURCE, |batch| batch.input);
    let source_snapshot = pins
        .get(stamp_input)
        .with_context(|| format!("{stamp_input} is not pinned"))?
        .to_string();
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let mut commit = Commit {
        table,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: source_snapshot.clone(),
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
    // What a rehearsal wrote, a file a 시도.
    let mut rehearsed = Vec::new();
    if let Err(why) = &document_rules {
        checks.push(json!({
            "table": table, "check": DOCUMENTS_CHECK, "skipped": why, "blocking": true,
        }));
    }
    for check in national.iter().filter(|check| !check.setup_sql.is_empty()) {
        duck.connection()
            .execute_batch(&check.setup_sql)
            .with_context(|| format!("the check {} of {table} did not set up", check.name))?;
    }
    let explaining = explaining(
        explanation,
        &moved,
        base.is_some() && moved_code_or_edition.is_none(),
    );
    if let Some(explanation) = &explaining {
        duck.connection()
            .execute_batch(&explanation.setup_sql)
            .with_context(|| format!("the explanation of {table} did not set up"))?;
    }
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
                // What the stage reads by its ranges: the 시도's, or the 시군구 that hold a changed
                // key.
                if !ranged.is_empty() {
                    let range = match named {
                        None => sido_range(sido)?,
                        Some(_) => format!(
                            "({}) AND pnu IN (SELECT pnu FROM targets)",
                            part_ranges(&target_parts(connection, sido)?)?
                        ),
                    };
                    rows += load_ranged(stage, connection, &source, &edition, &range)?;
                }
                phases.add("load", at);
            }
            named
        };
        if named == Some(0) {
            // No key of this 시도 changed: nothing is built, read from the head or staged. A table
            // that commits a 시도 at a time still records that the 시도 stands at this version.
            if let Some(state) = &recorded {
                states.record(
                    sido,
                    &wanted,
                    &built_from,
                    state.current_records,
                    state.digest_sum()?,
                );
                built_sidos.push(sido.clone());
                state_owed = true;
            }
            continue;
        }
        let (sido_diff, sido_digests, sido_files) = {
            let connection = duck.connection();
            let at = Instant::now();
            // One 시군구 at a time (rules.txt "per-sigungu is the shared default"): no statement holds a 시도.
            build_in_parts(stage, connection, named.is_none(), &source_snapshot, &stamp)?;
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
            // The 시도's share of the checks of the whole build, judged after the last 시도.
            for check in national.iter().filter(|check| !check.gather_sql.is_empty()) {
                connection
                    .execute_batch(&check.gather_sql)
                    .with_context(|| {
                        format!("the check {} of {table} did not gather", check.name)
                    })?;
            }
            if let Some(explanation) = &explaining {
                connection
                    .execute_batch(&explanation.gather_sql)
                    .with_context(|| format!("the explanation of {table} did not gather"))?;
            }
            let mut refused = run_result_checks(stage, connection, table, sido, &mut checks)?;
            if let Ok(rules) = &document_rules {
                let found = rules.changed_documents_hold_their_schema(connection, table)?;
                let result = json!({
                    "table": table, "sido": sido, "check": DOCUMENTS_CHECK,
                    "breaches": found.refused, "rendered": found.rendered,
                    "samples": found.samples, "blocking": true, "passed": found.refused == 0,
                });
                println!("gold-versions-check {result}");
                checks.push(result);
                if found.refused != 0 {
                    refused.push(DOCUMENTS_CHECK);
                }
            }
            phases.add("checks", at);
            ensure!(
                refused.is_empty(),
                "시도 {sido} of {table} breaks {}: nothing of it was appended",
                refused.join(", ")
            );
            let at = Instant::now();
            // A rehearsal writes every built row of the 시도; a run that commits stages the changed
            // ones.
            let sido_files = match &rehearsal {
                Some(rehearsal) => {
                    rehearsed.push(rehearsal.write(connection, sido, &commit.content_columns)?);
                    Vec::new()
                }
                None => versions::stage(
                    &duck,
                    shape
                        .as_ref()
                        .context("a run that commits knows its table's shape")?,
                    &run,
                    versions_contract,
                )?,
            };
            phases.add("stage", at);
            (sido_diff, sido_digests, sido_files)
        };
        if let (Some(total), Some(count)) = (targets.as_mut(), named) {
            *total += count;
        }
        if rehearsal.is_some() {
            diff += sido_diff;
            continue;
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
        // The 시도's own fingerprint goes on from the one the head records; a 시도 that held no row
        // starts at nothing. One appended before the table kept it stays unknown.
        let digest_before = match &recorded {
            Some(state) => state.digest_sum()?,
            None => (rows_before == Some(0)).then_some(0),
        };
        let digest_after = digest_before
            .map(|before| {
                (before + sido_digests.added)
                    .checked_sub(sido_digests.hidden)
                    .with_context(|| format!("시도 {sido} hides digests it does not hold"))
            })
            .transpose()?;
        states.record(sido, &wanted, &built_from, rows_after, digest_after);
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
            versions::BUILD_SCOPE_PROPERTY.to_owned(),
            versions::build_scope(whole_sido).to_owned(),
        );
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
            committer
                .as_ref()
                .context("a run that commits has a committer")?,
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
    // The checks of the whole build, before anything is appended (a table committed whole).
    run_national_checks(
        &national,
        duck.connection(),
        table,
        kind.replaced,
        &mut checks,
    )?;
    // A rehearsal stops here: what it built is in its files, and nothing is appended or tagged.
    if let Some(rehearsal) = &rehearsal {
        drop(duck);
        let summary = json!({
            "job": kind.job, "engine": "duckdb", "table": table, "outcome": "rehearsed",
            "rehearsed_edition": edition, "pins": pins, "code_version": code,
            "files": rehearsed, "rows": rows, "diff_against_head": diff, "checks": checks,
            "phase_millis": phases.0, "read_millis": read.read_millis,
            "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        });
        write_summary(&summary_path, &summary)?;
        write_summary(
            &rehearsal.dir.join("summary.json").display().to_string(),
            &summary,
        )?;
        println!(
            "rehearsed {table} edition {edition}: {} 시도 written to {}, nothing committed",
            rehearsed.len(),
            rehearsal.dir.display()
        );
        return Ok(());
    }
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
                versions::BUILD_SCOPE_PROPERTY.to_owned(),
                versions::build_scope(false).to_owned(),
            );
            properties.insert(
                versions::PARTITION_STATE_PROPERTY.to_owned(),
                states.property()?,
            );
            let batch = format!("{run}-state-{wanted}");
            commit.head = append_head;
            newest = Some(
                versions::append(
                    committer
                        .as_ref()
                        .context("a run that commits has a committer")?,
                    &catalog,
                    &commit,
                    &[],
                    &batch,
                    properties,
                )
                .await?,
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
        // The kind's own explanation is judged at any size, so commits under the bound are recorded
        // too: several in a row while the bake is behind must not sum past its bound unexplained
        // (root ADR-0596).
        let explained = match (&moved_code_or_edition, &explaining) {
            (Some(why), _) => Some(why.clone()),
            (None, Some(explanation)) => (explanation.explain)(
                duck.connection(),
                (diff.added, diff.changed, diff.removed),
                max_loss,
            )?,
            (None, None) => None,
        };
        match explained {
            Some(why) => recorded_explanation = Some(why),
            None if fraction > MAX_CHANGED_KEY_FRACTION => bail!(
                "{} of {table}'s {} current rows changed ({fraction:.3}, over \
                 {MAX_CHANGED_KEY_FRACTION}) with neither the code nor the served edition moved \
                 and nothing the kind declares explaining it: nothing was appended (root ADR-0564 \
                 §3.4)",
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
    properties.insert(
        versions::BUILD_SCOPE_PROPERTY.to_owned(),
        versions::build_scope(every_pnu).to_owned(),
    );
    if let Some(why) = &recorded_explanation {
        properties.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
    }
    if let Some(as_of) = &as_of {
        properties.insert(AS_OF_PROPERTY.to_owned(), as_of.clone());
    }
    if let (Some(batch), Some(id)) = (batch_input, &chosen_batch) {
        properties.insert(batch.property.to_owned(), id.clone());
    }
    let snapshot = versions::append(
        committer
            .as_ref()
            .context("a run that commits has a committer")?,
        &catalog,
        &commit,
        &files,
        &run,
        properties,
    )
    .await?;
    drop(duck);
    let tags = versions::pin(&catalog, table, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": kind.job, "engine": "duckdb", "table": table, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "moved": moved,
            "served_edition": edition, "shadow_of": shadow_build, "code_version": code, "as_of": as_of,
            "input_batch": chosen_batch,
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

/// The check every lane's changed documents get: rendered by the lane's renderer and held to its
/// schema-registry revision.
const DOCUMENTS_CHECK: &str = "changed_documents_hold_their_schema";
/// The pins the rebuild contract's `pins_from` of `replaced` names (root ADR-0190, ADR-0564 §6):
/// the snapshots the host Gold's build-tag commit read of those inputs; none when it names none.
async fn host_pins(
    catalog: &IcebergRestCatalog,
    replaced: &str,
) -> anyhow::Result<BTreeMap<String, i64>> {
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    let from = &contract["tables"][replaced]["pins_from"];
    if from.is_null() {
        return Ok(BTreeMap::new());
    }
    let host = from["gold_table"].as_str().with_context(|| {
        format!("the rebuild contract's pins_from of {replaced} names no gold_table")
    })?;
    // The host's build-tag commit and its pins, read as a shadow of the host reads them. When the
    // host's lane serves its versions table, its takeover PR names that table in pins_from, whose
    // build tag then moves instead (root ADR-0564 §6).
    let shadow = versions::shadow_of(catalog, &format!("{host}_versions"), host)
        .await?
        .with_context(|| format!("{host} has no build to pin {replaced}'s inputs to"))?;
    let mut pins = BTreeMap::new();
    for input in from["inputs"]
        .as_array()
        .with_context(|| format!("the pins_from of {replaced} names no inputs"))?
    {
        let input = input
            .as_str()
            .context("a pins_from input is not a table name")?;
        let snapshot = shadow
            .pins
            .get(input)
            .with_context(|| format!("{host} build {} records no pin of {input}", shadow.build))?;
        pins.insert(input.to_owned(), *snapshot);
    }
    Ok(pins)
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
            "SELECT DISTINCT substr(pnu, 1, 2) FROM {pinned}",
            pinned = foundation_publisher_silver::duckdb_parquet_stage::table_at(table, head)
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
mod tests;
