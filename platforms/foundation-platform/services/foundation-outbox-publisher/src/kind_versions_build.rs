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
        ensure_no_commit_tables, load_region, spool, KindVersionsStage, PARCEL_SOURCE,
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
    public_data_control_support::required_env_value,
};

pub(crate) const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";

/// One lane's build: the versions table, its by-PNU lane (whose served Gold says whether this run
/// is a shadow, root ADR-0564 §6), the table whose columns it carries and whose build a shadow
/// follows, and the stage that computes it.
pub(crate) struct KindBuild {
    pub table: &'static str,
    pub lane: &'static str,
    pub replaced: &'static str,
    pub job: &'static str,
    pub stage: &'static (dyn KindVersionsStage + Sync),
}

/// Lane 1: the 총괄표제부 (root ADR-0450).
pub(crate) const REGISTER_MASTERS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_register_masters_versions",
    lane: "parcel-register-masters",
    replaced: "gold.parcel_register_masters",
    job: "build-parcel-register-masters-versions",
    stage: &REGISTER_MASTERS,
};

/// Lane 2: the 철거·멸실 reports (root ADR-0399).
pub(crate) const DEMOLITIONS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_demolitions_versions",
    lane: "parcel-demolitions",
    replaced: "gold.parcel_demolitions",
    job: "build-parcel-demolitions-versions",
    stage: &DEMOLITIONS,
};

/// Lane 3: the building energy (root ADR-0398).
pub(crate) const ENERGY_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_energy_versions",
    lane: "parcel-energy",
    replaced: "gold.parcel_energy",
    job: "build-parcel-energy-versions",
    stage: &ENERGY,
};

/// Lane 4: the 부속지번, read from both of a link's parcels (root ADR-0399).
pub(crate) const SUB_PARCELS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_sub_parcels_versions",
    lane: "parcel-sub-parcels",
    replaced: "gold.parcel_sub_parcels",
    job: "build-parcel-sub-parcels-versions",
    stage: &SUB_PARCELS,
};

/// Lane 5: the 건축인허가 and the other files of the permit family (root ADR-0451, ADR-0464).
pub(crate) const BUILDING_PERMITS_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_building_permits_versions",
    lane: "parcel-building-permits",
    replaced: "gold.parcel_building_permits",
    job: "build-parcel-building-permits-versions",
    stage: &BUILDING_PERMITS,
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
};

/// Lane 6: the owners (root ADR-0361).
pub(crate) const OWNERSHIP_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_ownership_versions",
    lane: "parcel-ownership",
    replaced: "gold.parcel_ownership",
    job: "build-parcel-ownership-versions",
    stage: &OWNERSHIP,
};

/// Lane 7: the registered factories (root ADR-0449).
pub(crate) const FACTORIES_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_factories_versions",
    lane: "parcel-factories",
    replaced: "gold.parcel_factories",
    job: "build-parcel-factories-versions",
    stage: &FACTORIES,
};

/// Lane 8: the 공매 rounds (root ADR-0287, ADR-0340), built as of an instant.
pub(crate) const PUBLIC_AUCTION_BUILD: KindBuild = KindBuild {
    table: "gold.parcel_public_auction_versions",
    lane: "parcel-public-auction",
    replaced: "gold.parcel_public_auction",
    job: "build-parcel-public-auction-versions",
    stage: &PUBLIC_AUCTION,
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
    let unchanged = base.as_ref().is_some_and(|base| {
        moved.is_empty()
            && base.code_version == code
            && base.served_edition.as_deref() == Some(edition.as_str())
            && base.as_of == as_of
    });
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
    if !every_pnu {
        let base = base.as_ref().context("a partial run has a base")?;
        let old_pins = NearbyPins(base.pins.clone());
        let old_edition = base
            .served_edition
            .clone()
            .context("a partial run's base names its served edition")?;
        let old = spool(
            stage,
            connection,
            &Source::Pinned {
                catalog: LAKEHOUSE_CATALOG,
                pins: &old_pins,
            },
            &old_edition,
            "_old",
            spool_dir.path(),
        )?;
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
    let commit = Commit {
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
    for sido in &sidos {
        rows += load_region(stage, connection, spool_dir.path(), "", sido)?;
        let named = if every_pnu {
            None
        } else {
            load_region(stage, connection, spool_dir.path(), "_old", sido)?;
            Some(stage.changed_pnus(connection)?)
        };
        stage.build(connection, named.is_none(), &parcels_snapshot, &stamp)?;
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
        let in_sido = sido_range(sido)?;
        let range = match named {
            None => in_sido,
            // A partial run that names no PNU here diffs nothing.
            Some(0) => "FALSE".to_owned(),
            Some(_) => format!("{in_sido} AND pnu IN (SELECT pnu FROM targets)"),
        };
        diff += versions::diff(connection, &commit, &range)?;
        digests += versions::digests(connection, &commit)?;
        files.extend(versions::stage(&duck, &shape, &run, versions_contract)?);
        if let (Some(total), Some(count)) = (targets.as_mut(), named) {
            *total += count;
        }
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
        let max_loss = max_row_loss_fraction_of(kind.replaced)?;
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
    let mut properties = std::collections::BTreeMap::from([
        (VERSION_SEQ_PROPERTY.to_owned(), version_seq.to_string()),
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
            versions::pins_property(&pins)?,
        ),
        (KEYS_CHANGED_PROPERTY.to_owned(), diff.keys().to_string()),
        (CODE_VERSION_PROPERTY.to_owned(), code.clone()),
        (SERVED_EDITION_PROPERTY.to_owned(), edition.clone()),
    ]);
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
