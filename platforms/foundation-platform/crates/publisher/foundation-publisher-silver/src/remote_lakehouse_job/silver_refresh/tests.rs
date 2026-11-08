use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::NaiveDate;

use super::execute::{self, Runtime, SparkLoad};
use super::lane::{ExportKind, Lane, LaneContract, WriteMode};
use super::release::{self, LedgerObject, Release};
use super::*;

const TITLE: &str = "hubgokr__building_register_main";
const UNIT: &str = "hubgokr__building_register_exclusive_unit";
const BASIS: &str = "hubgokr__building_register_basis_outline";

fn month(year: i32, month: u32) -> anyhow::Result<NaiveDate> {
    NaiveDate::from_ymd_opt(year, month, 1).context("valid month")
}

/// A ledger row as the hub collector writes it: first-of-month snapshot, `OPN<day>` file.
fn object(slug: &str, day: &str, updated: Option<u32>) -> anyhow::Result<LedgerObject> {
    use chrono::Datelike as _;
    let date = NaiveDate::parse_from_str(day, "%Y%m%d")?;
    Ok(LedgerObject {
        slug: slug.to_owned(),
        object_key: format!("bronze/source={slug}/OPN{day}SYNTHETIC.zip"),
        snapshot_date: month(date.year(), date.month())?,
        provider_updated_at: updated.and_then(|d| NaiveDate::from_ymd_opt(2099, 12, d)),
        size_bytes: 1024,
        checksum_sha256: "a".repeat(64),
    })
}

fn roles(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(role, slug)| ((*role).to_owned(), (*slug).to_owned()))
        .collect()
}

fn unit_roles() -> BTreeMap<String, String> {
    roles(&[("basis", BASIS), ("title", TITLE), ("unit", UNIT)])
}

// ---- release selection (root ADR-0169 §2) --------------------------------------------------

#[test]
fn the_newest_month_holding_every_role_is_the_release() -> anyhow::Result<()> {
    let ledger = [
        object(UNIT, "20990720", None)?,
        object(TITLE, "20990720", None)?,
        object(BASIS, "20990720", None)?,
        object(UNIT, "20990620", None)?,
        object(TITLE, "20990620", None)?,
        object(BASIS, "20990620", None)?,
    ];
    let release = release::select(&unit_roles(), &ledger)?;
    assert_eq!(release.month.first_day(), month(2099, 7)?);
    assert_eq!(release.vintage(), "209907");
    assert_eq!(release.valid_from_utc(), "2099-07-01T00:00:00Z");
    assert!(release.skipped_incomplete.is_empty());
    assert_eq!(
        release.object("unit")?.file_name(),
        "OPN20990720SYNTHETIC.zip"
    );
    Ok(())
}

#[test]
fn a_newer_month_missing_a_role_is_skipped_not_loaded() -> anyhow::Result<()> {
    let ledger = [
        // 2099-08 has the units but not yet the title or basis export.
        object(UNIT, "20990820", None)?,
        object(UNIT, "20990720", None)?,
        object(TITLE, "20990720", None)?,
        object(BASIS, "20990720", None)?,
    ];
    let release = release::select(&unit_roles(), &ledger)?;
    assert_eq!(release.month.first_day(), month(2099, 7)?);
    assert_eq!(release.skipped_incomplete, [month(2099, 8)?]);
    Ok(())
}

#[test]
fn no_complete_month_is_refused() -> anyhow::Result<()> {
    let ledger = [
        object(UNIT, "20990820", None)?,
        object(TITLE, "20990720", None)?,
    ];
    let error = release::select(&unit_roles(), &ledger)
        .err()
        .context("an incomplete ledger has no release")?;
    assert!(
        error.to_string().contains("no complete release"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn two_objects_of_a_role_resolve_to_the_later_provider_update() -> anyhow::Result<()> {
    let title = roles(&[("title", TITLE)]);
    let mut first = object(TITLE, "20990720", Some(3))?;
    first.object_key = format!("bronze/source={TITLE}/OPN20990720FIRST.zip");
    let mut later = object(TITLE, "20990720", Some(9))?;
    later.object_key = format!("bronze/source={TITLE}/OPN20990720LATER.zip");
    let release = release::select(&title, &[first.clone(), later.clone()])?;
    assert_eq!(release.object("title")?, &later);
    // The order the ledger returns them in does not matter.
    let release = release::select(&title, &[later.clone(), first])?;
    assert_eq!(release.object("title")?, &later);
    Ok(())
}

#[test]
fn two_objects_with_the_same_or_no_update_date_are_refused() -> anyhow::Result<()> {
    let title = roles(&[("title", TITLE)]);
    for updated in [Some(9), None] {
        let mut first = object(TITLE, "20990720", updated)?;
        first.object_key = format!("bronze/source={TITLE}/OPN20990720FIRST.zip");
        let second = object(TITLE, "20990720", updated)?;
        let error = release::select(&title, &[first, second])
            .err()
            .context("a tie must not be broken by key order")?;
        assert!(
            error.to_string().contains("refusing to choose"),
            "{error:#}"
        );
    }
    Ok(())
}

#[test]
fn every_object_of_an_object_lane_is_a_release_in_publication_order() -> anyhow::Result<()> {
    // 월별 에너지 (root ADR-0396): each object is one usage month; a later ledger month may carry an
    // earlier usage month again, with no provider update date to tell them apart.
    const ENERGY: &str = "hubgokr__building_energy_monthly_electricity";
    let energy = roles(&[("source", ENERGY)]);
    let mut republished = object(ENERGY, "20990913", None)?;
    republished.object_key = format!("bronze/source={ENERGY}/OPN20990913A.zip");
    let newest = object(ENERGY, "20990913", None)?;
    let first = object(ENERGY, "20990807", None)?;
    let releases = release::select_each(
        &energy,
        &[newest.clone(), first.clone(), republished.clone()],
    )?;
    let keys: Vec<&str> = releases
        .iter()
        .map(|release| release.object("source").map(|o| o.object_key.as_str()))
        .collect::<anyhow::Result<_>>()?;
    assert_eq!(
        keys,
        [
            first.object_key.as_str(),
            republished.object_key.as_str(),
            newest.object_key.as_str()
        ]
    );
    assert_eq!(
        releases.iter().map(Release::vintage).collect::<Vec<_>>(),
        ["209908", "209909", "209909"]
    );
    // One source only: a release of two roles is a month's, not an object's.
    assert!(release::select_each(&unit_roles(), &[first]).is_err());
    Ok(())
}

#[test]
fn an_object_lane_compares_only_the_buckets_its_release_holds() -> anyhow::Result<()> {
    let contract = Lane::EnergyElectricity.contract()?;
    assert!(contract.releases_each_object && contract.compare_release_buckets_only);
    assert_eq!(contract.compare_bucket_prefix_length, 6);
    // Without the release scope every month the object lacks would read as removed.
    let unscoped = crate::building_energy_monthly_silver_export::ELECTRICITY_CONTRACT_JSON
        .replace(",\n      \"bucket_scope\": \"release\"", "");
    assert_ne!(
        unscoped,
        crate::building_energy_monthly_silver_export::ELECTRICITY_CONTRACT_JSON
    );
    assert!(LaneContract::parse(Lane::EnergyElectricity, &unscoped).is_err());
    assert!(!Lane::RegisterMaster.contract()?.releases_each_object);
    Ok(())
}

#[test]
fn a_release_whose_files_name_different_export_days_is_refused() -> anyhow::Result<()> {
    let ledger = [
        object(UNIT, "20990720", None)?,
        object(TITLE, "20990721", None)?,
        object(BASIS, "20990720", None)?,
    ];
    let error = release::select(&unit_roles(), &ledger)
        .err()
        .context("mixed export days must be refused")?;
    assert!(
        error
            .to_string()
            .contains("one provider month exported on one day"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn a_file_outside_its_ledger_month_or_source_is_refused() -> anyhow::Result<()> {
    let title = roles(&[("title", TITLE)]);
    let mut shifted = object(TITLE, "20990720", None)?;
    shifted.snapshot_date = month(2099, 6)?;
    assert!(release::select(&title, &[shifted]).is_err());
    let mut foreign = object(TITLE, "20990720", None)?;
    foreign.object_key = format!("bronze/source={UNIT}/OPN20990720SYNTHETIC.zip");
    assert!(release::select(&title, &[foreign]).is_err());
    let mut unhashed = object(TITLE, "20990720", None)?;
    unhashed.checksum_sha256 = "not-a-digest".to_owned();
    assert!(release::select(&title, &[unhashed]).is_err());
    Ok(())
}

#[test]
fn the_run_identity_names_the_lane_month_and_exact_bytes() -> anyhow::Result<()> {
    let title = roles(&[("title", TITLE)]);
    let release = release::select(&title, &[object(TITLE, "20990720", None)?])?;
    let identity = release.run_identity(Lane::Titles.id());
    assert!(identity.starts_with("silver-refresh-building-register-titles-209907-"));
    assert!(
        !identity.contains([',', '@']),
        "the ingest registry cannot hold {identity}"
    );
    assert_eq!(
        identity,
        release.run_identity(Lane::Titles.id()),
        "a re-run is the same load"
    );
    assert_eq!(
        release::run_identity_month(Lane::Titles.id(), &identity).as_deref(),
        Some("209907")
    );
    let mut recollected = object(TITLE, "20990720", None)?;
    recollected.checksum_sha256 = "b".repeat(64);
    let other = release::select(&title, &[recollected])?;
    assert_ne!(
        other.run_identity(Lane::Titles.id()),
        identity,
        "other bytes are another release"
    );
    assert_eq!(
        release::run_identity_month(Lane::Units.id(), &identity),
        None
    );
    Ok(())
}

// ---- lanes and their contracts ------------------------------------------------------------

#[test]
fn every_lane_parses_by_its_instance_name_and_its_contract_is_valid() -> anyhow::Result<()> {
    for lane in Lane::ALL {
        assert_eq!(Lane::parse(lane.id())?, lane);
        let contract = lane.contract()?;
        assert_eq!(contract.table, lane.table());
        assert!(lane
            .id()
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b == b'-'));
    }
    for unknown in [
        "",
        "building_register_titles",
        "building-register-floors",
        "Titles",
    ] {
        assert!(Lane::parse(unknown).is_err(), "accepted {unknown:?}");
    }
    Ok(())
}

/// The job list starts `foundation-silver-refresh@<lane>.service`; this enum runs `<lane>`. A lane
/// only one of them knows is a job the publisher refuses, or a lane nothing ever starts.
#[test]
fn the_lanes_are_the_silver_refresh_jobs_of_the_job_list() -> anyhow::Result<()> {
    let jobs: serde_json::Value =
        serde_json::from_str(include_str!("../../../../../../orchestration/jobs.v1.json"))?;
    let scheduled: BTreeSet<&str> = jobs["jobs"]
        .as_array()
        .context("jobs.v1.json lists jobs")?
        .iter()
        .filter_map(|job| job["systemd_service"].as_str())
        .filter_map(|service| service.strip_prefix("foundation-silver-refresh@"))
        .map(|instance| instance.strip_suffix(".service").unwrap_or(instance))
        .collect();
    let known: BTreeSet<&str> = Lane::ALL.into_iter().map(Lane::id).collect();
    assert_eq!(
        scheduled, known,
        "jobs.v1.json's silver refresh instances and Lane::ALL differ"
    );
    Ok(())
}

#[test]
fn the_lane_contracts_say_how_each_lane_runs() -> anyhow::Result<()> {
    let units = Lane::Units.contract()?;
    assert_eq!(units.kind, ExportKind::Unit);
    assert_eq!(units.roles, unit_roles());
    assert_eq!(units.spark.iceberg_write_mode, WriteMode::Overwrite);
    assert_eq!(units.spark.input_file_batch_size, 0);
    let price = Lane::ApartmentPrice.contract()?;
    assert!(!price.kind.loads_as_one_run());
    assert_eq!(price.spark.iceberg_write_mode, WriteMode::Append);
    assert_eq!(
        price.handoff_prefix.as_deref(),
        Some("silver-handoff/hubgokr__building_register_apartment_price")
    );
    // Its vintages are versions: staged on a branch, then only their changes appended (root ADR-0317).
    assert_eq!(
        price.table,
        "silver.building_register_apartment_price_versions"
    );
    let content = price
        .compare_content
        .context("the apartment-price lane appends its changes")?;
    for column in [
        "mgmt_key",
        "base_date",
        "vintage",
        "source_part_id",
        "source_line_number",
    ] {
        assert!(!content.iter().any(|c| c == column), "compares {column}");
    }
    assert!(content.iter().any(|c| c == "raw_columns"));
    Ok(())
}

fn planted(lane: Lane, edit: impl FnOnce(&mut serde_json::Value)) -> anyhow::Result<String> {
    let (_, text) = lane.contract_source();
    let mut value: serde_json::Value = serde_json::from_str(text)?;
    edit(&mut value);
    Ok(value.to_string())
}

#[test]
fn a_contract_that_would_misload_its_lane_is_refused() -> anyhow::Result<()> {
    let refused: Vec<(Lane, Box<dyn FnOnce(&mut serde_json::Value)>)> = vec![
        // another lane's table
        (
            Lane::Titles,
            Box::new(|v| v["silver_refresh"]["table"] = "silver.building_register_units".into()),
        ),
        // a role the export does not read
        (
            Lane::Titles,
            Box::new(|v| v["silver_refresh"]["roles"] = serde_json::json!({"unit": TITLE})),
        ),
        // a one-run load split into batches writes only its first batch
        (
            Lane::Titles,
            Box::new(|v| v["silver_refresh"]["spark"]["input_file_batch_size"] = 4.into()),
        ),
        // appending a second release beside the first breaks gold.building_panel
        (
            Lane::Titles,
            Box::new(|v| v["silver_refresh"]["spark"]["iceberg_write_mode"] = "append".into()),
        ),
        // a driver larger than any unit the cap knows is still a number of gigabytes
        (
            Lane::Titles,
            Box::new(|v| v["silver_refresh"]["spark"]["driver_memory"] = "lots".into()),
        ),
        // an export the refresh cannot run
        (
            Lane::Titles,
            Box::new(|v| {
                v["silver_refresh"]["export"] =
                    "export-building-register-floor-silver-handoff".into()
            }),
        ),
        // a hand-picked release back in the contract, in the block or beside it
        (
            Lane::Titles,
            Box::new(|v| v["silver_refresh"]["selected_vintage"] = "209907".into()),
        ),
        (
            Lane::Titles,
            Box::new(|v| v["selected_vintage"] = "209907".into()),
        ),
        (
            Lane::ApartmentPrice,
            Box::new(|v| v["objects"] = serde_json::json!([])),
        ),
        // a part lane overwriting its append-only table
        (
            Lane::ApartmentPrice,
            Box::new(|v| v["silver_refresh"]["spark"]["iceberg_write_mode"] = "overwrite".into()),
        ),
        // a part lane without its handoff prefix
        (
            Lane::ApartmentPrice,
            Box::new(|v| v["handoff_prefix"] = serde_json::Value::Null),
        ),
        // comparing on the key
        (
            Lane::ApartmentPrice,
            Box::new(|v| {
                v["silver_refresh"]["compare"] =
                    serde_json::json!({"content_columns": ["mgmt_key"]})
            }),
        ),
    ];
    for (lane, edit) in refused {
        let text = planted(lane, edit)?;
        assert!(LaneContract::parse(lane, &text).is_err(), "accepted {text}");
    }
    // The unedited contracts parse, so each refusal above is its planted break.
    for lane in [Lane::Titles, Lane::ApartmentPrice] {
        LaneContract::parse(lane, &planted(lane, |_| {})?)?;
    }
    Ok(())
}

#[test]
fn an_overwrite_nobody_declared_is_refused() -> anyhow::Result<()> {
    // silver.land_forest_ledger rewrote 39.9M rows a week for about 5K changes while its contract
    // said nothing about it (root ADR-0311): an overwriting lane names compare or a priced reason.
    let undeclared: Vec<(Lane, Box<dyn FnOnce(&mut serde_json::Value)>)> = vec![
        (
            Lane::LandForestLedger,
            Box::new(|v| {
                if let Some(block) = v["silver_refresh"].as_object_mut() {
                    block.remove("compare");
                }
            }),
        ),
        (
            Lane::Titles,
            Box::new(|v| {
                if let Some(block) = v["silver_refresh"].as_object_mut() {
                    block.remove("compare");
                }
            }),
        ),
        // a reason with no measured cost is not an exception anybody priced
        (
            Lane::Titles,
            Box::new(|v| {
                if let Some(block) = v["silver_refresh"].as_object_mut() {
                    block.remove("compare");
                    block.insert(
                        "full_reason".to_owned(),
                        serde_json::json!({"reason": "r", "measured_cost": " "}),
                    );
                }
            }),
        ),
        // a comparing part lane has no full write to excuse (root ADR-0318)
        (
            Lane::ExclusiveUnit,
            Box::new(|v| {
                v["silver_refresh"]["full_reason"] =
                    serde_json::json!({"reason": "r", "measured_cost": "c"});
            }),
        ),
        // both: a lane does one of them
        (
            Lane::LandForestLedger,
            Box::new(|v| {
                v["silver_refresh"]["full_reason"] =
                    serde_json::json!({"reason": "r", "measured_cost": "c"});
            }),
        ),
        // an appending lane has nothing to excuse
        (
            Lane::ApartmentPrice,
            Box::new(|v| {
                v["silver_refresh"]["full_reason"] =
                    serde_json::json!({"reason": "r", "measured_cost": "c"});
            }),
        ),
    ];
    for (lane, edit) in undeclared {
        let text = planted(lane, edit)?;
        assert!(LaneContract::parse(lane, &text).is_err(), "accepted {text}");
    }
    // A priced reason is still an exception a lane may declare instead of comparing.
    let priced = planted(Lane::Titles, |v| {
        if let Some(block) = v["silver_refresh"].as_object_mut() {
            block.remove("compare");
            block.insert(
                "full_reason".to_owned(),
                serde_json::json!({"reason": "r", "measured_cost": "c"}),
            );
        }
    })?;
    assert!(LaneContract::parse(Lane::Titles, &priced)?
        .compare_content
        .is_none());
    for lane in Lane::ALL {
        let (_, text) = lane.contract_source();
        let value: serde_json::Value = serde_json::from_str(text)?;
        let block = &value["silver_refresh"];
        let overwrites = block["spark"]["iceberg_write_mode"] == "overwrite";
        let declared = block.get("compare").is_some() != block.get("full_reason").is_some();
        // An appending lane may compare (root ADR-0318) but never names a full write.
        assert!(
            if overwrites {
                declared
            } else {
                block.get("full_reason").is_none()
            },
            "{lane:?}"
        );
    }
    Ok(())
}

// ---- unchanged or changed (root ADR-0169 §3–4) --------------------------------------------

fn title_release(day: &str) -> anyhow::Result<Release> {
    release::select(&roles(&[("title", TITLE)]), &[object(TITLE, day, None)?])
}

#[test]
fn a_release_the_table_already_records_is_unchanged() -> anyhow::Result<()> {
    let release = title_release("20990720")?;
    let identity = release.run_identity(Lane::Titles.id());
    let ingested: BTreeSet<String> =
        [identity.clone(), "remote-manual-load-20990101".to_owned()].into();
    assert_eq!(
        decide_run(Lane::Titles, &release, &ingested),
        Decision::Unchanged("already_loaded")
    );
    assert_eq!(
        outcome_line(Lane::Titles, &release.vintage(), &Decision::Unchanged("already_loaded"), &identity, None),
        format!("silver-refresh-outcome lane=building-register-titles outcome=unchanged reason=already_loaded release=209907 identity={identity} rows=0")
    );
    Ok(())
}

#[test]
fn a_newer_release_in_the_table_is_never_replaced_by_an_older_one() -> anyhow::Result<()> {
    let newer = title_release("20990820")?;
    let older = title_release("20990720")?;
    let ingested: BTreeSet<String> = [newer.run_identity(Lane::Titles.id())].into();
    assert_eq!(
        decide_run(Lane::Titles, &older, &ingested),
        Decision::Unchanged("newer_release_loaded")
    );
    // Another lane's identity is not this lane's release.
    let other: BTreeSet<String> = [newer.run_identity(Lane::Units.id())].into();
    assert_eq!(
        decide_run(Lane::Titles, &older, &other),
        Decision::ExportAndLoad
    );
    Ok(())
}

#[test]
fn a_release_the_table_lacks_is_exported_and_loaded() -> anyhow::Result<()> {
    let older = title_release("20990620")?;
    let release = title_release("20990720")?;
    let ingested: BTreeSet<String> = [older.run_identity(Lane::Titles.id())].into();
    let decision = decide_run(Lane::Titles, &release, &ingested);
    assert_eq!(decision, Decision::ExportAndLoad);
    let line = outcome_line(Lane::Titles, &release.vintage(), &decision, "x", Some(7));
    assert!(line.starts_with("silver-refresh-outcome "));
    assert!(line
        .contains(" outcome=changed reason=exported_and_loaded release=209907 identity=x rows=7"));
    // A plan's line is never read as a run's: another prefix, and no `changed`.
    let plan = as_plan(&outcome_line(
        Lane::Titles,
        &release.vintage(),
        &decision,
        "x",
        None,
    ));
    assert!(plan.starts_with("silver-refresh-plan ") && plan.contains(" outcome=would_change "));
    assert!(!plan.contains("silver-refresh-outcome") && !plan.contains("=changed "));
    Ok(())
}

const PRICE: &str = "hubgokr__building_register_apartment_price";
const PRICE_PREFIX: &str = "silver-handoff/hubgokr__building_register_apartment_price";

fn price_release(day: &str) -> anyhow::Result<Release> {
    release::select(&roles(&[("source", PRICE)]), &[object(PRICE, day, None)?])
}

fn hub_manifest(release: &Release, parts: &[u64]) -> anyhow::Result<HubManifest> {
    let stem = release
        .object("source")?
        .file_name()
        .trim_end_matches(".zip")
        .to_owned();
    let emitted = parts.iter().sum();
    Ok(HubManifest {
        schema_version: 1,
        status: "complete".to_owned(),
        input_object_key: release.object("source")?.object_key.clone(),
        output_object_prefix: PRICE_PREFIX.to_owned(),
        vintage: release.vintage(),
        rows_per_part: 10,
        rows_read: emitted + 1,
        rows_emitted: emitted,
        rejected_rows: 1,
        pnu_ok: emitted - 1,
        pnu_bad: 1,
        parts: parts
            .iter()
            .enumerate()
            .map(|(index, rows)| HubPart {
                object_key: format!(
                    "{PRICE_PREFIX}/{stem}/attempt=synthetic/part-{:04}.jsonl.gz",
                    index + 1
                ),
                rows: *rows,
                bytes: 123,
            })
            .collect(),
    })
}

#[test]
fn a_part_lane_is_unchanged_only_when_every_part_is_recorded() -> anyhow::Result<()> {
    let release = price_release("20990720")?;
    let manifest = hub_manifest(&release, &[10, 10, 3])?;
    validate_manifest(&manifest, PRICE_PREFIX, &release)?;
    assert_eq!(
        manifest_key(PRICE_PREFIX, &release)?,
        format!("{PRICE_PREFIX}/OPN20990720SYNTHETIC/manifest.json")
    );
    let all: BTreeSet<String> = manifest
        .parts
        .iter()
        .map(|p| p.object_key.clone())
        .collect();
    assert_eq!(
        decide_parts(PRICE_PREFIX, &release, Some(&manifest), &all),
        Decision::Unchanged("already_loaded")
    );
    let mut some = all.clone();
    some.pop_last();
    assert_eq!(
        decide_parts(PRICE_PREFIX, &release, Some(&manifest), &some),
        Decision::LoadOnly
    );
    assert_eq!(
        decide_parts(PRICE_PREFIX, &release, None, &BTreeSet::new()),
        Decision::ExportAndLoad
    );
    // Without a manifest for this month, a newer month's parts in the table hold it back.
    let newer = hub_manifest(&price_release("20990820")?, &[5])?;
    let held: BTreeSet<String> = newer.parts.iter().map(|p| p.object_key.clone()).collect();
    assert_eq!(
        decide_parts(PRICE_PREFIX, &release, None, &held),
        Decision::Unchanged("newer_release_loaded")
    );
    assert_eq!(
        part_batches(&manifest, 2)
            .iter()
            .map(|b| b.len())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    Ok(())
}

#[test]
fn a_manifest_of_another_release_or_with_broken_parts_is_refused() -> anyhow::Result<()> {
    let release = price_release("20990720")?;
    let good = hub_manifest(&release, &[10, 3])?;
    let breaks: Vec<Box<dyn Fn(&mut HubManifest)>> = vec![
        Box::new(|m| m.status = "partial".to_owned()),
        Box::new(|m| m.vintage = "209906".to_owned()),
        Box::new(|m| m.output_object_prefix = "silver-handoff/other".to_owned()),
        Box::new(|m| m.rows_emitted += 1),
        Box::new(|m| m.parts.swap(0, 1)),
        Box::new(|m| {
            m.parts[1].object_key = m.parts[1]
                .object_key
                .replace("attempt=synthetic", "attempt=other")
        }),
        Box::new(|m| m.parts[0].rows = 11),
        Box::new(|m| m.pnu_bad += 1),
        Box::new(|m| m.parts[0].bytes = 0),
        // a part short of rows_per_part before the last: a row went missing in the middle
        Box::new(|m| {
            m.parts[0].rows = 9;
            m.parts[1].rows = 4;
        }),
    ];
    for planted in breaks {
        let mut broken = good.clone();
        planted(&mut broken);
        assert!(validate_manifest(&broken, PRICE_PREFIX, &release).is_err());
    }
    Ok(())
}

// ---- what a changed run starts -------------------------------------------------------------

fn runtime() -> Runtime {
    Runtime {
        release_root: PathBuf::from("/opt/foundation-platform/releases/synthetic"),
        lane_root: PathBuf::from(
            "/data/foundation-platform/silver-refresh/building-register-units",
        ),
        spark_jars: "/home/spark/.ivy2/synthetic.jar".to_owned(),
        jars_dir: PathBuf::from("/opt/foundation-platform/artifacts/synthetic/jars"),
        project: format!("foundation-silver-refresh-{}", "0".repeat(32)),
        gid: "999".to_owned(),
        publisher: PathBuf::from(
            "/opt/foundation-platform/artifacts/synthetic/foundation-outbox-publisher",
        ),
    }
}

fn env_map(env: Vec<(String, String)>) -> BTreeMap<String, String> {
    env.into_iter().collect()
}

#[test]
fn the_unit_export_reads_exactly_the_release_the_ledger_picked() -> anyhow::Result<()> {
    let contract = Lane::Units.contract()?;
    let release = release::select(
        &unit_roles(),
        &[
            object(UNIT, "20990720", None)?,
            object(TITLE, "20990720", None)?,
            object(BASIS, "20990720", None)?,
        ],
    )?;
    let work = Path::new("/data/work");
    let env = env_map(execute::export_environment(
        &contract, &release, "id-1", work,
    )?);
    let key =
        |name: &str| format!("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_{name}");
    assert_eq!(env[&key("SOURCE_SLUG")], UNIT);
    assert_eq!(env[&key("SOURCE_OBJECT")], "OPN20990720SYNTHETIC.zip");
    assert_eq!(env[&key("TITLE_SOURCE_SLUG")], TITLE);
    assert_eq!(env[&key("BASIS_SOURCE_SLUG")], BASIS);
    assert_eq!(env[&key("SOURCE_SNAPSHOT_ID")], "id-1");
    assert_eq!(env[&key("VALID_FROM_UTC")], "2099-07-01T00:00:00Z");
    assert_eq!(env[&key("OUTPUT_FORMAT")], "parquet");
    assert_eq!(env[&key("CHUNK_ROWS")], "250000");
    assert_eq!(env[&key("APPLY_APPROVED_OVERRIDES")], "1");
    assert_eq!(Path::new(&env[&key("BRONZE_ROOT")]), work);
    Ok(())
}

/// Root ADR-0391 addendum: the unit areas load part by part, the way ADR-0357's part lanes do, so
/// no step of theirs is the 7,246s one-run Spark step of 2026-10-20.
#[test]
fn the_unit_areas_load_part_by_part_and_each_step_fits_the_unit() -> anyhow::Result<()> {
    let contract = Lane::UnitAreas.contract()?;
    assert_eq!(contract.kind, ExportKind::UnitArea);
    assert!(!contract.kind.loads_as_one_run());
    assert_eq!(contract.spark.iceberg_write_mode, WriteMode::Append);
    assert_eq!(
        contract.handoff_prefix.as_deref(),
        Some("silver-handoff/hubgokr__building_register_exclusive_common_area")
    );
    let part_rows = u64::from(contract.export_chunk_rows.context("part size")?);
    let batch_rows = part_rows * u64::from(contract.spark.input_file_batch_size);
    // A load batch is no bigger than the measured 2026-10-20 load cut to it: 114,461,830 rows took
    // 6,843s of Spark besides its 386s null-share check (13:24:29 to 15:24:58), so a batch takes that share; it must fit the budget.
    let batch_seconds = 6_843 * batch_rows / 114_461_830 + 386 + 60;
    let step = u64::from(
        contract
            .step_seconds_max
            .context("the unit areas measure a run budget")?,
    );
    assert!(
        batch_seconds <= step,
        "a load batch of {batch_rows} rows is about {batch_seconds}s, more than the budget {step}s"
    );
    let fit = UNIT_TIME_LIMIT_MINUTES * 60 * FIT_SHARE_PERCENT / 100;
    assert!(last_step_start_seconds(contract.step_seconds_max) + i64::try_from(step)? <= fit);

    let release = release::select(
        &roles(&[(
            "unit_area",
            "hubgokr__building_register_exclusive_common_area",
        )]),
        &[object(
            "hubgokr__building_register_exclusive_common_area",
            "20990720",
            None,
        )?],
    )?;
    let env = env_map(execute::export_environment(
        &contract,
        &release,
        "id-3",
        Path::new("/w"),
    )?);
    let key = |name: &str| {
        format!("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_{name}")
    };
    assert_eq!(env[&key("OUTPUT_FORMAT")], "r2-parts");
    assert_eq!(
        env[&key("OUTPUT_OBJECT_PREFIX")],
        "silver-handoff/hubgokr__building_register_exclusive_common_area"
    );
    assert_eq!(env[&key("CHUNK_ROWS")], "1000000");
    // 2026-11-07 07:00: the part path named its object by the role `source`, which this lane
    // calls `unit_area`, and every run stopped at "the release has no source object".
    assert_eq!(
        manifest_key(contract.handoff_prefix.as_deref().context("prefix")?, &release)?,
        "silver-handoff/hubgokr__building_register_exclusive_common_area/OPN20990720SYNTHETIC/manifest.json"
    );
    Ok(())
}

#[test]
fn a_unit_area_contract_that_loads_the_release_whole_is_refused() -> anyhow::Result<()> {
    // Synthetic: the one-run shape of 2026-10 (overwrite in one input batch) and a part lane with
    // no part size; either would bring back a step the unit cannot hold.
    let refused: Vec<Box<dyn FnOnce(&mut serde_json::Value)>> = vec![
        Box::new(|v| {
            v["silver_refresh"]["spark"]["iceberg_write_mode"] = "overwrite".into();
            v["silver_refresh"]["spark"]["input_file_batch_size"] = 0.into();
        }),
        Box::new(|v| {
            v["silver_refresh"]
                .as_object_mut()
                .map(|block| block.remove("export_chunk_rows"));
        }),
        Box::new(|v| {
            v.as_object_mut().map(|doc| doc.remove("handoff_prefix"));
        }),
    ];
    for edit in refused {
        let text = planted(Lane::UnitAreas, edit)?;
        assert!(
            LaneContract::parse(Lane::UnitAreas, &text).is_err(),
            "accepted {text}"
        );
    }
    Ok(())
}

#[test]
fn the_hub_export_gets_the_ledger_size_and_the_contract_prefix() -> anyhow::Result<()> {
    let contract = Lane::ApartmentPrice.contract()?;
    let release = price_release("20990720")?;
    let env = env_map(execute::export_environment(
        &contract,
        &release,
        "id-2",
        Path::new("/w"),
    )?);
    assert_eq!(
        env["FOUNDATION_PLATFORM_APARTMENT_PRICE_INPUT_OBJECT_KEY"],
        format!("bronze/source={PRICE}/OPN20990720SYNTHETIC.zip")
    );
    assert_eq!(
        env["FOUNDATION_PLATFORM_APARTMENT_PRICE_INPUT_OBJECT_BYTES"],
        "1024"
    );
    assert_eq!(
        env["FOUNDATION_PLATFORM_APARTMENT_PRICE_OUTPUT_OBJECT_PREFIX"],
        PRICE_PREFIX
    );
    Ok(())
}

#[test]
fn spark_gets_secrets_by_name_and_the_lane_contracts_runner_values() -> anyhow::Result<()> {
    let runtime = runtime();
    let units = Lane::Units.contract()?;
    let args = execute::spark_arguments(&runtime, &units, &SparkLoad::local(&units));
    let joined = args.join(" ");
    assert!(joined.contains(&format!("-p {}", runtime.project)));
    // A one-off container: never the write check as a dependency under a name another job's run
    // also creates (2026-10-29).
    assert!(joined.contains("--profile lakehouse-batch run --rm --no-deps -v "));
    assert!(joined.contains("--master local[8,8] --driver-memory 16g"));
    assert!(joined.contains("--input /workspace/target/lakehouse/handoff --input-format parquet"));
    assert!(joined.contains("--contract silver.building_register_units"));
    assert!(joined.contains("--iceberg-write-mode overwrite"));
    assert!(args.iter().any(|a| a == "--allow-non-smoke-overwrite"));
    assert!(!joined.contains("READER"), "a local load reads no R2");
    let price = Lane::ApartmentPrice.contract()?;
    let load = SparkLoad {
        input: "s3a://bucket/a,s3a://bucket/b".to_owned(),
        input_format: "jsonl",
        expected_count: Some(13),
        reads_r2: true,
        write_mode: WriteMode::Append,
        branch: None,
    };
    let args = execute::spark_arguments(&runtime, &price, &load);
    assert!(args.windows(2).any(|w| w
        == [
            "-e",
            "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY"
        ]));
    assert!(
        !args.iter().any(|a| a.contains('=') && a.contains("SECRET")),
        "a value reached the arguments"
    );
    assert!(args.windows(2).any(|w| w == ["--expected-count", "13"]));
    assert!(!args.iter().any(|a| a == "--allow-non-smoke-overwrite"));
    assert!(
        !args.iter().any(|a| a == "--iceberg-branch"),
        "a one-commit load writes main"
    );
    let branch = execute::load_branch("synthetic-load");
    assert_eq!(
        branch,
        execute::load_branch("synthetic-load"),
        "a retry stages on the same branch"
    );
    assert_ne!(branch, execute::load_branch("another-load"));
    assert!(
        branch.starts_with("load_")
            && branch.len() == 21
            && branch[5..].bytes().all(|b| b.is_ascii_hexdigit())
    );
    Ok(())
}

/// Root ADR-0391: a lane that measured its slowest step starts its last step early enough that the
/// step ends inside the fit of its unit, from the measured number (rt-molit's `FitsItsUnit`, #988).
#[test]
fn a_measured_lane_starts_its_last_step_early_enough_to_end_inside_its_unit() -> anyhow::Result<()>
{
    let fit = UNIT_TIME_LIMIT_MINUTES * 60 * FIT_SHARE_PERCENT / 100;
    let mut measured = Vec::new();
    for lane in Lane::ALL {
        let contract = lane.contract()?;
        let Some(step) = contract.step_seconds_max else {
            continue;
        };
        let start = last_step_start_seconds(Some(step));
        assert!(
            start >= MIN_STEP_START_SECONDS && start + i64::from(step) <= fit,
            "{}: a step of {step}s started at {start}s does not end inside {fit}s",
            lane.id()
        );
        measured.push(lane);
    }
    // The lanes whose measured runs did not fit their unit (2026-10-20, 10-21, 11-03).
    for lane in [
        Lane::ApartmentPrice,
        Lane::LandTransferHistory,
        Lane::UnitAreas,
    ] {
        assert!(
            measured.contains(&lane),
            "{} measures no run_budget",
            lane.id()
        );
    }
    // Without a measured step, the default the other lanes keep.
    assert_eq!(
        last_step_start_seconds(None),
        LAST_BUCKET_START_MINUTES * 60
    );
    Ok(())
}

#[test]
fn a_step_too_long_for_its_unit_or_an_unmeasured_budget_is_refused() -> anyhow::Result<()> {
    // Synthetic: the 전유공용면적 one-run load is one Spark step of 7,246s (2026-10-20), which leaves
    // a run 854s to start it; the guard refuses it rather than let the unit kill the run.
    for (step, measured) in [(7_246, "2026-10-20 journal"), (2_805, " "), (0, "x")] {
        let text = planted(Lane::LandTransferHistory, |v| {
            v["silver_refresh"]["run_budget"] =
                serde_json::json!({"step_seconds_max": step, "measured": measured});
        })?;
        assert!(
            LaneContract::parse(Lane::LandTransferHistory, &text).is_err(),
            "accepted {step}s measured {measured:?}"
        );
    }
    Ok(())
}
