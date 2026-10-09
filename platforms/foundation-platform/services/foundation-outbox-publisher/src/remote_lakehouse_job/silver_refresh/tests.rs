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
    assert_eq!(release.month, month(2099, 7)?);
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
    assert_eq!(release.month, month(2099, 7)?);
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
        error.to_string().contains("different provider export days"),
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
    Ok(())
}

fn planted(lane: Lane, edit: impl FnOnce(&mut serde_json::Value)) -> anyhow::Result<String> {
    let text = match lane {
        Lane::Titles => include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-title-source-objects.json"),
        Lane::ApartmentPrice => include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-apartment-price-source-objects.json"),
        _ => anyhow::bail!("not planted"),
    };
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
        outcome_line(Lane::Titles, &release, &Decision::Unchanged("already_loaded"), &identity, None),
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
    let line = outcome_line(Lane::Titles, &release, &decision, "x", Some(7));
    assert!(line.starts_with("silver-refresh-outcome "));
    assert!(line
        .contains(" outcome=changed reason=exported_and_loaded release=209907 identity=x rows=7"));
    // A plan's line is never read as a run's: another prefix, and no `changed`.
    let plan = as_plan(&outcome_line(Lane::Titles, &release, &decision, "x", None));
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
    assert!(joined.contains("--master local[8] --driver-memory 16g"));
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
    };
    let args = execute::spark_arguments(&runtime, &price, &load);
    assert!(args.windows(2).any(|w| w
        == [
            "-e",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY"
        ]));
    assert!(
        !args.iter().any(|a| a.contains('=') && a.contains("SECRET")),
        "a value reached the arguments"
    );
    assert!(args.windows(2).any(|w| w == ["--expected-count", "13"]));
    assert!(!args.iter().any(|a| a == "--allow-non-smoke-overwrite"));
    Ok(())
}
