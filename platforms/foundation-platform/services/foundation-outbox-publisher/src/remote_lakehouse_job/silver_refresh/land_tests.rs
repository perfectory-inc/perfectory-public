//! The VWorld land lanes (root ADR-0169 step 3): release selection from the ledger's ZIP members,
//! the table's own record, the contracts and what a changed run starts. Fixtures are synthetic:
//! region codes 97–99, object keys and dates that name no real file.
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::NaiveDate;

use super::execute::{self, Runtime, SparkLoad};
use super::land::{
    self, Candidate, Completeness, LandRelease, LandRule, MeasuredObject, Reading, MEASURE_COMMAND,
};
use super::lane::{ExportKind, Lane, LaneContract, WriteMode};
use super::release::LedgerObject;
use super::*;

const PRICE: &str = "vworldkr__land_individual_price";
const ZONE: &str = "vworldkr__land_use_zone_code";

const LAND_LANES: [Lane; 7] = [
    Lane::LandCharacteristic,
    Lane::LandForestLedger,
    Lane::LandIndividualPrice,
    Lane::LandRightRegistration,
    Lane::LandTransferHistory,
    Lane::LandUsePlan,
    Lane::LandUseZoneCode,
];

fn rule(count: usize) -> LandRule {
    LandRule {
        source: PRICE.to_owned(),
        member_name: r"^AL_D151_(?P<region>[0-9]{2})_(?P<vintage>[0-9]{8})\.csv$".to_owned(),
        completeness: Completeness::Sido { count },
        snapshot_label: "vworldkr-land-individual-price".to_owned(),
        handoff_prefix: format!("silver-handoff/{PRICE}"),
        handoff_suffix: ".jsonl.gz".to_owned(),
    }
}

fn zone_rule() -> LandRule {
    LandRule {
        source: ZONE.to_owned(),
        member_name: r"^LART_LMISZONE\.csv$".to_owned(),
        completeness: Completeness::National,
        snapshot_label: "vworldkr-land-use-zone-code".to_owned(),
        handoff_prefix: format!("silver-handoff/{ZONE}"),
        handoff_suffix: ".jsonl.gz".to_owned(),
    }
}

fn ledger(slug: &str, name: &str, day: u32, updated: Option<u32>) -> anyhow::Result<LedgerObject> {
    Ok(LedgerObject {
        slug: slug.to_owned(),
        object_key: format!("bronze/source={slug}/{name}.zip"),
        snapshot_date: NaiveDate::from_ymd_opt(2099, 12, day).context("valid day")?,
        provider_updated_at: updated.and_then(|d| NaiveDate::from_ymd_opt(2099, 12, d)),
        size_bytes: 2048,
        checksum_sha256: "c".repeat(64),
    })
}

/// A measured price ZIP holding one CSV for `region` of `vintage`, beside a DBF sibling member.
fn price(
    name: &str,
    region: &str,
    vintage: &str,
    updated: Option<u32>,
) -> anyhow::Result<MeasuredObject> {
    Ok(MeasuredObject {
        object: ledger(PRICE, name, 1, updated)?,
        reading: Reading::Zip(vec![
            format!("AL_D151_{region}_{vintage}.csv"),
            "README.txt".to_owned(),
        ]),
    })
}

fn converts_price(name: &str) -> bool {
    name.starts_with("AL_D151_") && name.ends_with(".csv")
}

fn select(rule: &LandRule, measured: &[MeasuredObject]) -> anyhow::Result<LandRelease> {
    let candidates = land::candidates(rule, measured, converts_price)?;
    land::select(rule, &candidates.found)
}

fn regions(release: &LandRelease) -> Vec<&str> {
    release
        .objects
        .iter()
        .map(|(region, _)| region.as_str())
        .collect()
}

// ---- release selection ---------------------------------------------------------------------

#[test]
fn the_newest_vintage_covering_every_region_is_the_release() -> anyhow::Result<()> {
    let measured = [
        price("SYNTHETIC-01", "97", "20990101", None)?,
        price("SYNTHETIC-02", "98", "20990101", None)?,
        price("SYNTHETIC-03", "97", "20990201", None)?,
        price("SYNTHETIC-04", "98", "20990201", None)?,
        // another dataset in the same prefix: no member the rule names
        MeasuredObject {
            object: ledger(PRICE, "SYNTHETIC-05", 1, None)?,
            reading: Reading::Zip(vec!["AL_D150_97_20990201.dbf".to_owned()]),
        },
        // settled readings that are not ZIPs hold nothing to load
        MeasuredObject {
            object: ledger(PRICE, "SYNTHETIC-06", 1, None)?,
            reading: Reading::NotLoadable,
        },
    ];
    let release = select(&rule(2), &measured)?;
    assert_eq!(release.vintage, "20990201");
    assert_eq!(regions(&release), ["97", "98"]);
    assert!(release.skipped_incomplete.is_empty());
    assert_eq!(
        release.object_keys(),
        [
            format!("bronze/source={PRICE}/SYNTHETIC-03.zip"),
            format!("bronze/source={PRICE}/SYNTHETIC-04.zip")
        ]
    );
    assert_eq!(
        release.source_snapshot_id(&rule(2))?,
        "vworldkr-land-individual-price:20990201"
    );
    Ok(())
}

#[test]
fn a_newer_vintage_missing_a_region_is_skipped_not_loaded() -> anyhow::Result<()> {
    let measured = [
        price("SYNTHETIC-01", "97", "20990101", None)?,
        price("SYNTHETIC-02", "98", "20990101", None)?,
        // the newest harvest has only one of the two regions so far
        price("SYNTHETIC-03", "97", "20990301", None)?,
    ];
    let release = select(&rule(2), &measured)?;
    assert_eq!(release.vintage, "20990101");
    assert_eq!(release.skipped_incomplete, ["20990301 regions=1"]);
    // A vintage with more regions than the contract's is not a release either.
    let error = select(&rule(1), &measured[..2])
        .err()
        .context("two regions are not the one the rule requires")?;
    assert!(
        error.to_string().contains("20990101 regions=2"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn an_object_not_yet_measured_refuses_the_run_and_names_the_command() -> anyhow::Result<()> {
    let measured = [
        price("SYNTHETIC-01", "97", "20990101", None)?,
        MeasuredObject {
            object: ledger(PRICE, "SYNTHETIC-NEW", 1, None)?,
            reading: Reading::Unmeasured,
        },
    ];
    let error = land::candidates(&rule(1), &measured, converts_price)
        .err()
        .context("an unmeasured object may hold the newest vintage")?;
    let message = error.to_string();
    assert!(message.contains(MEASURE_COMMAND), "{message}");
    assert!(message.contains("SYNTHETIC-NEW"), "{message}");
    assert!(message.contains(PRICE), "{message}");
    Ok(())
}

#[test]
fn two_objects_of_a_region_resolve_to_the_later_provider_update() -> anyhow::Result<()> {
    let earlier = price("SYNTHETIC-01", "97", "20990101", Some(3))?;
    let later = price("SYNTHETIC-02", "97", "20990101", Some(9))?;
    for measured in [
        [earlier.clone(), later.clone()],
        [later.clone(), earlier.clone()],
    ] {
        let release = select(&rule(1), &measured)?;
        assert_eq!(release.objects[0].1, later.object);
    }
    Ok(())
}

#[test]
fn two_objects_of_a_region_with_the_same_or_no_update_date_are_refused() -> anyhow::Result<()> {
    for updated in [Some(9), None] {
        let measured = [
            price("SYNTHETIC-01", "97", "20990101", updated)?,
            price("SYNTHETIC-02", "97", "20990101", updated)?,
        ];
        let error = select(&rule(1), &measured)
            .err()
            .context("a tie must not be broken by key order")?;
        assert!(
            format!("{error:#}").contains("refusing to choose"),
            "{error:#}"
        );
    }
    // A tie in an incomplete newer vintage does not matter: that vintage is not chosen.
    let measured = [
        price("SYNTHETIC-01", "97", "20990101", None)?,
        price("SYNTHETIC-02", "98", "20990101", None)?,
        price("SYNTHETIC-03", "97", "20990201", None)?,
        price("SYNTHETIC-04", "97", "20990201", None)?,
    ];
    assert_eq!(select(&rule(2), &measured)?.vintage, "20990101");
    Ok(())
}

#[test]
fn an_object_the_export_would_refuse_is_left_out_and_named() -> anyhow::Result<()> {
    let mut double = price("SYNTHETIC-02", "98", "20990101", None)?;
    double.reading = Reading::Zip(vec![
        "AL_D151_98_20990101.csv".to_owned(),
        "AL_D151_98_20990101_copy.csv".to_owned(),
    ]);
    let undated = price("SYNTHETIC-03", "97", "20991399", None)?;
    let measured = [
        price("SYNTHETIC-01", "97", "20990101", None)?,
        double,
        undated,
    ];
    let candidates = land::candidates(&rule(2), &measured, converts_price)?;
    assert_eq!(candidates.found.len(), 1);
    assert_eq!(candidates.refused.len(), 2, "{:?}", candidates.refused);
    assert!(candidates.refused[0].contains("SYNTHETIC-02"));
    assert!(candidates.refused[1].contains("no date"));
    // Without it 98 is uncovered, so there is no release rather than a guessed one.
    assert!(land::select(&rule(2), &candidates.found).is_err());
    Ok(())
}

#[test]
fn a_national_lane_loads_its_one_newest_object() -> anyhow::Result<()> {
    let code_table = |name: &str, day: u32| -> anyhow::Result<MeasuredObject> {
        Ok(MeasuredObject {
            object: ledger(ZONE, name, day, None)?,
            reading: Reading::Zip(vec!["LART_LMISZONE.csv".to_owned()]),
        })
    };
    let measured = [
        code_table("SYNTHETIC-1", 1)?,
        code_table("SYNTHETIC-3", 3)?,
        // the spreadsheet sibling is a ZIP too, of other members
        MeasuredObject {
            object: ledger(ZONE, "SYNTHETIC-2", 5, None)?,
            reading: Reading::Zip(vec!["[Content_Types].xml".to_owned()]),
        },
    ];
    let candidates = land::candidates(&zone_rule(), &measured, |n| n == "LART_LMISZONE.csv")?;
    let release = land::select(&zone_rule(), &candidates.found)?;
    assert_eq!(release.vintage, "20991203");
    assert_eq!(regions(&release), ["national"]);
    assert_eq!(
        release.source_snapshot_id(&zone_rule())?,
        "vworldkr-land-use-zone-code:SYNTHETIC-3"
    );
    // Two objects of one ledger day are a tie unless the provider dates them apart.
    let tied = [code_table("SYNTHETIC-1", 3)?, code_table("SYNTHETIC-3", 3)?];
    let candidates = land::candidates(&zone_rule(), &tied, |n| n == "LART_LMISZONE.csv")?;
    assert!(land::select(&zone_rule(), &candidates.found).is_err());
    Ok(())
}

// ---- unchanged or changed ------------------------------------------------------------------

fn candidates_of(measured: &[MeasuredObject]) -> anyhow::Result<Vec<Candidate>> {
    Ok(land::candidates(&rule(2), measured, converts_price)?.found)
}

#[test]
fn a_release_the_table_records_or_a_newer_one_is_unchanged() -> anyhow::Result<()> {
    let measured = [
        price("SYNTHETIC-01", "97", "20990101", None)?,
        price("SYNTHETIC-02", "98", "20990101", None)?,
        price("SYNTHETIC-03", "97", "20990201", None)?,
        price("SYNTHETIC-04", "98", "20990201", None)?,
    ];
    let all = candidates_of(&measured)?;
    let newest = land::select(&rule(2), &all)?;
    let key = |name: &str| format!("bronze/source={PRICE}/{name}.zip");
    let ingested: BTreeSet<String> = [
        key("SYNTHETIC-03"),
        key("SYNTHETIC-04"),
        key("SYNTHETIC-01"),
    ]
    .into();
    assert_eq!(
        land::decide(&newest, &all, &ingested),
        Decision::Unchanged("already_loaded")
    );
    // Half of the release recorded is a run that stopped: it is loaded again (and resumes).
    let half: BTreeSet<String> = [key("SYNTHETIC-03")].into();
    assert_eq!(land::decide(&newest, &all, &half), Decision::ExportAndLoad);
    // An older release in the table is replaced.
    let older: BTreeSet<String> = [key("SYNTHETIC-01"), key("SYNTHETIC-02")].into();
    assert_eq!(land::decide(&newest, &all, &older), Decision::ExportAndLoad);
    // A newer release in the table is never replaced by an older one.
    let previous = land::select(&rule(2), &candidates_of(&measured[..2])?)?;
    assert_eq!(
        land::decide(&previous, &all, &half),
        Decision::Unchanged("newer_release_loaded")
    );
    let line = outcome_line(
        Lane::LandIndividualPrice,
        &newest.vintage,
        &Decision::Unchanged("already_loaded"),
        &newest.source_snapshot_id(&rule(2))?,
        None,
    );
    assert_eq!(
        line,
        "silver-refresh-outcome lane=land-individual-price outcome=unchanged reason=already_loaded release=20990201 identity=vworldkr-land-individual-price:20990201 rows=0"
    );
    Ok(())
}

// ---- the contracts -------------------------------------------------------------------------

#[test]
fn every_land_contract_is_a_land_lane_of_its_export() -> anyhow::Result<()> {
    for lane in LAND_LANES {
        let contract = lane.contract()?;
        let rule = contract
            .land
            .as_ref()
            .context("a land lane has a land rule")?;
        let export = crate::land_use_silver_export::export_command(&contract.export)
            .context("a land export")?;
        assert_eq!(export.table, contract.table);
        assert!(
            matches!(contract.kind, ExportKind::Land { env_prefix } if env_prefix == export.env_prefix)
        );
        assert!(contract.roles.is_empty());
        assert_eq!(contract.spark.iceberg_write_mode, WriteMode::Overwrite);
        assert!(contract.spark.input_file_batch_size > 0);
        assert_eq!(
            rule.handoff_prefix,
            format!("silver-handoff/{}", rule.source),
            "{}",
            lane.id()
        );
        assert_eq!(
            rule.snapshot_label,
            rule.source.replace("__", "-").replace('_', "-")
        );
        // The export is a publisher command the release can start.
        assert!(crate::parse_command(["publisher", contract.export.as_str()]).is_ok());
    }
    Ok(())
}

/// For each lane, a synthetic member it loads and siblings in its prefix it must not.
fn members(lane: Lane) -> (&'static str, &'static [&'static str]) {
    match lane {
        Lane::LandCharacteristic => ("AL_D195_97_20991231.csv", &["AL_D194_97110_20991231.shp"]),
        Lane::LandForestLedger => ("AL_D003_97_20991231.csv", &["CH_D003_97_20991231.csv"]),
        Lane::LandIndividualPrice => ("AL_D151_97_20991231.csv", &["AL_D150_97_20991231.dbf"]),
        Lane::LandRightRegistration => ("AL_D006_97_20991231.csv", &["CH_D006_97_20991231.csv"]),
        Lane::LandTransferHistory => ("AL_D157_97_20991231.csv", &["CH_D157_97_20991231.csv"]),
        Lane::LandUsePlan => ("AL_D155_97_20991231.csv", &["AL_D154_97_20991231.shp"]),
        _ => ("LART_LMISZONE.csv", &["[Content_Types].xml"]),
    }
}

#[test]
fn every_land_rule_reads_what_its_export_converts_and_nothing_beside_it() -> anyhow::Result<()> {
    for lane in LAND_LANES {
        let contract = lane.contract()?;
        let rule = contract.land.as_ref().context("a land rule")?;
        let regex = rule.member_regex()?;
        let export = crate::land_use_silver_export::export_command(&contract.export)
            .context("a land export")?;
        let (loaded, siblings) = members(lane);
        assert!(
            regex.is_match(loaded) && export.converts_member(loaded),
            "{}",
            lane.id()
        );
        for sibling in siblings {
            assert!(
                !regex.is_match(sibling) && !export.converts_member(sibling),
                "{sibling}"
            );
        }
        if let Completeness::Sido { count } = rule.completeness {
            assert_eq!(count, 17, "{}: the 시도 a national load covers", lane.id());
            let captures = regex.captures(loaded).context("captures")?;
            assert_eq!(&captures["region"], "97");
            assert_eq!(&captures["vintage"], "20991231");
        } else {
            assert_eq!(lane, Lane::LandUseZoneCode);
        }
    }
    Ok(())
}

fn planted(lane: Lane, edit: impl FnOnce(&mut serde_json::Value)) -> anyhow::Result<String> {
    let (_, text) = lane.contract_source();
    let mut value: serde_json::Value = serde_json::from_str(text)?;
    edit(&mut value);
    Ok(value.to_string())
}

#[test]
fn a_land_contract_that_names_a_release_or_would_misload_is_refused() -> anyhow::Result<()> {
    type Edit = Box<dyn FnOnce(&mut serde_json::Value)>;
    let refused: Vec<(Lane, Edit)> = vec![
        // hand-picked releases back in the contract
        (
            Lane::LandUsePlan,
            Box::new(|v| v["selected_vintage"] = "20991231".into()),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["objects"] = serde_json::json!([])),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["granularity_counts"] = serde_json::json!({"sido": 17})),
        ),
        (
            Lane::LandUseZoneCode,
            Box::new(|v| v["objects"] = serde_json::json!([])),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["selected_vintage"] = "20991231".into()),
        ),
        // another table than the export writes
        (
            Lane::LandUsePlan,
            Box::new(|v| {
                v["silver_refresh"]["export"] = "export-land-individual-price-silver-handoff".into()
            }),
        ),
        // appending a second release beside the first breaks gold.parcel_panel
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["spark"]["iceberg_write_mode"] = "append".into()),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["spark"]["input_file_batch_size"] = 0.into()),
        ),
        // a rule that cannot say the region or the vintage, or is not anchored
        (
            Lane::LandUsePlan,
            Box::new(|v| {
                v["silver_refresh"]["member_name"] = r"^AL_D155_[0-9]{2}_[0-9]{8}\.csv$".into()
            }),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| {
                v["silver_refresh"]["member_name"] =
                    r"AL_D155_(?P<region>[0-9]{2})_(?P<vintage>[0-9]{8})".into()
            }),
        ),
        (
            Lane::LandUseZoneCode,
            Box::new(|v| {
                v["silver_refresh"]["member_name"] = r"^(?P<region>LART)_LMISZONE\.csv$".into()
            }),
        ),
        // completeness that is not 시도 N or national 1
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["completeness"]["count"] = 0.into()),
        ),
        (
            Lane::LandUseZoneCode,
            Box::new(|v| v["silver_refresh"]["completeness"]["count"] = 2.into()),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| {
                v["silver_refresh"]["completeness"]["load_granularity"] = "sigungu".into()
            }),
        ),
        // hub fields, or another provider's source
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["roles"] = serde_json::json!({"source": PRICE})),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["source"] = "hubgokr__building_register_main".into()),
        ),
        // a handoff the export would not compress or the loader not find
        (
            Lane::LandUsePlan,
            Box::new(|v| v["handoff_suffix"] = ".jsonl".into()),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["handoff_prefix"] = "bronze/elsewhere".into()),
        ),
    ];
    for (lane, edit) in refused {
        let text = planted(lane, edit)?;
        assert!(LaneContract::parse(lane, &text).is_err(), "accepted {text}");
    }
    // The unedited contracts parse, so each refusal above is its planted break.
    for lane in [Lane::LandUsePlan, Lane::LandUseZoneCode] {
        LaneContract::parse(lane, &planted(lane, |_| {})?)?;
    }
    // A hub lane may not carry land fields.
    let text = planted(Lane::Titles, |v| {
        v["silver_refresh"]["source"] = PRICE.into()
    })?;
    assert!(LaneContract::parse(Lane::Titles, &text).is_err());
    Ok(())
}

// ---- what a changed run starts -------------------------------------------------------------

#[test]
fn each_object_is_exported_to_its_own_handoff_under_the_release_id() -> anyhow::Result<()> {
    let contract = Lane::LandIndividualPrice.contract()?;
    let rule = contract.land.clone().context("a land rule")?;
    let object = ledger(PRICE, "SYNTHETIC-01", 1, None)?;
    let handoff = rule.handoff_key(&object)?;
    assert_eq!(
        handoff,
        format!("silver-handoff/{PRICE}/SYNTHETIC-01.jsonl.gz")
    );
    let ExportKind::Land { env_prefix } = contract.kind else {
        anyhow::bail!("a land kind");
    };
    let env: std::collections::BTreeMap<String, String> = land::export_environment(
        env_prefix,
        &object,
        &handoff,
        "vworldkr-land-individual-price:20991231",
        Path::new("/w/export-summary-97.json"),
    )
    .into_iter()
    .collect();
    assert_eq!(
        env["FOUNDATION_PLATFORM_LAND_INDIVIDUAL_PRICE_INPUT_OBJECT_KEY"],
        object.object_key
    );
    assert_eq!(
        env["FOUNDATION_PLATFORM_LAND_INDIVIDUAL_PRICE_OUTPUT_OBJECT_KEY"],
        handoff
    );
    assert_eq!(
        env["FOUNDATION_PLATFORM_LAND_INDIVIDUAL_PRICE_SOURCE_SNAPSHOT_ID"],
        "vworldkr-land-individual-price:20991231"
    );
    assert_eq!(env.len(), 4);
    Ok(())
}

#[test]
fn an_export_summary_says_its_rows_or_that_the_handoff_was_there() -> anyhow::Result<()> {
    let id = "vworldkr-land-individual-price:20991231";
    let converted = serde_json::json!({
        "outcome": "converted", "source": {"source_snapshot_id": id}, "output": {"row_count": 42}
    });
    assert_eq!(land::export_rows(&converted.to_string(), id)?, Some(42));
    let present =
        serde_json::json!({"outcome": "already_present", "source": {"source_snapshot_id": id}});
    assert_eq!(land::export_rows(&present.to_string(), id)?, None);
    assert!(land::export_rows(
        &converted.to_string(),
        "vworldkr-land-individual-price:20990101"
    )
    .is_err());
    let odd = serde_json::json!({"outcome": "skipped", "source": {"source_snapshot_id": id}});
    assert!(land::export_rows(&odd.to_string(), id).is_err());
    Ok(())
}

#[test]
fn the_first_load_batch_replaces_the_table_and_the_rest_append() -> anyhow::Result<()> {
    assert_eq!(land::batch_write_mode(0), WriteMode::Overwrite);
    assert_eq!(land::batch_write_mode(1), WriteMode::Append);
    assert_eq!(land::batch_write_mode(4), WriteMode::Append);
    let runtime = Runtime {
        release_root: PathBuf::from("/opt/foundation-platform/releases/synthetic"),
        lane_root: PathBuf::from("/data/foundation-platform/silver-refresh/land-use-plan"),
        spark_jars: "/home/spark/.ivy2/synthetic.jar".to_owned(),
        jars_dir: PathBuf::from("/opt/foundation-platform/artifacts/synthetic/jars"),
        project: format!("foundation-silver-refresh-{}", "0".repeat(32)),
        gid: "999".to_owned(),
        publisher: PathBuf::from(
            "/opt/foundation-platform/artifacts/synthetic/foundation-outbox-publisher",
        ),
    };
    let contract = Lane::LandUsePlan.contract()?;
    for (index, mode, flag) in [(0, "overwrite", true), (1, "append", false)] {
        let load = SparkLoad {
            input: "s3a://bucket/a.jsonl.gz,s3a://bucket/b.jsonl.gz".to_owned(),
            input_format: "jsonl",
            expected_count: Some(3),
            reads_r2: true,
            write_mode: land::batch_write_mode(index),
        };
        let args = execute::spark_arguments(&runtime, &contract, &load);
        assert!(args.windows(2).any(|w| w == ["--iceberg-write-mode", mode]));
        assert_eq!(
            args.iter().any(|a| a == "--allow-non-smoke-overwrite"),
            flag
        );
        assert!(args
            .windows(2)
            .any(|w| w == ["--contract", "silver.land_use_plan"]));
        assert!(args.windows(2).any(|w| w
            == [
                "-e",
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID"
            ]));
    }
    Ok(())
}
