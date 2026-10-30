//! The VWorld land lanes (root ADR-0169 step 3): release selection from the ledger's ZIP members,
//! the table's own record, the contracts and what a changed run starts. Fixtures are synthetic:
//! region codes 97–99, object keys and dates that name no real file.
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::NaiveDate;

use super::execute::{self, Runtime, SparkLoad};
use super::land::{
    self, Candidate, Completeness, LandRelease, LandRule, MeasuredObject, Reading, SidoCalendar,
    MEASURE_COMMAND,
};
use super::lane::{ExportKind, Lane, LaneContract, WriteMode};
use super::release::LedgerObject;
use super::*;

const PRICE: &str = "vworldkr__land_individual_price";
const ZONE: &str = "vworldkr__land_use_zone_code";

const LAND_LANES: [Lane; 8] = [
    Lane::LandCharacteristic,
    Lane::LandForestLedger,
    Lane::LandIndividualPrice,
    Lane::LandOwnership,
    Lane::LandRightRegistration,
    Lane::LandTransferHistory,
    Lane::LandUsePlan,
    Lane::LandUseZoneCode,
];

fn rule() -> LandRule {
    LandRule {
        source: PRICE.to_owned(),
        member_name: r"^AL_D151_(?P<region>[0-9]{2})_(?P<vintage>[0-9]{8})\.csv$".to_owned(),
        completeness: Completeness::Sido,
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

/// One row of `reference.legal_dong_code_snapshot`, as the scan decodes it.
fn code_row(
    snapshot: &str,
    sido: &str,
    status: &str,
    created: &str,
    abolished: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let row = serde_json::json!({
        "region_cd": format!("{sido}00000000"),
        "status": status,
        "snapshot_date": snapshot,
        "created_date": created,
        "abolished_date": abolished,
    });
    row.as_object().cloned().unwrap_or_default()
}

/// A code list whose 시도 are `codes`, all in force since always.
fn sido(codes: &[&str]) -> anyhow::Result<SidoCalendar> {
    let rows: Vec<_> = codes
        .iter()
        .map(|code| code_row("2099-12-01", code, "존재", "", ""))
        .collect();
    SidoCalendar::from_snapshot_rows(&rows)
}

/// Selects with a code list of the 시도 `codes`.
fn select(codes: &[&str], measured: &[MeasuredObject]) -> anyhow::Result<LandRelease> {
    let candidates = land::candidates(&rule(), measured, converts_price)?;
    land::select(&rule(), &candidates.found, Some(&sido(codes)?))
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
    let release = select(&["97", "98"], &measured)?;
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
        release.source_snapshot_id(&rule())?,
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
    let release = select(&["97", "98"], &measured)?;
    assert_eq!(release.vintage, "20990101");
    assert_eq!(release.skipped_incomplete, ["20990301 regions=97"]);
    // A vintage with more regions than the contract's is not a release either.
    let error = select(&["97"], &measured[..2])
        .err()
        .context("two regions are not the one the rule requires")?;
    assert!(
        error.to_string().contains("20990101 regions=97,98"),
        "{error:#}"
    );
    Ok(())
}

/// A code list in which 시도 98 and 99 merge into 96 on 2099-06-01 (synthetic codes).
fn merger() -> anyhow::Result<SidoCalendar> {
    SidoCalendar::from_snapshot_rows(&[
        code_row("2099-12-01", "97", "존재", "", ""),
        code_row("2099-12-01", "98", "폐지", "", "20990601"),
        code_row("2099-12-01", "99", "폐지", "19880101", "20990601"),
        code_row("2099-12-01", "96", "존재", "20990601", ""),
        // a 폐지 row without its day cannot be placed and is left out
        code_row("2099-12-01", "95", "폐지", "", ""),
        // not a 시도 row
        {
            let mut row = code_row("2099-12-01", "97", "존재", "", "");
            row.insert("region_cd".to_owned(), "9711000000".into());
            row
        },
        // an older snapshot is not read: the newest one says what each code's days are
        code_row("2099-11-01", "94", "존재", "", ""),
    ])
}

#[test]
fn the_code_list_says_which_sido_a_day_has() -> anyhow::Result<()> {
    let calendar = merger()?;
    let on = |day: &str| calendar.on(day).into_iter().collect::<Vec<_>>();
    assert_eq!(on("20990531"), ["97", "98", "99"]);
    assert_eq!(on("20990601"), ["96", "97"]);
    // A list that names no current 시도 is no list.
    let empty =
        SidoCalendar::from_snapshot_rows(&[code_row("2099-12-01", "98", "폐지", "", "20990601")]);
    assert!(empty.is_err());
    let undated =
        SidoCalendar::from_snapshot_rows(&[code_row("2099-12-01", "97", "존재", "2099", "")]);
    assert!(undated.is_err());
    Ok(())
}

/// A row of the code list at any level (`region_cd` in full).
fn region_row(
    region: &str,
    status: &str,
    created: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut row = code_row("2099-12-01", "00", status, created, "");
    row.insert("region_cd".to_owned(), region.into());
    row
}

#[test]
fn a_single_tier_city_is_a_sido_by_its_one_district_row() -> anyhow::Result<()> {
    // 세종특별자치시 has no 3600000000 row, only 3611000000; leaving it out made every vintage of
    // every land lane incomplete on 2026-10-15 (synthetic prefix 93 here).
    let calendar = SidoCalendar::from_snapshot_rows(&[
        code_row("2099-12-01", "97", "존재", "", ""),
        region_row("9711000000", "존재", ""),
        region_row("9311000000", "존재", "20990301"),
    ])?;
    assert_eq!(
        calendar.on("20990228").into_iter().collect::<Vec<_>>(),
        ["97"]
    );
    assert_eq!(
        calendar.on("20990301").into_iter().collect::<Vec<_>>(),
        ["93", "97"]
    );
    // Two district-level rows under a prefix with no 시도 row: which one is the 시도 is unknown.
    let ambiguous = SidoCalendar::from_snapshot_rows(&[
        code_row("2099-12-01", "97", "존재", "", ""),
        region_row("9311000000", "존재", ""),
        region_row("9312000000", "존재", ""),
    ]);
    assert!(ambiguous.is_err());
    Ok(())
}

#[test]
fn after_a_merger_the_fewer_sido_are_a_release_and_the_old_ones_are_not() -> anyhow::Result<()> {
    let measured = [
        price("SYNTHETIC-01", "97", "20990501", None)?,
        price("SYNTHETIC-02", "98", "20990501", None)?,
        price("SYNTHETIC-03", "99", "20990501", None)?,
        // after the merger: two 시도 cover the country
        price("SYNTHETIC-04", "96", "20990701", None)?,
        price("SYNTHETIC-05", "97", "20990701", None)?,
        // a newer harvest still under the old codes is not loaded under a date they had ended
        price("SYNTHETIC-06", "97", "20990801", None)?,
        price("SYNTHETIC-07", "98", "20990801", None)?,
        price("SYNTHETIC-08", "99", "20990801", None)?,
    ];
    let candidates = land::candidates(&rule(), &measured, converts_price)?;
    let release = land::select(&rule(), &candidates.found, Some(&merger()?))?;
    assert_eq!(release.vintage, "20990701");
    assert_eq!(regions(&release), ["96", "97"]);
    assert_eq!(release.skipped_incomplete, ["20990801 regions=97,98,99"]);
    // Before the merger's harvest, the three old 시도 are the release.
    let older = land::candidates(&rule(), &measured[..3], converts_price)?;
    let release = land::select(&rule(), &older.found, Some(&merger()?))?;
    assert_eq!(regions(&release), ["97", "98", "99"]);
    // A 시도 lane without the code list is refused, never guessed.
    assert!(land::select(&rule(), &candidates.found, None).is_err());
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
    let error = land::candidates(&rule(), &measured, converts_price)
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
        let release = select(&["97"], &measured)?;
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
        let error = select(&["97"], &measured)
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
    assert_eq!(select(&["97", "98"], &measured)?.vintage, "20990101");
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
    let candidates = land::candidates(&rule(), &measured, converts_price)?;
    assert_eq!(candidates.found.len(), 1);
    assert_eq!(candidates.refused.len(), 2, "{:?}", candidates.refused);
    assert!(candidates.refused[0].contains("SYNTHETIC-02"));
    assert!(candidates.refused[1].contains("no date"));
    // Without it 98 is uncovered, so there is no release rather than a guessed one.
    assert!(land::select(&rule(), &candidates.found, Some(&sido(&["97", "98"])?)).is_err());
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
    let release = land::select(&zone_rule(), &candidates.found, None)?;
    assert_eq!(release.vintage, "20991203");
    assert_eq!(regions(&release), ["national"]);
    assert_eq!(
        release.source_snapshot_id(&zone_rule())?,
        "vworldkr-land-use-zone-code:SYNTHETIC-3"
    );
    // Two objects of one ledger day are a tie unless the provider dates them apart.
    let tied = [code_table("SYNTHETIC-1", 3)?, code_table("SYNTHETIC-3", 3)?];
    let candidates = land::candidates(&zone_rule(), &tied, |n| n == "LART_LMISZONE.csv")?;
    assert!(land::select(&zone_rule(), &candidates.found, None).is_err());
    Ok(())
}

// ---- unchanged or changed ------------------------------------------------------------------

fn candidates_of(measured: &[MeasuredObject]) -> anyhow::Result<Vec<Candidate>> {
    Ok(land::candidates(&rule(), measured, converts_price)?.found)
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
    let newest = land::select(&rule(), &all, Some(&sido(&["97", "98"])?))?;
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
    let previous = land::select(
        &rule(),
        &candidates_of(&measured[..2])?,
        Some(&sido(&["97", "98"])?),
    )?;
    assert_eq!(
        land::decide(&previous, &all, &half),
        Decision::Unchanged("newer_release_loaded")
    );
    let line = outcome_line(
        Lane::LandIndividualPrice,
        &newest.vintage,
        &Decision::Unchanged("already_loaded"),
        &newest.source_snapshot_id(&rule())?,
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
    }
    Ok(())
}

/// For each lane, a synthetic member it loads and siblings in its prefix it must not.
fn members(lane: Lane) -> (&'static str, &'static [&'static str]) {
    match lane {
        Lane::LandCharacteristic => ("AL_D195_97_20991231.csv", &["AL_D194_97110_20991231.shp"]),
        Lane::LandForestLedger => ("AL_D003_97_20991231.csv", &["CH_D003_97_20991231.csv"]),
        Lane::LandIndividualPrice => ("AL_D151_97_20991231.csv", &["AL_D150_97_20991231.dbf"]),
        Lane::LandOwnership => ("AL_D161_97_20991231.csv", &["AL_D160_97_20991231.dbf"]),
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
        if rule.completeness == Completeness::Sido {
            // The 시도 are the code list's on the vintage's day, never a number in the contract.
            let (_, text) = lane.contract_source();
            let value: serde_json::Value = serde_json::from_str(text)?;
            let completeness = &value["silver_refresh"]["completeness"];
            assert!(completeness.get("count").is_none(), "{}", lane.id());
            assert_eq!(
                completeness["regions_from"],
                "reference.legal_dong_code_snapshot",
                "{}",
                lane.id()
            );
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
        // completeness that is not 시도 from the code list or national 1: a hand-written 시도
        // count (17 stopped being true on 2026-07-01) or another list of regions
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["completeness"]["count"] = 17.into()),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| v["silver_refresh"]["completeness"]["count"] = 16.into()),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| {
                v["silver_refresh"]["completeness"]["regions_from"] =
                    "silver.administrative_boundaries".into()
            }),
        ),
        (
            Lane::LandUsePlan,
            Box::new(|v| {
                if let Some(block) = v["silver_refresh"]["completeness"].as_object_mut() {
                    block.remove("regions_from");
                }
            }),
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
            branch: Some(execute::load_branch("synthetic-load")),
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
        // Every batch of the release stages on the load's one branch (root ADR-0183).
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--iceberg-branch" && w[1] == execute::load_branch("synthetic-load")));
        assert!(args
            .windows(2)
            .any(|w| w == ["-e", "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID"]));
    }
    Ok(())
}

#[test]
fn regions_exported_side_by_side_come_back_in_region_order() -> anyhow::Result<()> {
    // 2026-10-11: the transfer history's 17 regions ran one after another for 60 minutes.
    let regions: Vec<u64> = (0..12).collect();
    let exported = land::export_side_by_side(&regions, "synthetic", |region| {
        // Later regions finish first, as a slower large region would let them.
        std::thread::sleep(std::time::Duration::from_millis(12 - region));
        Ok(region * 10)
    })?;
    assert_eq!(
        exported,
        (0..12).map(|region| region * 10).collect::<Vec<_>>()
    );
    assert!(land::export_side_by_side(&Vec::<u64>::new(), "synthetic", |_| Ok(0))?.is_empty());
    Ok(())
}

#[test]
fn a_failed_region_export_fails_the_release() {
    let regions: Vec<u64> = (0..8).collect();
    let failed = land::export_side_by_side(&regions, "synthetic", |region| {
        anyhow::ensure!(*region != 5, "region {region} export failed");
        Ok(*region)
    });
    assert!(
        failed.is_err(),
        "a failed region was dropped from the release"
    );
    let first = land::export_side_by_side(&regions, "synthetic", |region| {
        anyhow::ensure!(*region != 0, "the first region failed");
        Ok(*region)
    });
    assert!(first
        .err()
        .is_some_and(|error| error.to_string().contains("the first region failed")));
}

// ---- a lane that appends a release's changes (root ADR-0275) --------------------------------

/// The land lanes whose table marks vanished keys append each release's changes (root ADR-0275,
/// ADR-0311); the other two state why they still write whole.
const COMPARING_LANES: [Lane; 8] = [
    Lane::LandCharacteristic,
    Lane::LandForestLedger,
    Lane::LandIndividualPrice,
    Lane::LandOwnership,
    Lane::LandRightRegistration,
    Lane::LandTransferHistory,
    Lane::LandUsePlan,
    Lane::LandUseZoneCode,
];

#[test]
fn the_comparing_land_lanes_never_compare_their_key_or_load_columns() -> anyhow::Result<()> {
    for lane in LAND_LANES {
        let contract = lane.contract()?;
        let compared = contract.compare_content;
        if COMPARING_LANES.contains(&lane) {
            let content = compared.with_context(|| format!("{lane:?} appends its changes"))?;
            let latest = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
                &contract.table,
            )
            .and_then(|table| table.current_row_latest)
            .with_context(|| format!("{lane:?} declares its current rows"))?;
            for column in latest.key.iter().copied().chain([
                "data_reference_date",
                "source_record_id",
                "source_snapshot_id",
                "ingested_at_utc",
                "row_state",
            ]) {
                assert!(
                    !content.iter().any(|c| c == column),
                    "{lane:?} compares {column}"
                );
            }
        } else {
            assert_eq!(compared, None, "{lane:?} publishes its release whole");
        }
    }
    Ok(())
}

#[test]
fn a_compare_the_table_cannot_answer_is_refused() -> anyhow::Result<()> {
    let compare = |columns: serde_json::Value| {
        move |v: &mut serde_json::Value| {
            v["silver_refresh"]["compare"] = serde_json::json!({ "content_columns": columns });
        }
    };
    let refused = [
        // the key, a column the table lacks, nothing
        (
            Lane::LandTransferHistory,
            compare(serde_json::json!(["pnu"])),
        ),
        (
            Lane::LandTransferHistory,
            compare(serde_json::json!(["no_such_column"])),
        ),
        (Lane::LandTransferHistory, compare(serde_json::json!([]))),
        // a table that does not mark vanished keys (and a lane naming compare beside full_reason)
        (Lane::LandUsePlan, compare(serde_json::json!(["pnu"]))),
        // a hub lane publishes whole
        (Lane::Titles, compare(serde_json::json!(["pnu"]))),
    ];
    for (lane, edit) in refused {
        let text = planted(lane, edit)?;
        assert!(LaneContract::parse(lane, &text).is_err(), "accepted {text}");
    }
    Ok(())
}

#[test]
fn a_release_compare_names_its_branch_release_and_every_object() -> anyhow::Result<()> {
    let runtime = Runtime {
        release_root: PathBuf::from("/opt/foundation-platform/releases/synthetic"),
        lane_root: PathBuf::from("/data/foundation-platform/silver-refresh/land-transfer-history"),
        spark_jars: "/home/spark/.ivy2/synthetic.jar".to_owned(),
        jars_dir: PathBuf::from("/opt/foundation-platform/artifacts/synthetic/jars"),
        project: format!("foundation-silver-refresh-{}", "0".repeat(32)),
        gid: "999".to_owned(),
        publisher: PathBuf::from(
            "/opt/foundation-platform/artifacts/synthetic/foundation-outbox-publisher",
        ),
    };
    let contract = Lane::LandTransferHistory.contract()?;
    let mut compare = execute::ReleaseCompare {
        branch: execute::load_branch("synthetic-load"),
        changes_branch: execute::changes_branch("synthetic-load"),
        source_snapshot_id: "vworldkr-land-transfer-history:20990901".to_owned(),
        objects: vec!["bronze/a.zip".to_owned(), "bronze/b.zip".to_owned()],
        content_columns: vec!["reason".to_owned(), "moved_at".to_owned()],
        bucket_prefix_length: 2,
        deadline_utc: Some("2099-09-01T08:50:00Z".to_owned()),
    };
    let args = execute::compare_arguments(&runtime, &contract, &compare)?;
    assert!(args
        .iter()
        .any(|a| a.ends_with("/spark/jobs/silver_release_compare.py")));
    for pair in [
        ["--contract", "silver.land_transfer_history"],
        ["--branch", compare.branch.as_str()],
        [
            "--source-snapshot-id",
            "vworldkr-land-transfer-history:20990901",
        ],
        ["--objects", "bronze/a.zip,bronze/b.zip"],
        ["--content-columns", "reason,moved_at"],
        ["--changes-branch", compare.changes_branch.as_str()],
        ["--bucket-prefix-length", "2"],
        ["--deadline-utc", "2099-09-01T08:50:00Z"],
    ] {
        assert!(args.windows(2).any(|w| w == pair), "missing {pair:?}");
    }
    // It reads Iceberg only, through the catalog: no R2 reader pair.
    assert!(!args
        .iter()
        .any(|a| a == "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID"));
    // The load and its changes stage on two branches named by the same identity.
    assert_ne!(compare.branch, compare.changes_branch);
    assert_eq!(
        compare.changes_branch.strip_prefix("changes_"),
        compare.branch.strip_prefix("load_")
    );
    compare.objects.push("bronze/c,d.zip".to_owned());
    assert!(execute::compare_arguments(&runtime, &contract, &compare).is_err());
    Ok(())
}

#[test]
fn a_sido_lane_compares_by_the_pnus_sido_and_a_national_lane_in_one_piece() -> anyhow::Result<()> {
    // ADR-0318: a 시도 lane's buckets are the PNU's 시도, its first two digits.
    for lane in Lane::ALL {
        let contract = lane.contract()?;
        let (Some(rule), Some(_)) = (&contract.land, &contract.compare_content) else {
            continue;
        };
        let latest =
            lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(&contract.table)
                .and_then(|table| table.current_row_latest)
                .context("a comparing lane's table keeps versions")?;
        let expected = match rule.completeness {
            Completeness::Sido => {
                assert_eq!(latest.key.first(), Some(&"pnu"), "{}", lane.id());
                2
            }
            Completeness::National => 0,
        };
        assert_eq!(
            contract.compare_bucket_prefix_length,
            expected,
            "{}",
            lane.id()
        );
    }
    for (raw, outcome) in [
        ("staged", Some(execute::CompareOutcome::Staged)),
        (
            "already_appended",
            Some(execute::CompareOutcome::AlreadyAppended),
        ),
        ("deadline", Some(execute::CompareOutcome::Deadline)),
        ("appended", None),
    ] {
        assert_eq!(
            execute::CompareOutcome::of(&serde_json::json!({ "outcome": raw })),
            outcome
        );
    }
    Ok(())
}

#[test]
fn no_compare_bucket_starts_too_late_for_the_unit_to_finish() {
    // ADR-0318: the deadline is counted against the unit's own limit, read from the unit file.
    let unit = include_str!("../../../../../../infra/systemd/foundation-silver-refresh@.service");
    assert!(
        unit.lines()
            .any(|line| line.trim()
                == format!("TimeoutStartSec={}m", super::UNIT_TIME_LIMIT_MINUTES)),
        "UNIT_TIME_LIMIT_MINUTES is not the unit's TimeoutStartSec"
    );
    const _: () = assert!(super::LAST_BUCKET_START_MINUTES + 30 <= super::UNIT_TIME_LIMIT_MINUTES);
}
