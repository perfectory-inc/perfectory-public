use super::*;

const REAL: &str =
    include_str!("../../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");

const CONTRACT: &str = r#"{
    "schema_version": 3, "source": "bronze/source=fixture__parcel", "load_granularity": "sigungu",
    "member_name": "^LSMD_CONT_LDREG_(?P<region>[0-9]{2}|[0-9]{5})_(?P<edition>[0-9]{6})\\.shp$",
    "served_edition": "209906", "served_edition_sido": ["91", "92"],
    "handoff_prefix": "h/root", "handoff_prefix_legacy": {"209906": "h/root"},
    "handoff_suffix": ".jsonl.gz",
    "new_edition_bounds": {"max_sido_set_difference": 3, "max_sigungu_count_change_share": 0.5}
}"#;

fn day(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn zip(file: &str, members: &[(&str, &str)]) -> LedgerObject {
    LedgerObject {
        object_key: format!("bronze/source=fixture__parcel/{file}"),
        size_bytes: 10,
        provider_updated_at: None,
        reading: Reading::Zip(
            members
                .iter()
                .map(|(name, date)| Member {
                    name: (*name).to_owned(),
                    modified: day(date),
                })
                .collect(),
        ),
    }
}

fn shp(file: &str, region: &str, edition: &str, date: &str) -> LedgerObject {
    zip(
        file,
        &[
            (&format!("LSMD_CONT_LDREG_{region}_{edition}.shp"), date),
            ("x.dbf", date),
        ],
    )
}

/// Two editions of two 시도: 209906 (one 시도 file, three 시군구) and 209909 (three 시군구).
fn ledger() -> Vec<LedgerObject> {
    vec![
        shp("1-1.zip", "91", "209906", "2099-06-10"),
        shp("1-2.zip", "91001", "209906", "2099-06-11"),
        shp("1-3.zip", "91002", "209906", "2099-06-12"),
        shp("1-4.zip", "92001", "209906", "2099-06-12"),
        shp("2-2--sha256-a.zip", "91001", "209909", "2099-09-13"),
        shp("2-3--sha256-b.zip", "91002", "209909", "2099-09-14"),
        shp("2-4--sha256-c.zip", "92001", "209909", "2099-09-16"),
        LedgerObject {
            object_key: "bronze/source=fixture__parcel/1-0.xlsx".to_owned(),
            size_bytes: 1,
            provider_updated_at: None,
            reading: Reading::NotZip,
        },
    ]
}

fn error_of(result: anyhow::Result<Value>) -> String {
    result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default()
}

#[test]
fn the_editions_are_the_ledgers() -> anyhow::Result<()> {
    let found = editions(CONTRACT, &ledger())?;
    let june = &found["editions"]["209906"];
    assert_eq!(june["granularity_counts"], json!({"sido": 1, "sigungu": 3}));
    assert_eq!(
        june["extracted_on"],
        json!({"earliest": "2099-06-10", "latest": "2099-06-12"})
    );
    assert_eq!(
        june["handoff_prefix"], "h/root",
        "an edition loaded before the rule keeps its prefix"
    );
    assert_eq!(june["objects"][0]["granularity"], "sido");
    assert_eq!(
        june["objects"][0]["dataset_name"],
        "LSMD_CONT_LDREG_91_209906"
    );
    let september = &found["editions"]["209909"];
    assert_eq!(september["handoff_prefix"], "h/root/edition=209909");
    assert_eq!(september["provider_base_month"], "2099-09");
    assert_eq!(september["objects"].as_array().map(Vec::len), Some(3));
    let view = served_edition_view(CONTRACT, &found)?;
    assert_eq!(view["edition"], "209906");
    assert_eq!(view["objects"].as_array().map(Vec::len), Some(4));
    Ok(())
}

#[test]
fn an_unmeasured_object_refuses_the_run() {
    let mut held = ledger();
    held.push(LedgerObject {
        reading: Reading::Unmeasured,
        ..shp("3-1.zip", "91001", "209912", "2099-12-01")
    });
    let error = error_of(editions(CONTRACT, &held));
    assert!(
        error.contains("no ZIP member reading yet") && error.contains(MEASURE_COMMAND),
        "{error}"
    );
}

#[test]
fn a_partial_collection_is_skipped_and_never_served() -> anyhow::Result<()> {
    let mut held = ledger();
    held.push(shp("3-1.zip", "91001", "209912", "2099-12-01"));
    let found = editions(CONTRACT, &held)?;
    assert!(
        found["editions"].get("209912").is_none(),
        "one 시도 of two is not an edition"
    );
    assert!(
        found["skipped"][0]
            .as_str()
            .unwrap_or_default()
            .starts_with("209912"),
        "{found}"
    );
    let served_partial = CONTRACT.replace(
        r#""served_edition": "209906""#,
        r#""served_edition": "209912""#,
    );
    assert!(error_of(editions(&served_partial, &held)).contains("served edition 209912 is partial"));
    Ok(())
}

#[test]
fn two_objects_of_one_region_need_a_later_provider_date() -> anyhow::Result<()> {
    let mut held = ledger();
    held.push(shp("9-2.zip", "91001", "209909", "2099-09-20"));
    assert!(error_of(editions(CONTRACT, &held)).contains("refusing to choose"));
    if let Some(last) = held.last_mut() {
        last.provider_updated_at = day("2099-09-21");
    }
    let found = editions(CONTRACT, &held)?;
    assert_eq!(
        found["editions"]["209909"]["objects"][0]["object_key"],
        "bronze/source=fixture__parcel/9-2.zip"
    );
    Ok(())
}

#[test]
fn the_contracts_served_sido_must_be_the_ledgers() {
    let wrong = CONTRACT.replace(r#"["91", "92"]"#, r#"["91"]"#);
    assert!(error_of(editions(&wrong, &ledger())).contains("served_edition_sido says"));
}

/// The defect this module removed: the contract listed every object of every edition by hand.
#[test]
fn the_contract_lists_no_edition_and_no_object() -> anyhow::Result<()> {
    assert!(retired_key(&serde_json::from_str(REAL)?).is_none());
    assert!(!served_sido(REAL)?.is_empty());
    let planted = REAL.replacen('{', r#"{"editions": {"209906": {"objects": []}},"#, 1);
    let error = served_sido(&planted)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(error.contains("holds `editions`"), "{error}");
    let nested = CONTRACT.replacen('{', r#"{"new": {"objects": []},"#, 1);
    assert!(error_of(editions(&nested, &ledger())).contains("holds `objects`"));
    Ok(())
}

#[test]
fn a_province_merger_is_an_edition_and_a_vanished_province_is_not() -> anyhow::Result<()> {
    let mut held = ledger();
    // 시도 합병: 92 가 떠나고 93 이 나타났다. 시군구 수는 그대로다.
    for (file, region) in [
        ("3-2.zip", "91001"),
        ("3-3.zip", "91002"),
        ("3-4.zip", "93001"),
    ] {
        held.push(shp(file, region, "209912", "2099-12-01"));
    }
    let found = editions(CONTRACT, &held)?;
    assert!(found["editions"].get("209912").is_some(), "{found}");
    held.retain(|object| !object.object_key.ends_with("3-4.zip"));
    held.push(shp("3-5.zip", "91003", "209912", "2099-12-01"));
    let found = editions(CONTRACT, &held)?;
    assert!(
        found["editions"].get("209912").is_none(),
        "92 left and nothing appeared: {found}"
    );
    Ok(())
}
