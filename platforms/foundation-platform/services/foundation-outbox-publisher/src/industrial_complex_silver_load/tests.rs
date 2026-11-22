use chrono::{DateTime, Utc};
use serde_json::json;

use super::{
    complex_of, mirror_path, newest, planned_keys, unrecorded_inputs, Listed, INPUTS, PLAN_SCHEMA,
};

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

fn listed(key: &str, when: &str) -> Listed {
    Listed {
        key: key.to_owned(),
        last_modified: at(when),
    }
}

/// The newest workbook, not the first: the profile prefix holds an old `.xlsx` and monthly zips.
#[test]
fn the_newest_object_of_the_inputs_extension_is_taken() -> anyhow::Result<()> {
    let profile = &INPUTS[0];
    let objects = [
        listed(
            "bronze/source=vworldkr__sandan_profile/20991231DS99999-1.xlsx",
            "2099-03-01T00:00:00Z",
        ),
        listed(
            "bronze/source=vworldkr__sandan_profile/20991231DS99999-6.zip",
            "2099-02-01T00:00:00Z",
        ),
        listed(
            "bronze/source=vworldkr__sandan_profile/20991231DS99999-7.zip",
            "2099-02-02T00:00:00Z",
        ),
    ];
    assert_eq!(
        newest(profile, &objects)?,
        ["bronze/source=vworldkr__sandan_profile/20991231DS99999-7.zip"]
    );
    assert!(
        newest(profile, &objects[..1]).is_err(),
        "no workbook: no export"
    );
    Ok(())
}

/// One detail per complex, the newest of each; no detail at all is a list that lacked none.
#[test]
fn the_newest_detail_of_each_complex_is_taken() -> anyhow::Result<()> {
    let detail = &INPUTS[3];
    assert!(detail.per_complex);
    let key = |code: &str, day: &str| {
        format!("bronze/source=industrylandorkr__industrial_complex_detail/operation=danjiDetInfo/complex={code}/ingest_date={day}/page-1.json")
    };
    let objects = [
        listed(&key("99999A", "2099-01-01"), "2099-01-01T00:00:00Z"),
        listed(&key("99999A", "2099-01-02"), "2099-01-02T00:00:00Z"),
        listed(&key("99999B", "2099-01-01"), "2099-01-01T00:00:00Z"),
    ];
    assert_eq!(
        newest(detail, &objects)?,
        [key("99999A", "2099-01-02"), key("99999B", "2099-01-01")]
    );
    assert!(newest(detail, &[])?.is_empty());
    assert_eq!(complex_of(&key("99999A", "2099-01-01")), Some("99999A"));
    assert_eq!(complex_of("bronze/source=x/complex=/a.json"), None);
    Ok(())
}

#[test]
fn a_fetched_object_lands_under_its_own_key_and_nowhere_else() -> anyhow::Result<()> {
    let root = std::path::Path::new("run");
    assert_eq!(
        mirror_path(
            root,
            "bronze/source=vworldkr__sandan_profile/20991231DS99999-6.zip"
        )?,
        root.join("bronze/source=vworldkr__sandan_profile/20991231DS99999-6.zip")
    );
    for key in [
        "silver/x.zip",
        "bronze/source=a/../../etc/x.zip",
        "bronze/source=a//x.zip",
        "bronze/source=a/x y.zip",
    ] {
        assert!(mirror_path(root, key).is_err(), "{key}");
    }
    Ok(())
}

#[test]
fn a_plan_names_each_input_once() -> anyhow::Result<()> {
    let plan =
        |objects: serde_json::Value| json!({"schema_version": PLAN_SCHEMA, "objects": objects});
    assert_eq!(
        planned_keys(&plan(json!([{"object_key": "b"}, {"object_key": "a"}])))?,
        ["a", "b"]
    );
    assert!(planned_keys(&plan(json!([{"object_key": "a"}, {"object_key": "a"}]))).is_err());
    assert!(planned_keys(&plan(json!([]))).is_err());
    assert!(planned_keys(&json!({"schema_version": "other", "objects": []})).is_err());
    Ok(())
}

/// 2026-11-22, the first run of this load: the batch named every input, the profile workbook among
/// them, which the 2026-09-01 and 2026-10-01 loads had already recorded, and the commit refused the
/// whole batch ("a batch is appended whole or not at all"). The commit records only what the table
/// does not hold yet; with nothing new it records nothing.
#[test]
fn the_commit_records_only_the_inputs_the_table_does_not_hold() {
    let profile = "bronze/source=vworldkr__sandan_profile/20991231DS99999-6.zip".to_owned();
    let list = "bronze/source=industrylandorkr__industrial_complex_list/operation=danjiList/scope=national/page-000001--sha256-aa.json".to_owned();
    let notice = "bronze/source=industrylandorkr__industrial_complex_notice/operation=danrefList/scope=national/page-000001--sha256-bb.json".to_owned();
    let planned = vec![profile.clone(), list.clone(), notice.clone()];
    let recorded: std::collections::BTreeSet<String> = [
        profile.clone(),
        format!("{profile}@development-stage-2099-01"),
    ]
    .into_iter()
    .collect();
    assert_eq!(unrecorded_inputs(&planned, &recorded), [list, notice]);
    let everything: std::collections::BTreeSet<String> = planned.iter().cloned().collect();
    assert!(unrecorded_inputs(&planned, &everything).is_empty());
}
