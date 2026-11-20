use super::*;

/// A stage holding the gate's inputs as plain tables (synthetic 99999 codes only).
fn stage(extra: &str) -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&format!(
        "CREATE TABLE parcels(pnu VARCHAR, source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP);
         CREATE TABLE code_list(region_cd VARCHAR, status VARCHAR, snapshot_date DATE);
         CREATE TABLE changes(kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR,
                              effective_date VARCHAR, source VARCHAR);
         CREATE TABLE admin(canonical_code VARCHAR);
         CREATE TABLE registry(parcel_id VARCHAR, pnu VARCHAR, status VARCHAR, valid_from VARCHAR,
                               valid_to VARCHAR, to_snapshot_id VARCHAR);
         -- The newest list holds 10100 and 20200; an older one held 10200 too.
         INSERT INTO code_list VALUES ('9999910100', '존재', DATE '2099-02-01'),
           ('9999920200', '존재', DATE '2099-02-01'), ('9999930300', '폐지', DATE '2099-02-01'),
           ('9999910200', '존재', DATE '2099-01-01');
         INSERT INTO changes VALUES ('pair', '9999910200', '9999920200', 'eupmyeondong', '2099-01-15', 'official:x');
         INSERT INTO admin VALUES ('9999910100'), ('9999920200');
         {extra}"
    ))?;
    Ok((directory, stage))
}

fn sources() -> GateSources {
    GateSources {
        parcels: "parcels".to_owned(),
        code_list: "code_list".to_owned(),
        code_changes: "changes".to_owned(),
        admin: "admin".to_owned(),
        registry: Some("registry".to_owned()),
    }
}

fn run<'a>(through: &'a [String], registry: Registry) -> GateRun<'a> {
    GateRun {
        snapshot_id: "edition-b",
        sido: "99",
        registry_through: through,
        registry,
    }
}

fn through() -> Vec<String> {
    vec!["edition-a".to_owned(), "edition-b".to_owned()]
}

#[test]
fn an_edition_whose_parcels_are_listed_bounded_and_registered_passes() -> anyhow::Result<()> {
    let (_dir, stage) = stage(
        "INSERT INTO parcels VALUES ('9999910100100010000', 'edition-b', NULL),
           ('9999910100100020000', 'edition-b', NULL), ('9999910100100020000', 'edition-b', NULL),
           ('9999910100100030000', 'edition-b', TIMESTAMP '2099-01-01'),
           ('9999910100100040000', 'edition-a', NULL);
         INSERT INTO registry VALUES ('P1', '9999910100100010000', 'current', '2099-01-01', '', 'edition-a'),
           ('P2', '9999910100100020000', 'current', '2099-01-01', '', 'edition-b'),
           ('P9', '9999910100100020000', 'current', '2099-03-01', '', 'edition-c');",
    )?;
    let ids = through();
    let verdict = verdict(&stage, &sources(), &run(&ids, Registry::Checked))?;
    assert_eq!(
        verdict,
        json!({
            "schema_version": VERDICT_SCHEMA_VERSION,
            "job": "parcel_matching_gate",
            "snapshot_id": "edition-b",
            "sido": "99",
            "registry": "checked",
            "snapshot_parcel_count": 2,
            "renumbered_legal_dongs": 1,
            "parcels": {"passed": true, "checked": 2, "violations": {}, "allowed": {}},
            "official_code_snapshot": "2099-02-01",
            "attributes": {},
            "passed": true,
        })
    );
    Ok(())
}

#[test]
fn each_refusal_is_counted_and_sampled_and_a_renumbered_dong_is_checked_under_its_successor(
) -> anyhow::Result<()> {
    let (_dir, stage) = stage(
        "INSERT INTO parcels VALUES
           ('9999910100100010000', 'edition-b', NULL),  -- registered, closed by a later historic row
           ('9999910200100020000', 'edition-b', NULL),  -- renumbered into 20200
           ('9999940400100030000', 'edition-b', NULL),  -- a stray dong, within the bound
           ('99999101001000X0000', 'edition-b', NULL),  -- malformed
           ('9999910100100050000', 'edition-b', NULL),  -- shares P5
           ('9999910100100060000', 'edition-b', NULL),  -- shares P5
           (NULL, 'edition-b', NULL);
         INSERT INTO registry VALUES
           ('P1', '9999910100100010000', 'current', '2099-01-01', '', 'edition-a'),
           ('P1', '9999910100100010000', 'historic', '', '2099-01-20', 'edition-b'),
           ('P2', '9999910200100020000', 'current', '2099-01-01', '', 'edition-a'),
           ('P5', '9999910100100050000', 'current', '2099-01-01', '', 'edition-a'),
           ('P5', '9999910100100060000', 'current', '2099-01-01', '', 'edition-a');
         DELETE FROM admin WHERE canonical_code = '9999920200';",
    )?;
    let ids = through();
    let verdict = verdict(&stage, &sources(), &run(&ids, Registry::Checked))?;
    assert_eq!(
        verdict["snapshot_parcel_count"], 7,
        "a NULL PNU is one distinct value"
    );
    assert_eq!(
        verdict["parcels"],
        json!({
            "passed": false,
            "checked": 6,
            "violations": {
                reasons::MALFORMED_PNU: {"count": 1, "sample": ["99999101001000X0000"]},
                reasons::NO_BOUNDARY: {"count": 1, "sample": ["9999910200100020000"]},
                reasons::NO_CURRENT_ID: {"count": 2, "sample": ["9999910100100010000", "9999940400100030000"]},
                reasons::ID_ON_TWO_PARCELS: {"count": 1, "sample": ["P5"]},
            },
            "allowed": {reasons::RENUMBERED_LEGAL_DONG: 1, reasons::UNEXPLAINED_LEGAL_DONG: 1},
            "unexplained_sample": ["9999940400100030000"],
        })
    );
    assert_eq!(verdict["passed"], false);
    Ok(())
}

#[test]
fn strays_past_the_bound_are_refused_as_unlisted_dongs() -> anyhow::Result<()> {
    let rows = (0..=UNEXPLAINED_LEGAL_DONG_BOUND)
        .map(|i| format!("('99999404001{i:08}', 'edition-b', NULL)"))
        .collect::<Vec<_>>()
        .join(", ");
    let (_dir, stage) = stage(&format!("INSERT INTO parcels VALUES {rows};"))?;
    let verdict = verdict(&stage, &sources(), &run(&[], Registry::Skipped))?;
    let parcels = &verdict["parcels"];
    assert_eq!(
        parcels["violations"][reasons::NO_LEGAL_DONG]["count"],
        UNEXPLAINED_LEGAL_DONG_BOUND + 1
    );
    assert_eq!(
        parcels["violations"][reasons::NO_LEGAL_DONG]["sample"]
            .as_array()
            .map(Vec::len),
        Some(SAMPLE)
    );
    assert_eq!(parcels["allowed"], json!({}));
    assert!(parcels.get("unexplained_sample").is_none());
    assert!(
        parcels["violations"].get(reasons::NO_CURRENT_ID).is_none(),
        "a skipped registry checks no ids"
    );
    assert_eq!(verdict["registry"], "skipped");
    Ok(())
}

#[test]
fn only_the_sido_asked_for_are_checked_but_the_whole_edition_is_counted() -> anyhow::Result<()> {
    let (_dir, stage) = stage(
        "INSERT INTO parcels VALUES ('9999910100100010000', 'edition-b', NULL),
           ('9899910100100010000', 'edition-b', NULL);",
    )?;
    let verdict = verdict(&stage, &sources(), &run(&[], Registry::Skipped))?;
    assert_eq!(verdict["snapshot_parcel_count"], 2);
    assert_eq!(verdict["parcels"]["checked"], 1);
    assert!(verdict_with_sido(&stage, "9").is_err());
    assert!(verdict_with_sido(&stage, "99,'1").is_err());
    Ok(())
}

fn verdict_with_sido(stage: &DuckDbParquetStage, sido: &str) -> anyhow::Result<Value> {
    verdict(
        stage,
        &sources(),
        &GateRun {
            sido,
            ..run(&[], Registry::Skipped)
        },
    )
}

/// The bytes `json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True) + "\n"` gives for the
/// same value (printed by CPython 3.13 for this fixture).
#[test]
fn the_verdict_file_is_written_as_the_spark_job_wrote_it() -> anyhow::Result<()> {
    let value = json!({
        "sido": "99", "passed": true, "attributes": {},
        "parcels": {"allowed": {reasons::RENUMBERED_LEGAL_DONG: 3}, "checked": 4, "passed": true,
                    "unexplained_sample": ["9999940400100030000"], "violations": {}},
    });
    let expected = "{\n  \"attributes\": {},\n  \"parcels\": {\n    \"allowed\": {\n      \
        \"legal dong renumbered since the edition, checked under its successor\": 3\n    },\n    \
        \"checked\": 4,\n    \"passed\": true,\n    \"unexplained_sample\": [\n      \
        \"9999940400100030000\"\n    ],\n    \"violations\": {}\n  },\n  \"passed\": true,\n  \
        \"sido\": \"99\"\n}\n";
    assert_eq!(String::from_utf8(verdict_file_bytes(&value)?)?, expected);
    Ok(())
}

#[test]
fn a_waiting_gate_names_the_absent_registry() {
    let value = waiting_verdict("edition-b", "99");
    assert_eq!(value["registry"], "wait");
    assert_eq!(value["inputs_absent"], json!([REGISTRY_TABLE]));
    assert_eq!(value["passed"], false);
}
