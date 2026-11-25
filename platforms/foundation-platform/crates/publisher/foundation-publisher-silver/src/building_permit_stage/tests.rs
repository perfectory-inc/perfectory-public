//! The 건축인허가 rules on synthetic inputs (PNUs under `99999`): permits newest first by their five
//! dates, a tied repeat once, blanks null, a permit's parking by key, an unserved or unplaced permit
//! left out, the bound, a renumbered code on the served parcel, each other file a `{total, records}`
//! list or null, an inspection on the parcel of the one 표제부 its key names, the bytes Spark writes,
//! the partial diff a changed parking row or 표제부 reaches, and the document's fields as the schema
//! registry records them. The national judgement against the Spark job was its takeover verdict.

use std::collections::BTreeMap;

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::*;
use crate::kind_versions_stage::read_for_test;

const EDITION: &str = "synthetic__parcel-209906";
const FACTORY: &str = "9999910100100010000";
const WAREHOUSE: &str = "9999910100100020000";
const UNSERVED: &str = "9999910100100030000";
/// A served parcel under the code a reorganization renumbered 10200 into.
const MOVED: &str = "9999910300100040000";

fn quoted(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |text| format!("'{text}'"))
}

/// The inputs: each Silver holds the rows `rows` gives it (none when absent), every one current.
#[derive(Default)]
struct Inputs(BTreeMap<&'static str, Vec<String>>);

impl Inputs {
    fn with(mut self, table: &'static str, row: String) -> Self {
        self.0.entry(table).or_default().push(row);
        self
    }

    fn load(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, \
               source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
             INSERT INTO silver_parcel_boundaries VALUES \
               ('{FACTORY}', NULL, '{EDITION}', NULL), ('{WAREHOUSE}', NULL, '{EDITION}', NULL), \
               ('{MOVED}', NULL, '{EDITION}', NULL), ('{UNSERVED}', NULL, 'synthetic__parcel-209903', NULL); \
             CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
               new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
             INSERT INTO reference_legal_dong_code_change VALUES \
               ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x');"
        ))?;
        let mut tables: Vec<(&str, Vec<&str>, &str)> = vec![
            (
                PERMIT_SOURCE,
                [&["mgmt_key", "pnu"][..], &RECORD_FIELDS].concat(),
                "vintage VARCHAR, ingested_at_utc TIMESTAMP",
            ),
            (
                PARKING_SOURCE,
                [&["mgmt_key"][..], &PARKING_FIELDS].concat(),
                "vintage VARCHAR, ingested_at_utc TIMESTAMP",
            ),
            (
                TITLE_SOURCE,
                vec!["mgm_bldrgst_pk", "pnu"],
                "valid_from_utc TIMESTAMP, ingested_at_utc TIMESTAMP",
            ),
        ];
        for detail in &DETAILS {
            let own: &[&str] = if detail.source == INSPECTION_SOURCE {
                &INSPECTION_COLUMNS
            } else {
                &["mgmt_key", "pnu"]
            };
            let mut columns = own.to_vec();
            columns.extend(detail.fields);
            tables.push((
                detail.source,
                columns,
                "vintage VARCHAR, ingested_at_utc TIMESTAMP",
            ));
        }
        for (table, columns, versions) in tables {
            let name = table.replace('.', "_");
            let text = columns
                .iter()
                .map(|column| format!("{column} VARCHAR"))
                .collect::<Vec<_>>()
                .join(", ");
            connection.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {name} ({text}, {versions}, row_state VARCHAR);"
            ))?;
            if let Some(rows) = self.0.get(table) {
                connection
                    .execute_batch(&format!("INSERT INTO {name} VALUES {};", rows.join(", ")))?;
            }
        }
        Ok(())
    }
}

/// A permit's row: `(key, pnu, fields…, vintage, ingested, row_state)`.
fn permit(
    key: &str,
    pnu: Option<&str>,
    purpose: &str,
    plot: &str,
    started: &str,
    approved: &str,
) -> String {
    format!(
        "('{key}', {}, '신축', '{purpose}', '{plot}', '400.25', '800', '', '', '', '{started}', \
         '{approved}', '202610', TIMESTAMP '2099-01-02', NULL)",
        quoted(pnu)
    )
}

fn parking(
    key: &str,
    indoor: Option<&str>,
    outdoor: Option<&str>,
    mechanical: Option<&str>,
) -> String {
    format!(
        "('{key}', {}, {}, {}, '202610', TIMESTAMP '2099-01-02', NULL)",
        quoted(indoor),
        quoted(outdoor),
        quoted(mechanical)
    )
}

/// A row of a detail file on `pnu`, every field `value`.
fn detail(source: &str, key: &str, pnu: &str, value: &str) -> String {
    let fields = DETAILS
        .iter()
        .find(|detail| detail.source == source)
        .map_or(0, |detail| detail.fields.len());
    let values = vec![format!("'{value}'"); fields].join(", ");
    format!("('{key}', '{pnu}', {values}, '202610', TIMESTAMP '2099-01-02', NULL)")
}

fn inspection(key: &str, sigungu: &str, bonbeon: &str, register: &str, processed: &str) -> String {
    format!(
        "('{key}', '{sigungu}', '10100', '{bonbeon}', '0000', '{register}', '기관', '20990101', \
         '{processed}', '202610', TIMESTAMP '2099-01-02', NULL)"
    )
}

fn title(key: &str, pnu: &str) -> String {
    format!("('{key}', '{pnu}', TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-02', NULL)")
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, String, i64, String)>> {
    Ok(connection
        .prepare(
            "SELECT pnu, building_permits_json, building_permit_total, row_digest FROM built ORDER BY pnu",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

fn build_all(
    connection: &Connection,
    inputs: &Inputs,
) -> anyhow::Result<Vec<(String, String, i64, String)>> {
    inputs.load(connection)?;
    read_for_test(&BUILDING_PERMITS, connection, EDITION, "")?;
    BUILDING_PERMITS.build(
        connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    crate::kind_versions_stage::ensure_no_commit_tables(connection)?;
    built(connection)
}

fn floors() -> &'static str {
    "silver.building_permit_floor_overview"
}

/// A permit record as Spark writes it: the fields of `permit()` and its parking fields `parking`.
fn permit_record(
    purpose: &str,
    plot: &str,
    started: &str,
    approved: &str,
    parking: &str,
) -> String {
    let text = |value: &str| {
        if value.is_empty() {
            "null".to_owned()
        } else {
            format!("\"{value}\"")
        }
    };
    format!(
        "{{\"arch_kind_name\":\"신축\",\"main_purpose_name\":\"{purpose}\",\"plot_area_m2\":{},\
         \"building_area_m2\":\"400.25\",\"total_floor_area_m2\":\"800\",\"permit_date\":null,\
         \"start_scheduled_date\":null,\"start_delayed_date\":null,\"start_actual_date\":{},\
         \"use_approval_date\":{},{parking}}}",
        text(plot),
        text(started),
        text(approved)
    )
}

/// Every detail list of a document in its order: the ones `details` gives, the others null.
fn details_json(details: &[(&str, String)]) -> String {
    DETAILS
        .iter()
        .map(|detail| {
            let value = details
                .iter()
                .find(|(key, _)| *key == detail.key)
                .map_or_else(|| "null".to_owned(), |(_, value)| value.clone());
            format!("\"{}\":{value}", detail.key)
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
fn a_parcel_s_permits_are_spark_s_bytes_newest_first_with_their_parking() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let inputs = Inputs::default()
        .with(
            PERMIT_SOURCE,
            permit("k1", Some(FACTORY), "공장", " 1000.5 ", "", "20990101"),
        )
        // The same current row twice (a tie the contract keeps): one permit.
        .with(
            PERMIT_SOURCE,
            permit("k1", Some(FACTORY), "공장", " 1000.5 ", "", "20990101"),
        )
        .with(
            PERMIT_SOURCE,
            permit("k2", Some(FACTORY), "창고/시설", "", "20990505", ""),
        )
        // An unserved parcel and an unplaced permit: no document.
        .with(
            PERMIT_SOURCE,
            permit("k3", Some(UNSERVED), "공장", "1", "", "20990101"),
        )
        .with(
            PERMIT_SOURCE,
            permit("k4", None, "공장", "1", "", "20990101"),
        )
        .with(PARKING_SOURCE, parking("k1", Some(" 3 "), None, Some("0")))
        .with(
            floors(),
            "('f1', '9999910100100010000', '지상', '1', '공장', NULL, '800', '신축', '202610', \
             TIMESTAMP '2099-01-02', NULL)"
                .to_owned(),
        )
        .with(
            "silver.building_permit_site_location",
            detail("silver.building_permit_site_location", "s1", WAREHOUSE, "Y"),
        );
    let rows = build_all(&connection, &inputs)?;
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(pnus, [FACTORY, WAREHOUSE]);
    // k2 (started 2099-05-05) before k1 (approved 2099-01-01); a '/' written as it is (Jackson's).
    let factory = format!(
        "{{\"records\":[{},{}],{}}}",
        permit_record(
            "창고/시설",
            "",
            "20990505",
            "",
            "\"indoor_self_parking_count\":null,\"outdoor_self_parking_count\":null,\
             \"indoor_mechanical_parking_count\":null"
        ),
        permit_record(
            "공장",
            "1000.5",
            "",
            "20990101",
            "\"indoor_self_parking_count\":\"3\",\"outdoor_self_parking_count\":null,\
             \"indoor_mechanical_parking_count\":\"0\""
        ),
        details_json(&[(
            "floor_overviews",
            "{\"total\":1,\"records\":[{\"floor_kind_name\":\"지상\",\"floor_no\":\"1\",\
             \"main_purpose_name\":\"공장\",\"structure_name\":null,\"floor_area_m2\":\"800\",\
             \"arch_kind_name\":\"신축\"}]}"
                .to_owned()
        )])
    );
    assert_eq!(rows[0].1, factory);
    assert_eq!(rows[0].2, 2);
    // Spark's row_digest: to_json of the content columns, the null attached_via left out.
    let content = format!(
        "{{\"pnu\":\"{FACTORY}\",\"building_permits_json\":{},\"building_permit_total\":2}}",
        serde_json::to_string(&factory)?
    );
    assert_eq!(
        rows[0].3,
        format!("{:x}", Sha256::digest(content.as_bytes()))
    );
    // A parcel with a detail row only: no permit, a document all the same.
    let warehouse: serde_json::Value = serde_json::from_str(&rows[1].1)?;
    assert_eq!(
        (
            warehouse["records"].clone(),
            rows[1].2,
            warehouse["site_locations"]["total"].clone()
        ),
        (serde_json::json!([]), 0, serde_json::json!(1))
    );
    Ok(())
}

#[test]
fn a_parcel_with_more_permits_than_the_bound_keeps_the_newest_and_counts_all() -> anyhow::Result<()>
{
    let connection = Connection::open_in_memory()?;
    let inputs = (0..RECORD_BOUND + 4).fold(Inputs::default(), |inputs, n| {
        inputs.with(
            PERMIT_SOURCE,
            permit(
                &format!("k{n:03}"),
                Some(FACTORY),
                "공장",
                "1",
                "",
                &format!("2000{:04}", 101 + n),
            ),
        )
    });
    let rows = build_all(&connection, &inputs)?;
    let document: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    let records = document["records"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        (records.len(), rows[0].2),
        (RECORD_BOUND as usize, i64::from(RECORD_BOUND + 4))
    );
    assert_eq!(
        records[0]["use_approval_date"],
        format!("2000{:04}", 101 + RECORD_BOUND + 3)
    );
    Ok(())
}

#[test]
fn a_row_under_a_renumbered_code_lands_on_the_served_parcel() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let inputs = Inputs::default()
        .with(
            PERMIT_SOURCE,
            permit(
                "k1",
                Some("9999910200100040000"),
                "공장",
                "1",
                "",
                "20990101",
            ),
        )
        .with(floors(), detail(floors(), "f1", "9999910200100040000", "1"));
    let rows = build_all(&connection, &inputs)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, MOVED);
    let document: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    assert_eq!(document["floor_overviews"]["total"], 1);
    Ok(())
}

/// 99999-123 names the 표제부 `<4>1123` on the factory, which starts with 99999 (linked); 99999-77
/// names two (no link); 88888-5 names one whose PNU neither starts with 88888 nor has this row's
/// 법정동·번·지 (no link); 88888-6 names one that has them (linked); 99999-9 names none.
fn inspections() -> Inputs {
    Inputs::default()
        .with(
            INSPECTION_SOURCE,
            inspection("a", "99999", "0001", "99999-123", "20990102"),
        )
        .with(
            INSPECTION_SOURCE,
            inspection("b", "99999", "0001", "99999-77", ""),
        )
        .with(
            INSPECTION_SOURCE,
            inspection("c", "88888", "0009", "88888-5", ""),
        )
        .with(
            INSPECTION_SOURCE,
            inspection("d", "88888", "0002", "88888-6", ""),
        )
        .with(
            INSPECTION_SOURCE,
            inspection("e", "99999", "0001", "99999-9", ""),
        )
        .with(TITLE_SOURCE, title("00011123", FACTORY))
        .with(TITLE_SOURCE, title("0001177", FACTORY))
        .with(TITLE_SOURCE, title("0002177", WAREHOUSE))
        .with(TITLE_SOURCE, title("000115", FACTORY))
        .with(TITLE_SOURCE, title("000116", WAREHOUSE))
}

#[test]
fn an_inspection_reaches_the_one_title_its_key_names() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let rows = build_all(&connection, &inspections())?;
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(pnus, [FACTORY, WAREHOUSE]);
    let factory: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    assert_eq!(
        factory["regular_inspections"],
        serde_json::json!({"total": 1, "records": [{"inspection_agency_name": "기관",
            "inspection_start_date": "20990101", "processed_date": "20990102"}]})
    );
    let warehouse: serde_json::Value = serde_json::from_str(&rows[1].1)?;
    assert_eq!(
        warehouse["regular_inspections"]["records"][0]["processed_date"],
        serde_json::Value::Null
    );
    Ok(())
}

#[test]
fn a_partial_run_follows_a_changed_parking_row_and_a_moved_title() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let base = inspections()
        .with(
            PERMIT_SOURCE,
            permit("k1", Some(FACTORY), "공장", "1", "", "20990101"),
        )
        .with(PARKING_SOURCE, parking("k1", Some("3"), None, None));
    base.load(&connection)?;
    read_for_test(&BUILDING_PERMITS, &connection, EDITION, "_old")?;
    // k1's parking changed; the 표제부 inspection d names moved to a parcel it no longer matches.
    let mut moved = inspections()
        .with(
            PERMIT_SOURCE,
            permit("k1", Some(FACTORY), "공장", "1", "", "20990101"),
        )
        .with(PARKING_SOURCE, parking("k1", Some("4"), None, None));
    if let Some(titles) = moved.0.get_mut(TITLE_SOURCE) {
        titles.retain(|row| !row.starts_with("('000116'"));
        titles.push(title("000116", MOVED));
    }
    moved.load(&connection)?;
    read_for_test(&BUILDING_PERMITS, &connection, EDITION, "")?;
    assert_eq!(
        crate::kind_versions_stage::changed_for_test(&BUILDING_PERMITS, &connection)?,
        2
    );
    BUILDING_PERMITS.build(
        &connection,
        false,
        "999990000000000002",
        "2099-01-04T00:00:00Z",
    )?;
    // The warehouse is rebuilt and has nothing left: the diff marks it removed.
    let rows = built(&connection)?;
    assert_eq!(rows.len(), 1);
    let factory: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    assert_eq!(factory["records"][0]["indoor_self_parking_count"], "4");
    // Nothing changed: no PNU to rebuild.
    read_for_test(&BUILDING_PERMITS, &connection, EDITION, "_old")?;
    assert_eq!(
        crate::kind_versions_stage::changed_for_test(&BUILDING_PERMITS, &connection)?,
        0
    );
    Ok(())
}

/// The lane's `spark_held_test` (root ADR-0418 §8c): the document this stage writes carries exactly
/// the fields of the subject's registered schema, as the deleted Spark job's test held it.
#[test]
fn a_document_carries_exactly_the_registry_s_fields() -> anyhow::Result<()> {
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../contracts/schema-registry/parcel_building_permits_by_pnu/1/4.schema.json"
    ))?;
    let mut recorded = std::collections::BTreeSet::new();
    crate::building_energy_stage::tests::schema_paths(
        &schema["properties"]["building_permits"],
        "building_permits",
        &mut recorded,
    );
    // A permit with its parking, a row of every other file of the family and a linked inspection,
    // all on the factory: every list holds a record.
    let mut inputs = inspections()
        .with(
            PERMIT_SOURCE,
            permit("k1", Some(FACTORY), "공장", "1", "", "20990101"),
        )
        .with(PARKING_SOURCE, parking("k1", Some("3"), None, None));
    for file in DETAILS
        .iter()
        .filter(|file| file.source != INSPECTION_SOURCE)
    {
        inputs = inputs.with(file.source, detail(file.source, "d1", FACTORY, "1"));
    }
    let connection = Connection::open_in_memory()?;
    let rows = build_all(&connection, &inputs)?;
    assert_eq!(rows[0].0, FACTORY);
    let mut written = std::collections::BTreeSet::new();
    crate::building_energy_stage::tests::json_paths(
        &serde_json::from_str(&rows[0].1)?,
        "building_permits",
        &mut written,
    );
    assert_eq!(written, recorded);
    Ok(())
}
