//! The placement rules on the Spark job's own cases (`infra/lakehouse/spark/tests/test_real_transaction_gold.py`,
//! `DocumentSparkTest`), as far as they place a deal: synthetic codes and PNUs (prefix 99999). The
//! national verdict against the served Gold is the slice's evidence; these hold the rules a verdict
//! on one day's data may not exercise.

use duckdb::Connection;
use serde_json::{json, Value};

use super::*;

pub(crate) const OLD: &str = "9999910100";
pub(crate) const NEW: &str = "9999920100";
pub(crate) const OTHER: &str = "9999910200";
pub(crate) const OLD_NAME: &str = "합성도 합성시 가동";
pub(crate) const NEW_NAME: &str = "합성도 합성시 신구 가동";
pub(crate) const OTHER_NAME: &str = "합성도 합성시 나동";
pub(crate) const INDUSTRIAL: &str = "real_transaction_industrial_trade";

pub(crate) fn pnu(code: &str, main: u32, sub: u32) -> String {
    format!("{code}1{main:04}{sub:04}")
}

pub(crate) fn inference(levels: &Value) -> Value {
    json!({"precision_bar": 0.99, "min_measured_rows": 200, "area_tolerance_m2": 0.05,
           "ownership_window_days": [0, 120], "ownership_causes": ["소유권이전"], "levels": levels})
}

/// One 공장창고 deal of 5억, 500 m² of building, as `DocumentSparkTest.deal` builds it by default.
pub(crate) fn deal(key: &str, address: &str, jibun: &str, day: &str, land: f64) -> String {
    Deal::new(key, address, jibun, day).land(land).row()
}

/// One deal row in the job's `DEAL_COLUMNS` order (`DocumentSparkTest.deal`): a cancelled deal is
/// cancelled on 2098-04-01.
#[derive(Clone)]
pub(crate) struct Deal {
    pub key: String,
    pub address: String,
    pub jibun: String,
    pub day: String,
    pub dataset: String,
    pub amount: i64,
    pub land: f64,
    pub building: Option<f64>,
    pub cancelled: bool,
}

impl Deal {
    pub(crate) fn new(key: &str, address: &str, jibun: &str, day: &str) -> Self {
        Self {
            key: key.to_owned(),
            address: address.to_owned(),
            jibun: jibun.to_owned(),
            day: day.to_owned(),
            dataset: INDUSTRIAL.to_owned(),
            amount: 50000,
            land: 1000.0,
            building: Some(500.0),
            cancelled: false,
        }
    }

    pub(crate) fn land(mut self, land: f64) -> Self {
        self.land = land;
        self
    }

    pub(crate) fn row(&self) -> String {
        let jibun = self.jibun.as_str();
        let bonbun = if jibun.bytes().all(|b| b.is_ascii_digit()) {
            jibun
        } else {
            ""
        };
        let bubun = if bonbun.is_empty() { "" } else { "0" };
        let building = self
            .building
            .map_or_else(|| "NULL".to_owned(), |b| format!("{b:?}"));
        let cancel_date = if self.cancelled {
            "DATE '2098-04-01'"
        } else {
            "NULL"
        };
        format!(
            "('{dataset}', '{ym}', '{key}', DATE '{day}', {amount}, NULL, NULL, NULL, '{address}', '{jibun}', \
             '{bonbun}', '{bubun}', '일반', '공장', NULL, {building}, 'total_floor', {land:?}, 'plottage', NULL, \
             2001, '계획관리', NULL, NULL, '중개거래', {cancelled}, {cancel_date})",
            dataset = self.dataset,
            ym = self.day.get(..7).unwrap_or_default().replace('-', ""),
            key = self.key,
            day = self.day,
            amount = self.amount,
            address = self.address,
            land = self.land,
            cancelled = self.cancelled,
        )
    }
}

/// The tables `place` reads: `deals`, the code pairs, the snapshot, the served `parcels` and their
/// `land`, `ownership` rows; no titles and no lineage.
pub(crate) fn inputs(
    deals: &[String],
    pairs: &[(&str, &str)],
    parcels: &[String],
    land: &[(String, &str, f64)],
    ownership: &[(String, &str, &str)],
) -> anyhow::Result<Connection> {
    let connection = Connection::open_in_memory()?;
    let pairs: Vec<String> = pairs
        .iter()
        .map(|(old, new)| {
            format!("('pair', '{old}', '{new}', 'eupmyeondong', '2098-01-01', 'synthetic')")
        })
        .collect();
    let values = |rows: Vec<String>, empty: &str| {
        if rows.is_empty() {
            format!("SELECT * FROM ({empty}) WHERE false")
        } else {
            format!("VALUES {}", rows.join(", "))
        }
    };
    connection.execute_batch(&format!(
        "CREATE TABLE deals_raw (dataset_slug VARCHAR, deal_ym VARCHAR, row_key VARCHAR, deal_date DATE, \
           deal_amount_10k_krw BIGINT, deposit_10k_krw BIGINT, monthly_rent_10k_krw BIGINT, rent_kind VARCHAR, \
           address_raw VARCHAR, jibun_raw VARCHAR, bonbun VARCHAR, bubun VARCHAR, building_type VARCHAR, \
           building_use VARCHAR, building_name VARCHAR, building_area_m2 DOUBLE, building_area_kind VARCHAR, \
           land_area_m2 DOUBLE, land_area_kind VARCHAR, floor VARCHAR, build_year BIGINT, land_use VARCHAR, \
           jimok VARCHAR, share_dealing_type VARCHAR, dealing_gbn VARCHAR, cancelled BOOLEAN, cancel_date DATE); \
         INSERT INTO deals_raw VALUES {deals}; \
         CREATE TABLE code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
           effective_date VARCHAR, source VARCHAR); \
         INSERT INTO code_changes {pairs}; \
         CREATE TABLE code_names AS SELECT * FROM (VALUES ('{OLD}', '{OLD_NAME}', '존재'), \
           ('{NEW}', '{NEW_NAME}', '존재'), ('{OTHER}', '{OTHER_NAME}', '존재')) AS t(region_cd, full_name, status); \
         CREATE TABLE parcels AS SELECT * FROM (VALUES {parcels}) AS t(pnu); \
         CREATE TABLE land AS SELECT * FROM (VALUES {land}) AS t(pnu, land_category, area_m2, zone_name_1); \
         CREATE TABLE titles (pnu VARCHAR, approval_year INTEGER, floor_area_m2 DOUBLE); \
         CREATE TABLE ownership (pnu VARCHAR, ownership_changed_on VARCHAR, ownership_change_reason VARCHAR); \
         INSERT INTO ownership {ownership}; \
         CREATE TABLE lineage (predecessor_pnu VARCHAR, successor_pnu VARCHAR, relation VARCHAR, grade VARCHAR, \
           evidence_kind VARCHAR, evidence_ref VARCHAR);",
        deals = deals.join(", "),
        pairs = values(pairs, "SELECT * FROM code_changes"),
        parcels = parcels.iter().map(|p| format!("('{p}')")).collect::<Vec<_>>().join(", "),
        land = land
            .iter()
            .map(|(p, category, area)| format!("('{p}', '{category}', {area}, '계획관리지역')"))
            .collect::<Vec<_>>()
            .join(", "),
        ownership = values(
            ownership
                .iter()
                .map(|(p, on, why)| format!("('{p}', '{on}', '{why}')"))
                .collect(),
            "SELECT * FROM ownership"
        ),
    ))?;
    Ok(connection)
}

/// `(row_key, pnu, place_identity_grade)` of one placed row.
type Placed = (String, Option<String>, Option<String>);

/// Every placed row, sorted.
fn placed(connection: &Connection) -> anyhow::Result<Vec<Placed>> {
    Ok(connection
        .prepare("SELECT row_key, pnu, place_identity_grade FROM placed ORDER BY row_key, pnu NULLS LAST")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?)
}

#[test]
fn a_legal_dong_code_change_re_places_a_deal_already_loaded() -> anyhow::Result<()> {
    // 2098-01: 가동 (OLD) became 신구 가동 (NEW); the served parcels carry the new code only. The deal
    // was loaded under the old name before the pair was recorded.
    let factory = pnu(NEW, 12, 0);
    let deals = [deal("a-1", OLD_NAME, "12", "2098-03-10", 1000.0)];
    let land = [(factory.clone(), "공장용지", 1000.0)];
    let before = inputs(&deals, &[], std::slice::from_ref(&factory), &land, &[])?;
    place(&before, &inference(&json!([])))?;
    let rows = placed(&before)?;
    assert!(
        rows.iter()
            .all(|(_, at, _)| at.as_deref() != Some(factory.as_str())),
        "without the pair no row is on the factory parcel: {rows:?}"
    );
    let after = inputs(
        &deals,
        &[(OLD, NEW)],
        std::slice::from_ref(&factory),
        &land,
        &[],
    )?;
    place(&after, &inference(&json!([])))?;
    assert_eq!(
        placed(&after)?,
        [(
            "a-1".to_owned(),
            Some(factory),
            Some("code_superseded".to_owned())
        )]
    );
    Ok(())
}

#[test]
fn an_unmasked_lot_stays_on_its_current_parcel_and_a_masked_one_without_a_level_is_unplaced(
) -> anyhow::Result<()> {
    let (factory, house) = (pnu(OTHER, 5, 0), pnu(OTHER, 7, 0));
    let deals = [
        deal("m-1", OTHER_NAME, "3**", "2098-04-02", 1000.0),
        deal("o-1", OTHER_NAME, "5", "2098-02-01", 800.0),
    ];
    let land = [
        (factory.clone(), "공장용지", 800.0),
        (house.clone(), "대", 300.0),
    ];
    let connection = inputs(&deals, &[], &[factory.clone(), house], &land, &[])?;
    place(&connection, &inference(&json!([])))?;
    let rows = placed(&connection)?;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!((rows[0].0.as_str(), rows[0].1.as_deref()), ("m-1", None));
    assert_eq!(
        (rows[1].0.as_str(), rows[1].1.as_deref()),
        ("o-1", Some(factory.as_str()))
    );
    Ok(())
}

#[test]
fn a_main_lot_group_deal_is_a_row_on_every_parcel_of_the_group() -> anyhow::Result<()> {
    // Root ADR-0405: the evidence leaves 300; the deal is linked to the 본번 300 group (300, 300-1), not
    // to 300 alone, and 301 (another 본번) does not carry it.
    let (pick, neighbour, other) = (pnu(OTHER, 300, 0), pnu(OTHER, 300, 1), pnu(OTHER, 301, 0));
    let deals = [deal("m-1", OTHER_NAME, "3**", "2098-04-02", 800.0)];
    let land = [
        (pick.clone(), "공장용지", 800.0),
        (neighbour.clone(), "공장용지", 500.0),
        (other.clone(), "공장용지", 900.0),
    ];
    let ownership = [(pick.clone(), "2098-05-01", "소유권이전")];
    let connection = inputs(
        &deals,
        &[],
        &[pick.clone(), neighbour.clone(), other],
        &land,
        &ownership,
    )?;
    let levels = json!([{
        "grade": "inferred_same_main_lot_group",
        "evidence": ["lot_range", "zone", "jimok", "land_area", "ownership_change"],
        "measured": {INDUSTRIAL: {"precision": 0.9963, "precision_wilson_low": 0.9953, "rows": 19483}}}]);
    place(&connection, &inference(&levels))?;
    let grade = Some("inferred_same_main_lot_group".to_owned());
    assert_eq!(
        placed(&connection)?,
        [
            ("m-1".to_owned(), Some(pick), grade.clone()),
            ("m-1".to_owned(), Some(neighbour), grade),
        ]
    );
    Ok(())
}
