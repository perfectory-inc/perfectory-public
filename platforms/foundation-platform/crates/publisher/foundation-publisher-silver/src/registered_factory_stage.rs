//! The registered factories' Silver load on `DuckDB` (root ADR-0280 §4, ADR-0559).
//!
//! It was the Spark job `data_go_kr_registered_factory.py`. One Bronze zip is one edition of
//! `silver.registered_factory` (한국산업단지공단 전국등록공장현황): its one data member, CP949, the
//! contract's exact header, a row per data line. Each row gets the PNU its address resolves to
//! through `place_identity_stage` (the 지번 step, then the 도로명 step; each row names the step or
//! why none), and the whole edition is appended once, in a commit that records the zip. A reader
//! takes the rows of the newest edition, so a follow of the 법정동 code changes that moves any
//! line appends the edition once more, every line (root ADR-0329 §3, ADR-0416).
//!
//! The stage's tables: `factory_rows` (an edition as read), `factory_current` (the newest edition
//! the table holds), `factory_link` (what the addresses resolve to), `factory_staged` (what a
//! commit appends).

use std::{collections::BTreeMap, io::Read as _, path::Path};

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;

use crate::{
    data_go_kr_file_data_stage::{
        column_names, count, decode, epoch, literal, member_name, py_strip, PlaceInputs,
        PyCsvRecords, CONTRACT,
    },
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage},
    place_identity_stage,
};

/// The table this load appends to.
pub const TABLE: &str = "silver.registered_factory";
/// The Bronze source its zips land under (root ADR-0014 `{provider}__{dataset}`).
pub const SOURCE_SLUG: &str = "datagokr__registered_factory";
const DATASET_SLUG: &str = "registered_factory";
/// What a link is: the columns a follow compares, null-safe.
const RESOLVED: [&str; 5] = [
    "pnu",
    "pnu_basis",
    "pnu_unresolved_reason",
    "pnu_candidate_count",
    "legal_dong_code",
];

/// The file's entry of the data.go.kr file data contract: its member's name pattern, encoding,
/// header and bounds.
#[derive(Clone, Debug)]
pub struct FactorySource {
    pattern: regex::Regex,
    encoding: String,
    header: Vec<String>,
    min_rows: u64,
    max_shrink_share: f64,
}

impl FactorySource {
    /// The contract compiled into the publisher.
    ///
    /// # Errors
    /// The contract is not the JSON this reads.
    pub fn compiled() -> anyhow::Result<Self> {
        let contract: Value = serde_json::from_str(CONTRACT)?;
        let member = &contract["files"][DATASET_SLUG]["member"];
        let text = |name: &str| {
            member[name]
                .as_str()
                .with_context(|| format!("the file data contract has no member.{name}"))
        };
        Ok(Self {
            pattern: regex::Regex::new(text("name_pattern")?)?,
            encoding: text("encoding")?.to_owned(),
            header: member["header"]
                .as_array()
                .context("the file data contract has no member.header")?
                .iter()
                .map(|cell| cell.as_str().map(str::to_owned).context("a header cell"))
                .collect::<anyhow::Result<_>>()?,
            min_rows: member["bounds"]["min_rows"]
                .as_u64()
                .context("the file data contract has no bounds.min_rows")?,
            max_shrink_share: member["bounds"]["max_shrink_share"]
                .as_f64()
                .context("the file data contract has no bounds.max_shrink_share")?,
        })
    }

    /// Refuses an edition under the row floor, or shrunk past its bound against the `previous`
    /// rows of the newest edition the table holds (none while it holds none).
    ///
    /// # Errors
    /// The edition is out of its bounds.
    pub fn check_bounds(&self, rows: u64, previous: u64) -> anyhow::Result<()> {
        ensure!(
            rows >= self.min_rows,
            "{rows} rows is under the floor of {}",
            self.min_rows
        );
        #[allow(clippy::cast_precision_loss)]
        let shrunk =
            previous > 0 && (rows as f64) < (previous as f64) * (1.0 - self.max_shrink_share);
        ensure!(
            !shrunk,
            "{rows} rows shrank past {:.0}% of the last loaded {previous}",
            self.max_shrink_share * 100.0
        );
        Ok(())
    }
}

/// An edition as read from its zip.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Edition {
    /// The 기준일 its data member is named by (`YYYYMMDD`): the edition id.
    pub snapshot_id: String,
    /// Its data rows.
    pub rows: u64,
}

/// `whole`: the digits of a cell as a number, or none.
fn whole(cell: &str) -> anyhow::Result<Option<i64>> {
    let text = py_strip(cell);
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(None);
    }
    text.parse()
        .map(Some)
        .with_context(|| format!("{text} is no 64-bit number"))
}

/// `text_or_none`: a cell without its outer white space, or none when nothing is left.
fn text_or_none(cell: &str) -> Option<&str> {
    Some(py_strip(cell)).filter(|text| !text.is_empty())
}

/// Reads the zip's one data member into `factory_rows`: a row per data line that is not blank,
/// numbered from 2 in the order kept, with the address as written (`address`) for the joins.
///
/// # Errors
/// The zip holds no member the contract names or more than one, the member does not decode
/// strictly, or its header is not the contract's.
pub fn read_edition(
    stage: &DuckDbParquetStage,
    source: &FactorySource,
    path: &Path,
) -> anyhow::Result<Edition> {
    let mut archive = zip::ZipArchive::new(std::fs::File::open(path)?)?;
    let mut found = Vec::new();
    for index in 0..archive.len() {
        let name = member_name(&archive.by_index(index)?)?;
        if let Some(captures) = source.pattern.captures(&name) {
            let snapshot = captures
                .get(1)
                .context("the member pattern captures no 기준일")?;
            found.push((index, snapshot.as_str().to_owned()));
        }
    }
    ensure!(
        found.len() == 1,
        "expected one data member matching {}, found {}",
        source.pattern,
        found.len()
    );
    let (index, snapshot_id) = found.remove(0);
    let mut bytes = Vec::new();
    archive.by_index(index)?.read_to_end(&mut bytes)?;
    let text = decode(&bytes, &source.encoding)?;
    drop(bytes);

    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE factory_rows (_row BIGINT NOT NULL, serial_no BIGINT, company_name VARCHAR, \
         complex_name VARCHAR, products VARCHAR, factory_address_raw VARCHAR, address VARCHAR NOT NULL, \
         source_line_number INTEGER NOT NULL)",
    )?;
    let mut records = PyCsvRecords { rest: &text };
    let header = records
        .next()
        .unwrap_or_default()
        .iter()
        .map(|cell| py_strip(cell).trim_start_matches('\u{feff}').to_owned())
        .collect::<Vec<_>>();
    ensure!(header == source.header, "the header is not the contract's");
    let mut appender = connection.appender("factory_rows")?;
    let mut rows = 0_i64;
    for record in records {
        if record.iter().all(|cell| py_strip(cell).is_empty()) {
            continue;
        }
        let cell = |at: usize| record.get(at).map_or("", String::as_str);
        rows += 1;
        // Line 2 is the first data line kept.
        let line = i32::try_from(rows + 1)?;
        appender.append_row(duckdb::params![
            rows,
            whole(cell(0))?,
            text_or_none(cell(1)),
            text_or_none(cell(2)),
            text_or_none(cell(3)),
            text_or_none(cell(4)),
            cell(4),
            line
        ])?;
    }
    appender.flush()?;
    Ok(Edition {
        snapshot_id,
        rows: u64::try_from(rows)?,
    })
}

/// `factory_link(_row, pnu, pnu_basis, pnu_unresolved_reason, pnu_candidate_count,
/// legal_dong_code)` of the rows of `input` (`_row`, `address`): `address_pnu.resolve` over what
/// each address offers.
///
/// The place-identity reference tables are on the stage (`place_identity_stage::load_reference`).
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn link(stage: &DuckDbParquetStage, input: &str, place: PlaceInputs<'_>) -> anyhow::Result<()> {
    let connection = stage.connection();
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE place_rows AS SELECT _row, NULL::VARCHAR AS pnu, address FROM {input}"
    ))?;
    place_identity_stage::offers(stage)?;
    place_identity_stage::read_served(stage, place.pins, place.title_contract)?;
    place_identity_stage::place_offered(stage)?;
    connection
        .execute_batch(
            "CREATE OR REPLACE TEMP TABLE factory_link AS \
             SELECT a._row, a.pnu, a.pnu_basis, a.reason AS pnu_unresolved_reason, \
                    a.candidate_count AS pnu_candidate_count, o.jibun_code AS legal_dong_code \
             FROM place_address_result a JOIN place_offers o USING (_row)",
        )
        .context("DuckDB failed to link the factories")
}

/// `factory_staged`: the edition (`factory_rows` and their `factory_link`) as the table's columns.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn staged_edition(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    object_key: &str,
    snapshot_id: &str,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE factory_staged AS SELECT {columns} FROM (\
             SELECT r.* EXCLUDE (_row, address), l.* EXCLUDE (_row), {object} AS source_record_id, \
                    {snapshot} AS source_snapshot_id, {at} AS ingested_at_utc \
             FROM factory_rows r JOIN factory_link l USING (_row))",
            columns = column_names(contract).join(", "),
            object = literal(object_key),
            snapshot = literal(snapshot_id),
            at = epoch(ingested_at),
        ))
        .context("DuckDB failed to stage the edition")
}

/// What an edition's rows resolved to (`summary_of`).
#[derive(Clone, Debug, PartialEq)]
pub struct LinkSummary {
    /// Rows of the edition.
    pub rows: u64,
    /// Rows per step; `unresolved` for the rows no step resolved.
    pub by_basis: BTreeMap<String, u64>,
    /// Rows per reason, of the rows no step resolved.
    pub unresolved_reasons: BTreeMap<String, u64>,
    /// The share that got a PNU, to four places; none for an edition of no rows.
    pub match_rate: Option<f64>,
}

/// The link summary of `factory_staged`.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn link_summary(stage: &DuckDbParquetStage) -> anyhow::Result<LinkSummary> {
    let counts = |column: &str, filter: &str| -> anyhow::Result<BTreeMap<String, u64>> {
        let mut found = stage.connection().prepare(&format!(
            "SELECT {column}, count(*) FROM factory_staged WHERE {filter} GROUP BY 1 ORDER BY 1"
        ))?;
        let rows = found
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(name, rows)| Ok((name, u64::try_from(rows)?)))
            .collect()
    };
    let by_basis = counts("coalesce(nullif(pnu_basis, ''), 'unresolved')", "TRUE")?;
    let unresolved_reasons = counts(
        "pnu_unresolved_reason",
        "pnu_unresolved_reason IS NOT NULL AND pnu_unresolved_reason <> ''",
    )?;
    let rows = count(stage, "factory_staged")?;
    let unresolved = by_basis.get("unresolved").copied().unwrap_or(0);
    #[allow(clippy::cast_precision_loss)]
    let match_rate = (rows > 0)
        .then(|| ((rows - unresolved) as f64 / rows as f64 * 10_000.0).round() / 10_000.0);
    Ok(LinkSummary {
        rows,
        by_basis,
        unresolved_reasons,
        match_rate,
    })
}

/// The newest edition the table holds at `head` as `factory_current`; returns how many rows.
///
/// Every column of the contract, of the rows of the newest (기준일, load instant), so a reload of
/// a 기준일 outranks its earlier load (root ADR-0286 §3). No rows for a table with no snapshot.
///
/// # Errors
/// `DuckDB` fails the read.
pub fn current(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
) -> anyhow::Result<u64> {
    let columns = column_names(contract).join(", ");
    let connection = stage.connection();
    let query = match head {
        None => {
            let typed = contract
                .columns
                .iter()
                .map(|column| {
                    let sql_type = match column.logical_type {
                        "int" => "INTEGER",
                        "long" => "BIGINT",
                        "timestamp" => "TIMESTAMPTZ",
                        _ => "VARCHAR",
                    };
                    format!("CAST(NULL AS {sql_type}) AS {}", column.name)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("SELECT {typed} WHERE false")
        }
        Some(head) => {
            let pinned = crate::duckdb_parquet_stage::table_at(contract.table_name, head);
            format!(
                "WITH versions AS (SELECT {columns} FROM {source}), \
                 newest AS (SELECT source_snapshot_id AS edition, ingested_at_utc AS loaded FROM versions \
                            ORDER BY 1 DESC NULLS LAST, 2 DESC NULLS LAST LIMIT 1) \
                 SELECT v.* FROM versions v, newest n \
                 WHERE v.source_snapshot_id IS NOT DISTINCT FROM n.edition \
                   AND v.ingested_at_utc IS NOT DISTINCT FROM n.loaded",
                source = with_contract_columns(connection, contract, &pinned)?,
            )
        }
    };
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE factory_current AS {query}"
        ))
        .context("DuckDB failed to read the newest edition")?;
    count(stage, "factory_current")
}

/// What a follow found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Followed {
    /// Lines of the newest edition a code change can move.
    pub checked: u64,
    /// Of them, lines whose link differs now.
    pub relinked: u64,
    /// Rows `factory_staged` holds: the whole edition when any line moved, else none.
    pub appended: u64,
}

/// Links again the lines of `factory_current` a code change can move.
///
/// When any link moved, `factory_staged` is the edition once more: every line, the moved ones
/// re-linked and the rest as they were, at this run's instant.
///
/// `prefixes_known` says `file_prefixes` holds the changed prefixes
/// (`data_go_kr_file_data_stage::changed_prefixes`); without them every line is linked again. A
/// line is affected when its 법정동 code or its PNU is under a changed prefix, or it has no PNU.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn follow(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    prefixes_known: bool,
    place: PlaceInputs<'_>,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<Followed> {
    let connection = stage.connection();
    let columns = column_names(contract).join(", ");
    let at = epoch(ingested_at);
    let under = |column: &str| {
        format!("EXISTS (SELECT 1 FROM file_prefixes p WHERE starts_with(c.{column}, p.prefix))")
    };
    let filter = if prefixes_known {
        format!(
            "c.pnu IS NULL OR {} OR {}",
            under("legal_dong_code"),
            under("pnu")
        )
    } else {
        "TRUE".to_owned()
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE factory_follow_in AS \
         SELECT row_number() OVER () AS _row, c.*, coalesce(c.factory_address_raw, '') AS address \
         FROM factory_current c WHERE {filter};
         CREATE OR REPLACE TEMP TABLE factory_staged AS SELECT {columns} FROM factory_current WHERE false;"
    ))?;
    let checked = count(stage, "factory_follow_in")?;
    if checked == 0 {
        return Ok(Followed {
            checked,
            relinked: 0,
            appended: 0,
        });
    }
    link(stage, "factory_follow_in", place)?;
    let differs = RESOLVED
        .iter()
        .map(|column| format!("i.{column} IS DISTINCT FROM l.{column}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let replaced = RESOLVED.join(", ");
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE factory_moved AS SELECT {columns} FROM (\
             SELECT i.* EXCLUDE (_row, address, {replaced}, ingested_at_utc), l.* EXCLUDE (_row), {at} AS ingested_at_utc \
             FROM factory_follow_in i JOIN factory_link l USING (_row) WHERE {differs})"
        ))
        .context("DuckDB failed to compare the links")?;
    let relinked = count(stage, "factory_moved")?;
    if relinked > 0 {
        connection
            .execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE factory_staged AS \
                 SELECT {columns} FROM (SELECT c.* REPLACE ({at} AS ingested_at_utc) FROM factory_current c \
                   WHERE NOT EXISTS (SELECT 1 FROM factory_moved m WHERE m.source_record_id = c.source_record_id \
                                     AND m.source_line_number = c.source_line_number)) \
                 UNION ALL SELECT {columns} FROM factory_moved"
            ))
            .context("DuckDB failed to stage the edition again")?;
    }
    Ok(Followed {
        checked,
        relinked,
        appended: count(stage, "factory_staged")?,
    })
}

#[cfg(test)]
mod tests;
