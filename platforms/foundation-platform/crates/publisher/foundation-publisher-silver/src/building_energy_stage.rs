//! The parcel building energy kind's Gold in `DuckDB` (root ADR-0398, ADR-0564 §6): the twin of
//! `building_energy_silver_to_gold.py`.
//!
//! A PNU's document is its last [`MONTH_BOUND`] months of 전기 and 가스 use, newest first: per month
//! the parcel's summed 사용량 of each energy (`decimal(38,3)`, the rows the provider wrote as digits
//! with an optional fraction), how many rows each energy had, the unit of 전기, the newest month and
//! how many rows of the kept months could not be read as a number. Only PNUs of the served parcel
//! edition have a document, a row under a renumbered code is read under the served one.
//!
//! The bytes are Spark's. Its `to_json` writes a `decimal(38,3)` as Java's `BigDecimal` text, every
//! digit of the scale (`123.450`), which `DuckDB`'s `to_json` does not (it writes a number), so the
//! months are written from `DuckDB`'s decimal text, field by field in Spark's order, a null as
//! `null`. `row_digest` is the SHA-256 of `to_json` of the content columns with the null
//! `attached_via` left out.

use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, load_current, load_served_and_renumbering, renumbering_join,
    served_pnu, KindInputs, KindVersionsStage, CODE_CHANGE_SOURCE, PARCEL_SOURCE, SHARED_SQL,
};
use crate::parcel_nearby_stage::Source;

/// The 전기 Silver.
pub const ELECTRICITY_SOURCE: &str = "silver.building_energy_monthly_electricity";
/// The 가스 Silver.
pub const GAS_SOURCE: &str = "silver.building_energy_monthly_gas";
/// Each energy's Silver and its name in the document.
pub const ENERGIES: [(&str, &str); 2] = [(ELECTRICITY_SOURCE, "electricity"), (GAS_SOURCE, "gas")];
/// A year of months: the panel's chart.
pub const MONTH_BOUND: u32 = 12;
/// data.go.kr 15135963 (건축HUB 건물에너지정보 서비스): 제공단위 `KWh` for 전기 사용량.
pub const ELECTRICITY_UNIT: &str = "kWh";
/// A 사용량 the provider writes as digits with an optional fraction (Spark's `NUMBER`).
pub const NUMBER: &str = r"^\s*-?[0-9]+(\.[0-9]+)?\s*$";
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-energy-duckdb-v1";

/// `<energy>_rows<suffix>`: the current rows, re-keyed to the served code, of served PNUs only.
const ROWS_SQL: &str = "\
CREATE OR REPLACE TABLE @energy@_rows@suffix@ AS \
SELECT * FROM ( \
  SELECT @pnu@ AS pnu, m.use_ym, m.usage_amount FROM current_@energy@@suffix@ m @join@ \
  WHERE m.pnu IS NOT NULL) \
WHERE pnu IN (SELECT pnu FROM served@suffix@)";

/// One energy's months: its summed use, its rows and its unreadable rows per (pnu, month).
const MONTHLY_SQL: &str = "\
monthly_@energy@ AS ( \
  SELECT pnu, use_ym, \
    sum(CASE WHEN regexp_matches(usage_amount, '@number@') \
      THEN CAST(regexp_replace(usage_amount, '^\\s+|\\s+$', '', 'g') AS DECIMAL(38, 3)) END) AS @energy@_usage, \
    count(*) AS @energy@_rows, \
    sum(CASE WHEN regexp_matches(usage_amount, '@number@') THEN 0 ELSE 1 END) AS @energy@_unparsed \
  FROM @energy@_rows WHERE @targets@ GROUP BY pnu, use_ym)";

/// The months of the PNUs of `targets` folded into one document each, inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH @monthly@, \
months AS ( \
  SELECT coalesce(e.pnu, g.pnu) AS pnu, coalesce(e.use_ym, g.use_ym) AS use_ym, \
    e.electricity_usage, e.electricity_rows, e.electricity_unparsed, \
    g.gas_usage, g.gas_rows, g.gas_unparsed \
  FROM monthly_electricity e FULL OUTER JOIN monthly_gas g ON e.pnu = g.pnu AND e.use_ym = g.use_ym), \
kept AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY use_ym DESC NULLS LAST) AS n FROM months \
  QUALIFY n <= @bound@), \
per_pnu AS ( \
  SELECT pnu, \
    string_agg('{\"use_ym\":' || coalesce(to_json(use_ym)::VARCHAR, 'null') \
      || ',\"electricity_usage\":' || coalesce(CAST(electricity_usage AS VARCHAR), 'null') \
      || ',\"electricity_rows\":' || CAST(CAST(coalesce(electricity_rows, 0) AS BIGINT) AS VARCHAR) \
      || ',\"gas_usage\":' || coalesce(CAST(gas_usage AS VARCHAR), 'null') \
      || ',\"gas_rows\":' || CAST(CAST(coalesce(gas_rows, 0) AS BIGINT) AS VARCHAR) || '}', ',' ORDER BY n) AS months, \
    max(use_ym) AS latest_use_ym, \
    CAST(sum(coalesce(electricity_unparsed, 0)) + sum(coalesce(gas_unparsed, 0)) AS BIGINT) \
      AS unparsed_usage_rows \
  FROM kept GROUP BY pnu), \
documents AS ( \
  SELECT pnu, '{\"months\":[' || months || '],\"electricity_unit\":' || to_json('@unit@')::VARCHAR \
    || ',\"latest_use_ym\":' || coalesce(to_json(latest_use_ym)::VARCHAR, 'null') \
    || ',\"unparsed_usage_rows\":' || CAST(unparsed_usage_rows AS VARCHAR) || '}' AS energy_json \
  FROM per_pnu) \
SELECT pnu, energy_json, CAST(NULL AS VARCHAR) AS attached_via, \
  sha256(to_json({'pnu': pnu, 'energy_json': energy_json})::VARCHAR) AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, energy_json VARCHAR, \
    attached_via VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// The energy kind.
#[derive(Clone, Copy, Debug)]
pub struct EnergyKind;

/// The one energy kind.
pub const ENERGY: EnergyKind = EnergyKind;

impl KindVersionsStage for EnergyKind {
    fn section_column(&self) -> &'static str {
        "energy_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![
            PARCEL_SOURCE,
            ELECTRICITY_SOURCE,
            GAS_SOURCE,
            CODE_CHANGE_SOURCE,
        ]
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        for text in [
            RULES_VERSION,
            NUMBER,
            ELECTRICITY_UNIT,
            ROWS_SQL,
            MONTHLY_SQL,
            DOCUMENT_SQL,
        ]
        .into_iter()
        .chain(SHARED_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        hash.update(MONTH_BOUND.to_le_bytes());
        format!("{:x}", hash.finalize())
    }

    fn load_inputs(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        edition: &str,
        suffix: &str,
    ) -> anyhow::Result<KindInputs> {
        let mut read_millis = std::collections::BTreeMap::new();
        let served_parcels =
            load_served_and_renumbering(connection, source, edition, suffix, &mut read_millis)?;
        let mut rows = 0;
        for (table, energy) in ENERGIES {
            let millis = load_current(
                connection,
                source,
                table,
                &["pnu", "use_ym", "usage_amount"],
                &format!("current_{energy}{suffix}"),
            )?;
            read_millis.insert(table.to_owned(), millis);
            connection.execute_batch(&fill(
                ROWS_SQL,
                &[
                    ("energy", energy),
                    ("suffix", suffix),
                    ("pnu", &served_pnu("m")),
                    ("join", &renumbering_join("m", suffix)),
                ],
            ))?;
            let count: i64 = connection.query_row(
                &format!("SELECT count(*) FROM {energy}_rows{suffix}"),
                [],
                |row| row.get(0),
            )?;
            rows += u64::try_from(count)?;
        }
        if suffix.is_empty() {
            connection.execute_batch(BUILT_TABLE)?;
        }
        Ok(KindInputs {
            served_parcels,
            rows,
            read_millis,
        })
    }

    fn changed_pnus(&self, connection: &Connection) -> anyhow::Result<u64> {
        let mut parts = Vec::new();
        for (_, energy) in ENERGIES {
            for (a, b) in [("", "_old"), ("_old", "")] {
                parts.push(format!(
                    "(SELECT pnu, use_ym, usage_amount FROM {energy}_rows{a} \
                     EXCEPT ALL SELECT pnu, use_ym, usage_amount FROM {energy}_rows{b})"
                ));
            }
        }
        for (a, b) in [("", "_old"), ("_old", "")] {
            parts.push(format!(
                "(SELECT pnu, NULL, NULL FROM (SELECT pnu FROM served{a} EXCEPT SELECT pnu FROM served{b}))"
            ));
        }
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE targets AS SELECT DISTINCT pnu FROM ({});",
            parts.join(" UNION ALL ")
        ))?;
        let count: i64 =
            connection.query_row("SELECT count(*) FROM targets", [], |row| row.get(0))?;
        Ok(u64::try_from(count)?)
    }

    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        let targets = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        let monthly = ENERGIES
            .iter()
            .map(|(_, energy)| {
                fill(
                    MONTHLY_SQL,
                    &[("energy", energy), ("number", NUMBER), ("targets", targets)],
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let bound = MONTH_BOUND.to_string();
        let at = std::time::Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("monthly", &monthly),
                    ("bound", &bound),
                    ("unit", ELECTRICITY_UNIT),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the energy document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
