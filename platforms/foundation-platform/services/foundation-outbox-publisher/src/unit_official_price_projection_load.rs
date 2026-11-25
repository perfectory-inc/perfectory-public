//! Append and publish a completed Silver price snapshot without losing history (ADR-0101).
//!
//! The pinned Silver snapshots are read by the publisher's own `DuckDB` with the catalog's read-only
//! token and the read-only key pair (`lakehouse_reader`, root ADR-0565), where they were read through
//! Trino's CLI. A province's rows stream from `DuckDB` on a blocking thread into the `COPY` a
//! megabyte at a time, so the load holds a few megabytes whatever the province's size.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context};
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use sqlx::{Connection, PgConnection};

use crate::handoff_object_support::copy_text_escape;
use crate::lakehouse_reader::{source, LakehouseReader};
use crate::public_data_control_support::{optional_bool_env, required_env_value};

#[path = "unit_official_price_publication.rs"]
mod publication;
use publication::{prepare_stage, promote};

const PREFIX: &str = "FOUNDATION_PLATFORM_UNIT_PRICE_";
/// COPY lines handed from the read to the `COPY` at once, and how many such batches may wait.
const COPY_BATCH_BYTES: usize = 1024 * 1024;
const COPY_BATCHES_WAITING: usize = 4;
const COPY_SQL: &str = "COPY unit_official_price_stage
    (pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id) FROM STDIN WITH (FORMAT text)";

struct Config {
    database_url: String,
    work: String,
    iceberg_snapshot: i64,
    source_snapshot: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = format!("{PREFIX}PROJECTION_LOAD_CONFIRM");
        if !optional_bool_env(&confirm)?.unwrap_or(false) {
            bail!("{confirm}=true is required: this command appends and publishes a catalog.unit_official_price batch");
        }
        let iceberg_snapshot = required_env_value(&format!("{PREFIX}ICEBERG_SNAPSHOT_ID"))?
            .parse::<i64>()
            .context("ICEBERG_SNAPSHOT_ID must be a positive integer")?;
        if iceberg_snapshot <= 0 {
            bail!("ICEBERG_SNAPSHOT_ID must be positive");
        }
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            work: required_env_value(&format!("{PREFIX}WORK"))?,
            iceberg_snapshot,
            source_snapshot: required_env_value(&format!("{PREFIX}SOURCE_SNAPSHOT_ID"))?,
        })
    }

    fn source_sql(&self) -> String {
        source("silver.unit_official_price", Some(self.iceberg_snapshot))
    }

    fn selection_sql(&self) -> String {
        format!(
            "source_snapshot_id = '{}'",
            self.source_snapshot.replace('\'', "''")
        )
    }

    /// The price Silver snapshot this projection was joined from, parsed out of the
    /// `price:<n>|exclusive:<n>|vintage:<v>` lineage the Spark job stamped.
    fn price_snapshot(&self) -> anyhow::Result<i64> {
        self.source_snapshot
            .split('|')
            .find_map(|part| part.strip_prefix("price:"))
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|snapshot| *snapshot > 0)
            .context("source_snapshot_id does not name a positive price snapshot")
    }
}

#[derive(Deserialize)]
struct Province {
    sido: String,
    row_count: i64,
}

#[derive(Deserialize)]
struct SidoOnly {
    sido: String,
}

#[derive(Deserialize)]
struct PriceRow {
    pnu: String,
    dong_name: String,
    ho_name: String,
    base_date: String,
    price_won: i64,
}

impl PriceRow {
    fn copy_line(&self, source_snapshot: &str) -> anyhow::Result<String> {
        Pnu::parse(self.pnu.clone()).context("price row has an invalid PNU")?;
        if !catalog_domain::unit_official_price::valid_base_date(&self.base_date)
            || self.price_won < 0
        {
            bail!("price row has an invalid base_date or price_won");
        }
        Ok(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            self.pnu,
            copy_text_escape(&self.dong_name),
            copy_text_escape(&self.ho_name),
            self.base_date,
            self.price_won,
            copy_text_escape(source_snapshot)
        ))
    }
}

/// The province set the pinned price snapshot actually carries. Administrative
/// mergers change how many provinces exist (the 202608 data merges Gwangju and
/// Jeonnam into one code), so completeness is defined by the source, not a constant.
fn expected_provinces(
    reader: &LakehouseReader,
    config: &Config,
) -> anyhow::Result<std::collections::BTreeSet<String>> {
    let sql = format!(
        "SELECT DISTINCT substr(sigungu_cd, 1, 2) AS sido
         FROM {}
         WHERE sigungu_cd IS NOT NULL",
        source(
            "silver.building_register_apartment_price",
            Some(config.price_snapshot()?)
        )
    );
    let mut expected = std::collections::BTreeSet::new();
    for row in reader.rows::<SidoOnly>(&sql)? {
        if row.sido.len() != 2 || !row.sido.bytes().all(|b| b.is_ascii_digit()) {
            bail!("price snapshot carries an invalid province code");
        }
        expected.insert(row.sido);
    }
    if expected.is_empty() {
        bail!("price snapshot names no provinces");
    }
    Ok(expected)
}

fn provinces(reader: &LakehouseReader, config: &Config) -> anyhow::Result<Vec<Province>> {
    let sql = format!(
        "SELECT sido, COUNT(*) AS row_count FROM {} WHERE {} GROUP BY sido ORDER BY sido",
        config.source_sql(),
        config.selection_sql()
    );
    let mut provinces = Vec::new();
    for province in reader.rows::<Province>(&sql)? {
        if province.sido.len() != 2
            || !province.sido.bytes().all(|b| b.is_ascii_digit())
            || province.row_count <= 0
        {
            bail!("Silver snapshot carries an invalid or empty province");
        }
        provinces.push(province);
    }
    // Completeness is the source's province set, not a hardcoded 17: the projection
    // must cover exactly the provinces its pinned price snapshot contains.
    let expected = expected_provinces(reader, config)?;
    let projected: std::collections::BTreeSet<String> =
        provinces.iter().map(|p| p.sido.clone()).collect();
    if projected != expected {
        let missing: Vec<_> = expected.difference(&projected).cloned().collect();
        let extra: Vec<_> = projected.difference(&expected).cloned().collect();
        bail!(
            "projection province set differs from the price snapshot: missing={missing:?} extra={extra:?}"
        );
    }
    Ok(provinces)
}

async fn load_province(
    conn: &mut PgConnection,
    reader: &Arc<LakehouseReader>,
    config: &Config,
    province: &Province,
) -> anyhow::Result<u64> {
    let sql = format!(
        "SELECT pnu,dong_name,ho_name,base_date,price_won FROM {} WHERE {} AND sido = '{}'",
        config.source_sql(),
        config.selection_sql(),
        province.sido
    );
    let (sender, mut batches) = tokio::sync::mpsc::channel::<String>(COPY_BATCHES_WAITING);
    let reading = {
        let reader = Arc::clone(reader);
        let sido = province.sido.clone();
        let source_snapshot = config.source_snapshot.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
            let mut buffer = String::with_capacity(COPY_BATCH_BYTES);
            let mut count = 0_u64;
            let hand_over = |buffer: String| {
                sender
                    .blocking_send(buffer)
                    .map_err(|_| anyhow::anyhow!("the COPY stopped taking rows"))
            };
            reader.each_row::<PriceRow>(&sql, |row| {
                if !row.pnu.starts_with(&sido) {
                    bail!("price row is outside its declared province");
                }
                buffer.push_str(&row.copy_line(&source_snapshot)?);
                count += 1;
                if buffer.len() >= COPY_BATCH_BYTES {
                    hand_over(std::mem::replace(
                        &mut buffer,
                        String::with_capacity(COPY_BATCH_BYTES),
                    ))?;
                }
                Ok(())
            })?;
            if !buffer.is_empty() {
                hand_over(buffer)?;
            }
            Ok(count)
        })
    };
    let mut copy = conn.copy_in_raw(COPY_SQL).await?;
    while let Some(batch) = batches.recv().await {
        copy.send(batch.as_bytes()).await?;
    }
    let count = reading.await.context("the province's read stopped")??;
    if count != u64::try_from(province.row_count)? {
        bail!("province row count changed within an immutable snapshot");
    }
    copy.finish().await?;
    Ok(count)
}

/// Appends a complete pinned Silver batch and atomically publishes its selection.
///
/// # Errors
/// Refuses invalid configuration, incomplete snapshots, failed reads, COPY or promotion.
pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let reader = Arc::new(LakehouseReader::open(Path::new(&config.work))?);
    let provinces = provinces(&reader, &config)?;
    let mut conn = PgConnection::connect(&config.database_url).await?;
    prepare_stage(&mut conn).await?;
    let mut staged = 0;
    for province in &provinces {
        let started = std::time::Instant::now();
        let rows = load_province(&mut conn, &reader, &config, province).await?;
        staged += rows;
        tracing::info!(sido = %province.sido, rows, elapsed_seconds = started.elapsed().as_secs_f64(), "unit price province staged");
    }
    let (inserted, published) = promote(&mut conn).await?;
    tracing::info!(staged, inserted, published,
        iceberg_snapshot = config.iceberg_snapshot, source_snapshot = %config.source_snapshot,
        "unit-official-price-projection-load-ok");
    Ok(())
}

#[cfg(test)]
#[path = "unit_official_price_projection_load_tests.rs"]
mod tests;
