//! Append and publish a completed Silver price snapshot without losing history (ADR-0101).

use std::process::Stdio;

use anyhow::{bail, Context};
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use sqlx::{Connection, PgConnection};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};

use crate::handoff_object_support::copy_text_escape;
use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

#[path = "unit_official_price_publication.rs"]
mod publication;
use publication::{prepare_stage, promote};

const PREFIX: &str = "FOUNDATION_PLATFORM_UNIT_PRICE_";
const COPY_SQL: &str = "COPY unit_official_price_stage
    (pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id) FROM STDIN WITH (FORMAT text)";

struct Config {
    database_url: String,
    container: String,
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
            container: optional_env_value(&format!("{PREFIX}TRINO_CONTAINER"))?
                .unwrap_or_else(|| "foundation-platform-trino".to_owned()),
            iceberg_snapshot,
            source_snapshot: required_env_value(&format!("{PREFIX}SOURCE_SNAPSHOT_ID"))?,
        })
    }

    fn source_sql(&self) -> String {
        format!(
            "r2.silver.unit_official_price FOR VERSION AS OF {}",
            self.iceberg_snapshot
        )
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

/// Trino's supported CLI owns protocol pagination, retries and cancellation.
struct TrinoRows {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
}

impl TrinoRows {
    fn start(container: &str, sql: &str) -> anyhow::Result<Self> {
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                container,
                "trino",
                "--output-format",
                "JSON",
                "--execute",
                sql,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("failed to start the Trino CLI")?;
        let stdout = child.stdout.take().context("Trino stdout is absent")?;
        Ok(Self {
            child,
            lines: BufReader::new(stdout).lines(),
        })
    }

    async fn next<T: serde::de::DeserializeOwned>(&mut self) -> anyhow::Result<Option<T>> {
        if let Some(line) = self
            .lines
            .next_line()
            .await
            .context("failed to read Trino output")?
        {
            return serde_json::from_str(&line)
                .context("Trino output is not the expected JSON row")
                .map(Some);
        }
        let status = self
            .child
            .wait()
            .await
            .context("failed to wait for Trino")?;
        if !status.success() {
            bail!("Trino failed with {status}; staged rows will not be promoted");
        }
        Ok(None)
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
async fn expected_provinces(config: &Config) -> anyhow::Result<std::collections::BTreeSet<String>> {
    let sql = format!(
        "SELECT DISTINCT substr(sigungu_cd, 1, 2) AS sido
         FROM r2.silver.building_register_apartment_price FOR VERSION AS OF {}
         WHERE sigungu_cd IS NOT NULL",
        config.price_snapshot()?
    );
    let mut reader = TrinoRows::start(&config.container, &sql)?;
    let mut expected = std::collections::BTreeSet::new();
    while let Some(row) = reader.next::<SidoOnly>().await? {
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

async fn provinces(config: &Config) -> anyhow::Result<Vec<Province>> {
    let sql = format!(
        "SELECT sido, COUNT(*) AS row_count FROM {} WHERE {} GROUP BY sido ORDER BY sido",
        config.source_sql(),
        config.selection_sql()
    );
    let mut reader = TrinoRows::start(&config.container, &sql)?;
    let mut provinces = Vec::new();
    while let Some(province) = reader.next::<Province>().await? {
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
    let expected = expected_provinces(config).await?;
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
    config: &Config,
    province: &Province,
) -> anyhow::Result<u64> {
    let sql = format!(
        "SELECT pnu,dong_name,ho_name,base_date,price_won FROM {} WHERE {} AND sido = '{}'",
        config.source_sql(),
        config.selection_sql(),
        province.sido
    );
    let mut reader = TrinoRows::start(&config.container, &sql)?;
    let mut copy = conn.copy_in_raw(COPY_SQL).await?;
    let mut buffer = String::with_capacity(1024 * 1024);
    let mut count = 0_u64;
    while let Some(row) = reader.next::<PriceRow>().await? {
        if !row.pnu.starts_with(&province.sido) {
            bail!("price row is outside its declared province");
        }
        buffer.push_str(&row.copy_line(&config.source_snapshot)?);
        count += 1;
        if buffer.len() >= 1024 * 1024 {
            copy.send(buffer.as_bytes()).await?;
            buffer.clear();
        }
    }
    if count != u64::try_from(province.row_count)? {
        bail!("province row count changed within an immutable snapshot");
    }
    if !buffer.is_empty() {
        copy.send(buffer.as_bytes()).await?;
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
    let provinces = provinces(&config).await?;
    let mut conn = PgConnection::connect(&config.database_url).await?;
    prepare_stage(&mut conn).await?;
    let mut staged = 0;
    for province in &provinces {
        let started = std::time::Instant::now();
        let rows = load_province(&mut conn, &config, province).await?;
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
