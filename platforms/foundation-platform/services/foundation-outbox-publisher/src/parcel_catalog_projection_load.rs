//! Loads the parcels the lakehouse holds into the canonical catalog table.
//!
//! `catalog.parcel` was empty and had no producer, and four of the eight foreign keys pointing at
//! it are `NOT NULL`, so `catalog.building`, `catalog.building_unit`, `catalog.manufacturer` and
//! `catalog.parcel_industry_assignment` had nothing to reference either. The upstream has been
//! there since 2026-08-28: `silver.parcel_boundaries` holds 39,861,511 rows.
//!
//! **The identifier is derived, not generated.** `parcel_id_for_pnu` hashes the PNU, so re-running
//! this load produces the same identifiers and `ON CONFLICT (pnu) DO NOTHING` turns a repeat into
//! a no-op. A generated identifier would mint a second row for a parcel that has not changed and
//! the load would stop being something anyone dares repeat. This is the reason warehouse practice
//! hashes a surrogate key from the natural key rather than counting, and the PNU is kept: it is
//! the natural key and `catalog.parcel.pnu` carries it under `UNIQUE`.
//!
//! **Staged, then merged.** Rows land in a session-temporary stage with no indexes and no
//! constraints, one handoff object at a time, through `COPY`; the merge into `catalog.parcel` is a
//! separate statement per object. Loading straight into the target would pay index maintenance and
//! conflict resolution on every row of a forty-million-row stream. This is the shape
//! `postgis_parcel_boundary_mirror_national_rebuild` already uses on the same inputs.
//!
//! **It carries only what it can source.** `kind` and `area_m2` stay null: the first is a judgment
//! a person records through `update_parcel_kind`, and the second is a cadastral figure the boundary
//! source does not include (root ADR-0070). Filling either from here would be inventing.

use std::collections::BTreeSet;

use anyhow::{bail, Context};
use catalog_domain::parcel_id_for_pnu;
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::handoff_object_support::{gunzip_text, object_retry_delay};
use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_CATALOG_PROJECTION_LOAD_CONFIRM";
const SOURCE_CONTRACT_ENV: &str = "VWORLD_PARCEL_SOURCE_CONTRACT";
const HANDOFF_PREFIX_ENV: &str = "VWORLD_PARCEL_HANDOFF_PREFIX";
const SOURCE_CONTRACT_SCHEMA_VERSION: u32 = 1;
/// How many times one object is attempted before the run steps over it.
///
/// The byte-range reads underneath already retry five times with backoff, so this is the
/// layer above that: a whole-object attempt, for the case where those five were all inside
/// one bad minute.
const OBJECT_ATTEMPTS: usize = 3;
const OBJECT_RETRY_BASE_DELAY_SECONDS: u64 = 5;

/// One handoff row, of which this load reads a single field.
///
/// Deserialised rather than string-searched so a handoff whose shape changed fails here instead of
/// silently contributing nothing.
#[derive(Debug, Deserialize)]
struct HandoffRow {
    pnu: String,
}

#[derive(Debug, Deserialize)]
struct SourceContract {
    schema_version: u32,
    load_granularity: String,
    /// Where the converted handoff objects live.
    ///
    /// Read from the contract that already names the objects rather than defaulted here. It used
    /// to be a default in three callers — the exporter, the batch loader, and this command — so
    /// renaming the prefix meant editing three files and any one missed would read from a prefix
    /// nothing was written to.
    handoff_prefix: String,
    handoff_suffix: String,
    objects: Vec<SourceObject>,
}

#[derive(Debug, Deserialize)]
struct SourceObject {
    object_key: String,
    granularity: String,
}

struct Config {
    database_url: String,
    /// Overrides the contract, for a run against another bucket. Absent by default.
    handoff_prefix_override: Option<String>,
    confirmed: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            handoff_prefix_override: optional_env_value(HANDOFF_PREFIX_ENV)?,
            confirmed: optional_bool_env(CONFIRM_ENV)?.unwrap_or(false),
        })
    }
}

/// Derives the handoff object keys from the source contract.
///
/// Read from the contract rather than by listing the bucket. A listing reports whatever happens to
/// be there, so a half-converted prefix looks like the whole dataset; the contract says what the
/// dataset is, and an object that has not been converted fails when it is read rather than being
/// quietly absent from the total.
fn handoff_keys(contract: &SourceContract, prefix: &str) -> anyhow::Result<Vec<String>> {
    if contract.schema_version != SOURCE_CONTRACT_SCHEMA_VERSION {
        bail!(
            "source object contract schema_version {} is not the {SOURCE_CONTRACT_SCHEMA_VERSION} this command reads",
            contract.schema_version
        );
    }
    let mut keys: Vec<String> = contract
        .objects
        .iter()
        .filter(|object| object.granularity == contract.load_granularity)
        .map(|object| {
            let file = object
                .object_key
                .rsplit('/')
                .next()
                .unwrap_or(object.object_key.as_str());
            let base = file.strip_suffix(".zip").unwrap_or(file);
            format!("{prefix}/{base}{}", contract.handoff_suffix)
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        bail!("the source contract names no objects at its own load granularity");
    }
    Ok(keys)
}

/// Reads one handoff object into the distinct PNUs it carries.
///
/// The handoff is gzip: the national conversion on 2026-08-31 took 46.82 GiB to 9.03 GiB, and a
/// reader that assumed plain text would parse the compressed bytes as zero rows and report an
/// empty object as success.
fn parcels_in_object(object_bytes: &[u8], object_key: &str) -> anyhow::Result<BTreeSet<String>> {
    let text = gunzip_text(object_bytes, object_key)?;

    let mut parcels = BTreeSet::new();
    let mut line_count = 0_u64;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        line_count += 1;
        let row: HandoffRow = serde_json::from_str(line).with_context(|| {
            format!("handoff object {object_key} line {line_count} is not a row")
        })?;
        let pnu = Pnu::parse(row.pnu)
            .with_context(|| format!("handoff object {object_key} line {line_count} carries a PNU this catalog cannot hold"))?;
        // Parsed for validation, kept as canonical text: `Pnu` is not ordered, and the set
        // exists to give COPY a stable order.
        parcels.insert(pnu.as_str().to_owned());
    }
    if line_count == 0 {
        bail!("handoff object {object_key} carried no rows, which is not a state this dataset has");
    }
    Ok(parcels)
}

/// Reads one object and merges it, so the retry above has a single unit to repeat.
async fn load_one(
    storage: &R2ObjectStorage,
    conn: &mut PgConnection,
    key: &str,
) -> anyhow::Result<(u64, u64)> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read handoff object {key}"))?;
    let parcels = parcels_in_object(&bytes, key)?;
    load_object(conn, &parcels).await
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    // No index, no constraint, no default: the stage exists to receive bytes quickly, and every
    // structure on it would be paid once per row of a forty-million-row stream.
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_catalog_projection_stage (
             id uuid NOT NULL,
             pnu text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create the parcel catalog projection stage")?;
    Ok(())
}

/// Streams one object's parcels into the stage and merges them into `catalog.parcel`.
///
/// Returns how many the target gained. A repeat run reports zero and writes nothing, which is the
/// property that makes this load safe to run again.
async fn load_object(
    conn: &mut PgConnection,
    parcels: &BTreeSet<String>,
) -> anyhow::Result<(u64, u64)> {
    conn.execute("TRUNCATE TABLE parcel_catalog_projection_stage")
        .await
        .context("failed to truncate the parcel catalog projection stage")?;

    let mut copy = conn
        .copy_in_raw("COPY parcel_catalog_projection_stage (id, pnu) FROM STDIN WITH (FORMAT text)")
        .await
        .context("failed to start COPY into the parcel catalog projection stage")?;
    let mut buffer = String::with_capacity(1024 * 1024);
    for raw in parcels {
        let pnu = Pnu::parse(raw.clone())
            .context("a staged PNU stopped being a PNU between reading and writing")?;
        buffer.push_str(&parcel_id_for_pnu(&pnu).as_uuid().to_string());
        buffer.push('\t');
        buffer.push_str(pnu.as_str());
        buffer.push('\n');
        if buffer.len() >= 8 * 1024 * 1024 {
            copy.send(buffer.as_bytes())
                .await
                .context("COPY send failed")?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        copy.send(buffer.as_bytes())
            .await
            .context("COPY send failed")?;
    }
    let staged = copy.finish().await.context("COPY finish failed")?;
    if staged != parcels.len() as u64 {
        bail!(
            "COPY reported {staged} rows but the object carried {}",
            parcels.len()
        );
    }

    let inserted = sqlx::query(
        "INSERT INTO catalog.parcel (id, pnu)
         SELECT id, pnu FROM parcel_catalog_projection_stage
         ON CONFLICT (pnu) DO NOTHING",
    )
    .execute(&mut *conn)
    .await
    .context("failed to merge the stage into catalog.parcel")?
    .rows_affected();

    Ok((staged, inserted))
}

/// Decides what a finished pass amounts to, and refuses to call a partial one complete.
///
/// A function rather than lines inside `run` so a test can ask it directly. The property is not
/// "an error is printed somewhere" but "an unread object makes this return `Err`", and only a
/// value can be asked that.
fn verdict(
    object_count: usize,
    unread: &[String],
    staged: u64,
    inserted: u64,
    table_rows: i64,
) -> anyhow::Result<()> {
    if unread.is_empty() {
        println!(
            "parcel-catalog-projection-load-ok objects={object_count} distinct_staged={staged} inserted={inserted} table_rows={table_rows}"
        );
        return Ok(());
    }
    println!(
        "parcel-catalog-projection-load-incomplete objects={object_count} unread={} distinct_staged={staged} inserted={inserted} table_rows={table_rows}",
        unread.len()
    );
    for key in unread {
        println!("parcel-catalog-projection-load-incomplete-key {key}");
    }
    bail!(
        "{} of {object_count} objects could not be read; the table holds {table_rows} rows and is \
         not the whole dataset. Re-running loads only what is missing.",
        unread.len()
    );
}

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let contract_path = optional_env_value(SOURCE_CONTRACT_ENV)?.unwrap_or_else(|| {
        "infra/lakehouse/contracts/vworld-parcel-source-objects.json".to_owned()
    });
    let contract: SourceContract = serde_json::from_str(
        &std::fs::read_to_string(&contract_path)
            .with_context(|| format!("failed to read the source contract {contract_path}"))?,
    )
    .with_context(|| format!("failed to parse the source contract {contract_path}"))?;

    // No bound knob. A run either covers what the contract names or it does not, and a
    // truncating flag makes "was this the whole dataset" a question about how it was invoked
    // rather than about the contract. A smaller contract file is how a smaller run is asked for.
    let prefix = config
        .handoff_prefix_override
        .clone()
        .unwrap_or_else(|| contract.handoff_prefix.clone());
    let keys = handoff_keys(&contract, prefix.as_str())?;

    if !config.confirmed {
        println!(
            "parcel-catalog-projection-load-plan objects={} prefix={prefix} (set {CONFIRM_ENV}=true to load)",
            keys.len()
        );
        return Ok(());
    }

    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for the parcel catalog projection load")?;
    let mut conn = PgConnection::connect(config.database_url.as_str())
        .await
        .context("failed to connect to DATABASE_URL for the parcel catalog projection load")?;
    prepare_stage(&mut conn).await?;

    let mut staged_total = 0_u64;
    let mut inserted_total = 0_u64;
    let mut unread: Vec<String> = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        // One object must not end a run over 255 of them. Measured 2026-09-01: object 222 failed
        // with an R2 streaming error after the byte-range retries underneath were exhausted, and
        // the whole run stopped forty minutes in. The same object read cleanly on the next run, so
        // the failure was the network rather than the object — and a run that has to start over
        // because of a blip is a run nobody schedules.
        let mut outcome = None;
        for attempt in 1..=OBJECT_ATTEMPTS {
            match load_one(&storage, &mut conn, key.as_str()).await {
                Ok(counts) => {
                    outcome = Some(counts);
                    break;
                }
                Err(error) if attempt < OBJECT_ATTEMPTS => {
                    println!(
                        "parcel-catalog-projection-load-retry key={key} attempt={attempt}/{OBJECT_ATTEMPTS} error={error:#}"
                    );
                    tokio::time::sleep(object_retry_delay(
                        OBJECT_RETRY_BASE_DELAY_SECONDS,
                        attempt,
                    ))
                    .await;
                }
                Err(error) => {
                    // Recorded and stepped over, never swallowed: the run ends non-zero below and
                    // names every object it could not read. A partial load reported as a whole one
                    // is the shape this repository keeps finding.
                    println!(
                        "parcel-catalog-projection-load-unread key={key} attempts={OBJECT_ATTEMPTS} error={error:#}"
                    );
                    unread.push(key.clone());
                }
            }
        }
        if let Some((staged, inserted)) = outcome {
            staged_total += staged;
            inserted_total += inserted;
            println!(
                "parcel-catalog-projection-load-object {}/{} key={key} distinct={staged} inserted={inserted}",
                index + 1,
                keys.len()
            );
        }
    }

    // The planner has just seen the table go from empty to tens of millions of rows. Without this
    // the next reader plans against statistics describing a table that no longer exists.
    conn.execute("ANALYZE catalog.parcel")
        .await
        .context("failed to analyze catalog.parcel after loading")?;

    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.parcel")
        .fetch_one(&mut conn)
        .await
        .context("failed to count catalog.parcel after loading")?;
    verdict(keys.len(), &unread, staged_total, inserted_total, total)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn contract() -> SourceContract {
        SourceContract {
            schema_version: 1,
            load_granularity: "sigungu".to_owned(),
            handoff_prefix: "silver-handoff/vworldkr__parcel".to_owned(),
            handoff_suffix: ".jsonl.gz".to_owned(),
            objects: vec![
                SourceObject {
                    object_key: "bronze/source=vworldkr__parcel/20991231DS99990-1.zip".to_owned(),
                    granularity: "sigungu".to_owned(),
                },
                SourceObject {
                    object_key: "bronze/source=vworldkr__parcel/20991231DS99991-1.zip".to_owned(),
                    granularity: "sido".to_owned(),
                },
            ],
        }
    }

    fn gzipped(body: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(body.as_bytes()).expect("fixture write");
        encoder.finish().expect("fixture finish")
    }

    #[test]
    fn a_pass_with_nothing_unread_is_complete() {
        verdict(255, &[], 39_861_511, 9_349_707, 39_861_511).expect("a complete pass");
    }

    #[test]
    fn one_unread_object_makes_the_pass_fail_and_names_what_it_missed() {
        // Not "an error is printed somewhere": the pass must not succeed. On 2026-09-01 object 222
        // of 255 failed and the loader stopped; stepping over it is the fix, and reporting the
        // shorter table as a finished load would be a worse defect than stopping was.
        let unread = vec!["silver-handoff/vworldkr__parcel/30563-65.jsonl.gz".to_owned()];

        let error = verdict(255, &unread, 39_000_000, 100, 39_000_000)
            .expect_err("a pass that could not read an object is not a finished pass");

        let text = format!("{error:#}");
        assert!(
            text.contains("1 of 255"),
            "the verdict must count what it missed"
        );
        assert!(
            text.contains("not the whole dataset"),
            "the verdict must say the table is short, not merely that something failed"
        );
    }

    #[test]
    fn only_the_granularity_the_contract_loads_becomes_a_key() {
        // The source carries the whole country twice — seventeen sido archives and 255 sigungu
        // ones. Loading both would put every parcel in twice under a different object.
        let contract = contract();
        let keys = handoff_keys(&contract, contract.handoff_prefix.as_str()).expect("keys");

        assert_eq!(
            keys,
            vec!["silver-handoff/vworldkr__parcel/20991231DS99990-1.jsonl.gz".to_owned()]
        );
    }

    #[test]
    fn the_prefix_comes_from_the_contract() {
        // Not a default in this file. It was one here and in two load scripts, so renaming the
        // prefix meant editing three files and any one missed would read from a prefix nothing
        // was written to.
        let mut moved = contract();
        moved.handoff_prefix = "silver-handoff/somewhere-else".to_owned();

        let keys = handoff_keys(&moved, moved.handoff_prefix.as_str()).expect("keys");

        assert!(
            keys[0].starts_with("silver-handoff/somewhere-else/"),
            "the contract moved the prefix and the keys did not follow: {}",
            keys[0]
        );
    }

    #[test]
    fn a_contract_this_command_cannot_read_stops_it() {
        let mut ahead = contract();
        ahead.schema_version = 2;

        let error = handoff_keys(&ahead, "p").expect_err("a newer contract must not be guessed at");
        assert!(error.to_string().contains("schema_version"));
    }

    #[test]
    fn a_compressed_object_yields_its_parcels() {
        let body = "{\"pnu\":\"9999900000100000001\"}\n{\"pnu\":\"9999900000100000002\"}\n";

        let parcels = parcels_in_object(&gzipped(body), "k").expect("parcels");

        assert_eq!(parcels.len(), 2);
    }

    #[test]
    fn the_same_parcel_twice_in_one_object_is_one_parcel() {
        let body = "{\"pnu\":\"9999900000100000001\"}\n{\"pnu\":\"9999900000100000001\"}\n";

        let parcels = parcels_in_object(&gzipped(body), "k").expect("parcels");

        assert_eq!(parcels.len(), 1, "the target holds one row per PNU");
    }

    #[test]
    fn plain_text_where_gzip_was_promised_is_an_error() {
        // Read as text, the compressed bytes yield no rows, and an empty object would be reported
        // as a successfully loaded one.
        let error = parcels_in_object(b"{\"pnu\":\"9999900000100000001\"}\n", "k")
            .expect_err("uncompressed input must not be accepted");
        assert!(error.to_string().contains("decompress"));
    }

    #[test]
    fn an_empty_object_is_an_error_not_a_success() {
        let error =
            parcels_in_object(&gzipped(""), "k").expect_err("an empty object must not be a pass");
        assert!(error.to_string().contains("no rows"));
    }
}
