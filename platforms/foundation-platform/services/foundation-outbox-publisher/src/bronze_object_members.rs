//! `measure-bronze-object-members`: records what is inside each Bronze ZIP beside the ledger
//! (root ADR-0169 §1).
//!
//! The ledger (`catalog.bronze_object`) knows an object's key, size and provider title; which
//! province and which edition a VWorld file holds is written only in the names of the files
//! inside it. This command reads each unmeasured ZIP's central directory with ranged GETs (the
//! ledger's `size_bytes` places the end, so no `HeadObject` is spent) and appends one
//! `catalog.bronze_object_measurement` row and its `catalog.bronze_object_member` rows per object,
//! in one transaction.
//!
//! - An object is measured once: a settled outcome (`zip`, `not_zip`, `unreadable`) is unique per
//!   object, so a second run, or two runs at once, add nothing.
//! - A `failed` outcome (the bytes could not be fetched) is recorded and retried by the next run;
//!   the run exits non-zero so the unit's failure hook hears it.
//! - Dry run (`FOUNDATION_PLATFORM_BRONZE_MEMBER_DRY_RUN=true`) measures and prints the same
//!   summary but writes nothing.
//!
//! Settings (all environment): `DATABASE_URL`; the lakehouse R2 endpoint, bucket and read-only
//! key pair (`R2ObjectStorageConfig::lakehouse_reader_from_env`);
//! `FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES`, a comma list of source slugs where a trailing `*`
//! means "every slug starting with"; `FOUNDATION_PLATFORM_BRONZE_MEMBER_LIMIT` (objects per run);
//! `FOUNDATION_PLATFORM_BRONZE_MEMBER_CONCURRENCY` (objects in flight).

mod zip_directory;

#[cfg(test)]
mod tests;

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::{bail, Context};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use foundation_outbox::{object_storage::R2ObjectStorageConfig, R2ObjectStorage};
use futures_util::{stream, StreamExt};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::r2_command_support::env_bool;
use zip_directory::{Member, Tail};

const SOURCES_ENV: &str = "FOUNDATION_PLATFORM_BRONZE_MEMBER_SOURCES";
const LIMIT_ENV: &str = "FOUNDATION_PLATFORM_BRONZE_MEMBER_LIMIT";
const CONCURRENCY_ENV: &str = "FOUNDATION_PLATFORM_BRONZE_MEMBER_CONCURRENCY";
const DRY_RUN_ENV: &str = "FOUNDATION_PLATFORM_BRONZE_MEMBER_DRY_RUN";
/// Set by the operator script to the admitted release, so a row names the code that wrote it.
const RELEASE_ENV: &str = "FOUNDATION_PLATFORM_RELEASE_ID";

const DEFAULT_LIMIT: i64 = 2_000;
const DEFAULT_CONCURRENCY: usize = 8;
const MAX_CONCURRENCY: usize = 32;

/// Bumped when the reading of a directory changes, so rows say which reading made them.
const READER_VERSION: u32 = 1;
const SUMMARY_SCHEMA: &str = "foundation-platform.bronze_object_member_measurement.v1";

/// Which sources' objects to measure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SourceSelector {
    exact: Vec<String>,
    prefixes: Vec<String>,
}

impl SourceSelector {
    /// Parses `slug,slug,prefix*`. A bare `*` selects every source.
    pub(crate) fn parse(raw: &str) -> anyhow::Result<Self> {
        let mut selector = Self::default();
        for item in raw.split(',').map(str::trim) {
            if item.is_empty() {
                bail!("{SOURCES_ENV} has an empty entry: {raw:?}");
            }
            let (slug, prefix) = item
                .strip_suffix('*')
                .map_or((item, false), |stem| (stem, true));
            let valid = slug
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
            if !valid || (!prefix && slug.is_empty()) {
                bail!("{SOURCES_ENV} entry {item:?} is not a source slug or a slug prefix with *");
            }
            if prefix {
                selector.prefixes.push(slug.to_owned());
            } else {
                selector.exact.push(slug.to_owned());
            }
        }
        Ok(selector)
    }
}

/// One run's settings.
#[derive(Clone, Debug)]
pub(crate) struct RunConfig {
    pub sources: SourceSelector,
    pub limit: i64,
    pub concurrency: usize,
    pub dry_run: bool,
    pub measured_by: String,
}

impl RunConfig {
    fn from_env() -> anyhow::Result<Self> {
        let sources = SourceSelector::parse(&required_env_value(SOURCES_ENV)?)?;
        let limit = match optional_env_value(LIMIT_ENV)? {
            Some(raw) => raw
                .parse::<i64>()
                .ok()
                .filter(|limit| *limit > 0)
                .with_context(|| format!("{LIMIT_ENV} must be a positive integer"))?,
            None => DEFAULT_LIMIT,
        };
        let concurrency = match optional_env_value(CONCURRENCY_ENV)? {
            Some(raw) => raw
                .parse::<usize>()
                .ok()
                .filter(|n| (1..=MAX_CONCURRENCY).contains(n))
                .with_context(|| format!("{CONCURRENCY_ENV} must be 1..={MAX_CONCURRENCY}"))?,
            None => DEFAULT_CONCURRENCY,
        };
        let release = optional_env_value(RELEASE_ENV)?;
        if let Some(release) = &release {
            if release.len() != 40 || !release.chars().all(|c| c.is_ascii_hexdigit()) {
                bail!("{RELEASE_ENV} must be a 40-character commit id");
            }
        }
        Ok(Self {
            sources,
            limit,
            concurrency,
            dry_run: env_bool(DRY_RUN_ENV, false)?,
            measured_by: measured_by(release.as_deref()),
        })
    }
}

fn measured_by(release: Option<&str>) -> String {
    let tool = format!("measure-bronze-object-members/v{READER_VERSION}");
    release.map_or_else(|| tool.clone(), |release| format!("{tool}@{release}"))
}

/// Exact byte ranges of an object. The R2 implementation is one GET per call (plus retries).
#[async_trait]
pub(crate) trait ObjectRanges: Send + Sync {
    /// Bytes `[start, end)` of `key`.
    async fn read(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
impl ObjectRanges for R2ObjectStorage {
    async fn read(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
        if end <= start {
            bail!("empty range {start}..{end} of {key}");
        }
        Ok(self.get_object_range(key, start, end - 1).await?)
    }
}

/// What one object turned out to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Zip(Vec<Member>),
    NotZip(String),
    Unreadable(String),
    Failed(String),
}

impl Outcome {
    const fn wire(&self) -> &'static str {
        match self {
            Self::Zip(_) => "zip",
            Self::NotZip(_) => "not_zip",
            Self::Unreadable(_) => "unreadable",
            Self::Failed(_) => "failed",
        }
    }
}

/// Reads one object's directory: the tail, then (only when they lie before it) the ZIP64 end
/// record and the central directory. Returns the outcome and the requests it spent.
pub(crate) async fn measure(ranges: &dyn ObjectRanges, key: &str, size: u64) -> (Outcome, u32) {
    let mut requests = 0_u32;
    let outcome = measure_counting(ranges, key, size, &mut requests).await;
    (outcome, requests)
}

async fn fetch(
    ranges: &dyn ObjectRanges,
    key: &str,
    start: u64,
    end: u64,
    requests: &mut u32,
) -> Result<Vec<u8>, String> {
    *requests += 1;
    let bytes = ranges
        .read(key, start, end)
        .await
        .map_err(|error| format!("{error:#}"))?;
    if bytes.len() as u64 == end - start {
        Ok(bytes)
    } else {
        Err(format!(
            "asked for {} bytes at {start}, got {}; the ledger size may be wrong",
            end - start,
            bytes.len()
        ))
    }
}

async fn measure_counting(
    ranges: &dyn ObjectRanges,
    key: &str,
    size: u64,
    requests: &mut u32,
) -> Outcome {
    if size == 0 {
        return Outcome::NotZip("an empty object".to_owned());
    }
    let (tail_start, tail_end) = zip_directory::tail_range(size);
    let tail = match fetch(ranges, key, tail_start, tail_end, requests).await {
        Ok(tail) => tail,
        Err(reason) => return Outcome::Failed(reason),
    };
    let mut verdict = zip_directory::read_tail(&tail, tail_start, size);
    if let Tail::Zip64EndAt { offset } = verdict {
        let end = offset + zip_directory::ZIP64_END_RECORD_BYTES;
        verdict = match fetch(ranges, key, offset, end, requests).await {
            Ok(record) => zip_directory::read_zip64_end(&record, offset),
            Err(reason) => return Outcome::Failed(reason),
        };
    }
    let directory = match verdict {
        Tail::Directory(directory) => directory,
        Tail::NotZip(reason) => return Outcome::NotZip(reason),
        Tail::Unreadable(reason) => return Outcome::Unreadable(reason),
        Tail::Zip64EndAt { offset } => {
            return Outcome::Unreadable(format!("a second ZIP64 end record pointer at {offset}"))
        }
    };
    let directory_end = directory.start + directory.size;
    let parsed = if directory.start >= tail_start {
        let from = (directory.start - tail_start) as usize;
        let to = (directory_end - tail_start) as usize;
        zip_directory::read_directory(&tail[from..to], directory)
    } else {
        match fetch(ranges, key, directory.start, directory_end, requests).await {
            Ok(bytes) => zip_directory::read_directory(&bytes, directory),
            Err(reason) => return Outcome::Failed(reason),
        }
    };
    match parsed {
        Ok(members) => {
            if members.iter().any(|m| {
                i64::try_from(m.uncompressed_size).is_err()
                    || i64::try_from(m.compressed_size).is_err()
            }) {
                return Outcome::Unreadable(
                    "a member size above the signed 64-bit range".to_owned(),
                );
            }
            Outcome::Zip(members)
        }
        Err(reason) => Outcome::Unreadable(reason),
    }
}

/// An unmeasured ledger object.
#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct Candidate {
    pub id: Uuid,
    pub object_key: String,
    pub size_bytes: i64,
}

/// Objects of the selected sources whose key ends in `.zip` and that have no settled measurement,
/// those never attempted first.
pub(crate) async fn candidates(
    pool: &PgPool,
    sources: &SourceSelector,
    limit: i64,
) -> anyhow::Result<Vec<Candidate>> {
    sqlx::query_as::<_, Candidate>(
        "SELECT object.id, object.object_key, object.size_bytes
           FROM catalog.bronze_object AS object
           JOIN catalog.source_catalog AS source ON source.id = object.source_catalog_id
          WHERE lower(object.object_key) LIKE '%.zip'
            AND (source.slug = ANY($1::text[])
                 OR EXISTS (SELECT 1 FROM unnest($2::text[]) AS prefix(value)
                             WHERE starts_with(source.slug, prefix.value)))
            AND NOT EXISTS (SELECT 1 FROM catalog.bronze_object_measurement AS settled
                             WHERE settled.bronze_object_id = object.id
                               AND settled.outcome <> 'failed')
          ORDER BY (SELECT count(*) FROM catalog.bronze_object_measurement AS attempt
                     WHERE attempt.bronze_object_id = object.id),
                   object.collected_at, object.id
          LIMIT $3",
    )
    .bind(&sources.exact)
    .bind(&sources.prefixes)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("cannot select unmeasured Bronze objects")
}

/// Whether a record call wrote anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Recorded {
    Written,
    /// Another run settled this object first; nothing was written.
    AlreadySettled,
}

/// Appends one measurement and its members in one transaction.
pub(crate) async fn record(
    pool: &PgPool,
    bronze_object_id: Uuid,
    outcome: &Outcome,
    measured_by: &str,
) -> anyhow::Result<Recorded> {
    let (member_count, detail) = match outcome {
        Outcome::Zip(members) => (
            Some(i32::try_from(members.len()).context("too many members for one measurement")?),
            None,
        ),
        Outcome::NotZip(reason) | Outcome::Unreadable(reason) | Outcome::Failed(reason) => {
            (None, Some(reason.as_str()))
        }
    };
    let mut transaction = pool.begin().await?;
    let measurement_id: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO catalog.bronze_object_measurement
                (id, bronze_object_id, outcome, member_count, detail, measured_by)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (bronze_object_id) WHERE outcome <> 'failed' DO NOTHING
         RETURNING id",
    )
    .bind(Uuid::now_v7())
    .bind(bronze_object_id)
    .bind(outcome.wire())
    .bind(member_count)
    .bind(detail)
    .bind(measured_by)
    .fetch_optional(&mut *transaction)
    .await
    .context("cannot append a Bronze object measurement")?;
    let Some(measurement_id) = measurement_id else {
        transaction.rollback().await?;
        return Ok(Recorded::AlreadySettled);
    };
    if let Outcome::Zip(members) = outcome {
        let mut indexes = Vec::with_capacity(members.len());
        let mut names = Vec::with_capacity(members.len());
        let mut encodings = Vec::with_capacity(members.len());
        let mut uncompressed = Vec::with_capacity(members.len());
        let mut compressed = Vec::with_capacity(members.len());
        let mut modified: Vec<Option<NaiveDateTime>> = Vec::with_capacity(members.len());
        for member in members {
            indexes.push(i32::try_from(member.index).context("member index above i32")?);
            names.push(member.name.as_str());
            encodings.push(member.name_encoding.as_str());
            uncompressed.push(i64::try_from(member.uncompressed_size)?);
            compressed.push(i64::try_from(member.compressed_size)?);
            modified.push(member.modified);
        }
        sqlx::query(
            "INSERT INTO catalog.bronze_object_member
                    (measurement_id, bronze_object_id, member_index, member_name,
                     member_name_encoding, member_uncompressed_size, member_compressed_size,
                     member_modified)
             SELECT $1, $2, member.*
               FROM unnest($3::int4[], $4::text[], $5::text[], $6::int8[], $7::int8[],
                           $8::timestamp[]) AS member",
        )
        .bind(measurement_id)
        .bind(bronze_object_id)
        .bind(&indexes)
        .bind(&names)
        .bind(&encodings)
        .bind(&uncompressed)
        .bind(&compressed)
        .bind(&modified)
        .execute(&mut *transaction)
        .await
        .context("cannot append the members of a Bronze object")?;
    }
    transaction.commit().await?;
    Ok(Recorded::Written)
}

/// The line a run prints last.
#[derive(Debug, Default, Serialize, PartialEq, Eq)]
pub(crate) struct Summary {
    pub schema_version: &'static str,
    pub dry_run: bool,
    pub measured_by: String,
    /// Unmeasured objects this run took (at most the limit).
    pub selected: u64,
    /// Objects given a settled outcome (in a dry run: that would have been).
    pub measured: u64,
    pub zip: u64,
    pub not_zip: u64,
    pub unreadable: u64,
    /// Member rows written (in a dry run: that would have been).
    pub members: u64,
    /// Settled by another run between selection and write.
    pub skipped: u64,
    /// Objects whose bytes could not be read; recorded as `failed` and retried next run.
    pub failed: u64,
    pub r2_get_requests: u64,
}

#[derive(Default)]
struct Counters {
    measured: AtomicU64,
    zip: AtomicU64,
    not_zip: AtomicU64,
    unreadable: AtomicU64,
    members: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
    requests: AtomicU64,
}

/// Measures and records one object, counting what happened.
async fn measure_one(
    pool: &PgPool,
    ranges: &dyn ObjectRanges,
    config: &RunConfig,
    counters: &Counters,
    candidate: Candidate,
) -> anyhow::Result<()> {
    let size = u64::try_from(candidate.size_bytes).unwrap_or(0);
    let (outcome, requests) = measure(ranges, &candidate.object_key, size).await;
    counters
        .requests
        .fetch_add(u64::from(requests), Ordering::Relaxed);
    if let Outcome::Failed(reason) = &outcome {
        tracing::warn!(object_key = %candidate.object_key, %reason, "cannot measure");
    }
    let recorded = if config.dry_run {
        Recorded::Written
    } else {
        record(pool, candidate.id, &outcome, &config.measured_by).await?
    };
    if recorded == Recorded::AlreadySettled {
        counters.skipped.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let bucket = match &outcome {
        Outcome::Zip(members) => {
            counters
                .members
                .fetch_add(members.len() as u64, Ordering::Relaxed);
            &counters.zip
        }
        Outcome::NotZip(_) => &counters.not_zip,
        Outcome::Unreadable(_) => &counters.unreadable,
        Outcome::Failed(_) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
    };
    bucket.fetch_add(1, Ordering::Relaxed);
    counters.measured.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Measures and records up to `config.limit` objects, `config.concurrency` at a time.
///
/// # Errors
/// Returns an error only for a database failure; an object that cannot be read is counted.
pub(crate) async fn run_with(
    pool: &PgPool,
    ranges: Arc<dyn ObjectRanges>,
    config: &RunConfig,
) -> anyhow::Result<Summary> {
    let selected = candidates(pool, &config.sources, config.limit).await?;
    let selected_count = selected.len() as u64;
    let counters = Counters::default();
    let results = stream::iter(selected)
        .map(|candidate| measure_one(pool, ranges.as_ref(), config, &counters, candidate))
        .buffer_unordered(config.concurrency)
        .collect::<Vec<_>>()
        .await;
    results.into_iter().collect::<anyhow::Result<Vec<()>>>()?;
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    Ok(Summary {
        schema_version: SUMMARY_SCHEMA,
        dry_run: config.dry_run,
        measured_by: config.measured_by.clone(),
        selected: selected_count,
        measured: load(&counters.measured),
        zip: load(&counters.zip),
        not_zip: load(&counters.not_zip),
        unreadable: load(&counters.unreadable),
        members: load(&counters.members),
        skipped: load(&counters.skipped),
        failed: load(&counters.failed),
        r2_get_requests: load(&counters.requests),
    })
}

/// The command.
pub(crate) async fn run() -> anyhow::Result<()> {
    let config = RunConfig::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(u32::try_from(config.concurrency)? + 1)
        .connect(&required_env_value("DATABASE_URL")?)
        .await
        .context("cannot connect to the Foundation database")?;
    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("cannot configure the lakehouse R2 reader")?,
    );
    let summary = run_with(&pool, Arc::new(storage), &config).await?;
    println!(
        "bronze-object-members-json {}",
        serde_json::to_string(&summary)?
    );
    if summary.failed > 0 {
        bail!(
            "{} Bronze objects could not be read; recorded as failed, retried next run",
            summary.failed
        );
    }
    Ok(())
}
