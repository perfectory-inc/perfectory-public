//! `DuckDB` stages a transform's rows as Parquet files an Iceberg table can adopt.
//!
//! `DuckDB` inside the publisher computes every transform (root ADR-0559 §1); this is where its
//! rows leave it (root ADR-0560 §2). The file is written by `DuckDB` itself (`COPY … FIELD_IDS`):
//! every column carries the table's Iceberg field id, so any engine reads it without a name
//! mapping, and the footer carries the counts and bounds the manifest entry is built from
//! (`foundation_publisher_core::lakehouse_data_commit`). Nothing here commits.
//!
//! Every `DuckDB` the publisher opens is opened by [`open_engine`] (root ADR-0127):
//! [`MEMORY_SHARE`] of the memory the process may still take, read from whatever runs it
//! (`process_memory::available_bytes`: a unit's `MemoryMax`/`MemoryHigh`, a container's or a task's
//! cgroup), `DuckDB`'s own thread count (it follows the cgroup's CPU quota) but no more than one
//! per [`MEMORY_PER_THREAD`] of that memory, a spill directory the
//! call owns, capped at [`DISK_SHARE`] of what its filesystem has free, and no extension installed
//! or loaded at run time. A stage that does not read the lakehouse also reads and writes no file
//! outside its directory.

use std::path::Path;

use anyhow::{bail, ensure, Context};
use duckdb::{Config, Connection};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use lakehouse_domain::LakehouseTableContract;

/// What a `DuckDB` may hold before it spills, as a share of the memory its process may still take.
///
/// `numerator / denominator`: the one value every `DuckDB` of the publisher uses. The rest is for
/// what the buffer manager does not count: `DuckDB`'s own structures, the allocator's arenas, a
/// commit that reads a staged file whole to upload it (root ADR-0599 §4). W81b measured a 5.01 GiB
/// peak at a 5 GB limit, so the limit is not the peak.
pub const MEMORY_SHARE: (u64, u64) = (3, 5);

/// What a `DuckDB` may spill, as a share of what the filesystem of its spill directory has free
/// when it opens.
///
/// `numerator / denominator`: the one value every `DuckDB` of the publisher uses. A third, so two
/// stages that open on one disk and both reach their caps still leave a third of it: on ai-server
/// the stages' disk is the runtime Postgres's (`/`, the Bronze ledger beside the collectors). On
/// AWS the disk is the task's own ephemeral storage. `DuckDB`'s own default would be 90%
/// (v2.0.0 `TemporaryFileManager::GetDefaultMax`).
pub const DISK_SHARE: (u64, u64) = (1, 3);

/// What a `DuckDB` of the publisher holds per thread: it runs at most one thread per this many
/// bytes of its memory limit, and at least one.
///
/// `DuckDB`'s own sizing, v2.0 "Environment" guide
/// (<https://duckdb.org/docs/2.0/guides/performance/environment>): "Aim for 1-4 GB memory per
/// thread", 125 MB being its minimum. Its own thread count follows the CPUs only, so the lower
/// end: a `DuckDB` that holds 16 GB still runs 16 threads. Prevents: a unit with no CPU quota ran 20 threads
/// under a 1.4 GiB limit, and the court auction Silver load ran out of memory on a 52 MiB
/// 매각공고 object (2026-11-26 21:35Z) that one thread reads at that limit (root ADR-0616 §3-1).
pub const MEMORY_PER_THREAD: u64 = 1_000_000_000;

/// Opens a `DuckDB` — the file `database`, or in memory — spilling into `spill` when given.
///
/// `memory_limit` is [`MEMORY_SHARE`] of `process_memory::available_bytes`, taken when the
/// connection opens; a platform that publishes no limit (not Linux) keeps `DuckDB`'s own default.
/// `threads` is `DuckDB`'s own count, at most one per [`MEMORY_PER_THREAD`] of that limit
/// (`held_threads`): `DuckDB` reads the cgroup's CPU quota itself (`cpu.max`, or v1's CFS quota)
/// and takes the hardware threads when there is none (v2.0.0 `DBConfig::GetSystemMaxThreads`,
/// `CGroups::GetCPULimit`), so a task's vCPU count reaches it unchanged and only its memory can
/// lower it. `max_temp_directory_size` is [`DISK_SHARE`] of what the filesystem of `spill` has
/// free, set on the live buffer manager. This is the only place a memory limit, a thread count or
/// a spill ceiling is written (`tools/semgrep/rules/duckdb-settings.yml`).
///
/// # Errors
/// The free space of `spill` cannot be measured, or `DuckDB` refuses a setting.
pub fn open_engine(database: Option<&Path>, spill: Option<&Path>) -> anyhow::Result<Connection> {
    let config = engine_config(spill)?;
    let connection = match database {
        Some(database) => Connection::open_with_flags(database, config)?,
        None => Connection::open_in_memory_with_flags(config)?,
    };
    let threads = held_threads(
        line_parts::reported_memory_limit(&connection)?,
        connection.query_row("SELECT current_setting('threads')::BIGINT", [], |row| {
            row.get(0)
        })?,
    );
    connection.execute("SET threads = ?", [threads])?;
    let ceiling = spill.map(spill_ceiling).transpose()?.flatten();
    if let Some(bytes) = ceiling {
        connection.execute("SET max_temp_directory_size = ?", [format!("{bytes}B")])?;
    }
    if let (Some(spill), Some(ceiling)) = (spill, ceiling) {
        require_declared_room(spill, ceiling)?;
    }
    Ok(connection)
}

/// The environment variable a job's unit sets to its definition's `disk_max`, in bytes.
///
/// The most its `DuckDB` stage measured holding on disk, its database and staged files
/// (`disk_measured` says where from). Rendered by `scripts/ci/unit_memory.py` beside the unit's
/// memory lines.
pub const DISK_MAX_ENV: &str = "FOUNDATION_PLATFORM_DUCKDB_DISK_MAX_BYTES";

/// Refuses to open when the filesystem of `spill`, less the spill `ceiling`, has less free than the
/// job declares it holds (`DISK_MAX_ENV`). Nothing is checked where no job declares it (a test, a
/// command run by hand): every job's unit does (`declared_definitions.py` refuses a job without it).
fn require_declared_room(spill: &Path, ceiling: u64) -> anyhow::Result<()> {
    let Some(declared) = std::env::var_os(DISK_MAX_ENV) else {
        return Ok(());
    };
    let declared: u64 = declared
        .to_str()
        .and_then(|text| text.parse().ok())
        .with_context(|| format!("{DISK_MAX_ENV} is not a byte count"))?;
    let measured = existing_ancestor(spill)?;
    let free = foundation_publisher_core::free_space::free_bytes(measured)?;
    ensure!(
        leaves_room(free, ceiling, declared),
        "the disk of {} has {free} bytes free; this stage may spill {ceiling} and its job declares it holds {declared} more (definition disk_max), which does not fit. Free space, or give the job a larger disk",
        measured.display()
    );
    Ok(())
}

/// Whether `free` bytes, less a spill `ceiling`, hold what a job declares.
const fn leaves_room(free: u64, ceiling: u64, declared: u64) -> bool {
    free.saturating_sub(ceiling) >= declared
}

/// The nearest part of `path` that exists: a spill directory is made at the first spill.
fn existing_ancestor(path: &Path) -> anyhow::Result<&Path> {
    path.ancestors()
        .find(|path| path.exists())
        .with_context(|| format!("no part of {} exists", path.display()))
}

/// The bytes of the files under `directory`.
fn directory_bytes(directory: &Path) -> std::io::Result<u64> {
    let mut bytes = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            bytes += directory_bytes(&entry.path())?;
        } else if kind.is_file() {
            bytes += entry.metadata()?.len();
        }
    }
    Ok(bytes)
}

/// The configuration [`open_engine`] opens with.
fn engine_config(spill: Option<&Path>) -> anyhow::Result<Config> {
    let mut config = Config::default()
        .with("autoload_known_extensions", "false")?
        .with("autoinstall_known_extensions", "false")?;
    if let Some(bytes) = memory_limit(crate::process_memory::available_bytes()) {
        config = config.max_memory(&format!("{bytes}B"))?;
    }
    if let Some(spill) = spill {
        config = config.with("temp_directory", sql_path(spill)?)?;
    }
    Ok(config)
}

/// The threads a `DuckDB` of `memory_limit` bytes runs: its own count `threads` (the CPUs it may
/// use), at most one per [`MEMORY_PER_THREAD`], at least one.
fn held_threads(memory_limit: u64, threads: i64) -> i64 {
    let held = i64::try_from(memory_limit / MEMORY_PER_THREAD).unwrap_or(i64::MAX);
    threads.min(held).max(1)
}

/// [`MEMORY_SHARE`] of `available` bytes; `None` when the platform publishes no limit.
const fn memory_limit(available: Option<u64>) -> Option<u64> {
    match available {
        Some(available) => Some(available / MEMORY_SHARE.1 * MEMORY_SHARE.0),
        None => None,
    }
}

/// [`DISK_SHARE`] of what the filesystem of `spill` has free; the directory need not exist yet
/// (`DuckDB` makes it at its first spill), so its nearest existing parent is measured. `None` where
/// free space is not measured (not Unix), which keeps `DuckDB`'s own default.
fn spill_ceiling(spill: &Path) -> anyhow::Result<Option<u64>> {
    if !cfg!(unix) {
        return Ok(None);
    }
    Ok(Some(disk_share(
        foundation_publisher_core::free_space::free_bytes(existing_ancestor(spill)?)?,
    )))
}

/// [`DISK_SHARE`] of `free` bytes.
const fn disk_share(free: u64) -> u64 {
    free / DISK_SHARE.1 * DISK_SHARE.0
}

/// The name a stage that reads the lakehouse attaches its catalog under.
pub const LAKEHOUSE_CATALOG: &str = "lakehouse";

/// How a stage attaches the lakehouse catalog: read-only.
///
/// `DuckDB` refuses every statement that would write an attached database in this mode
/// ("attached in read-only mode", v2.0.0
/// `ClientContext`), the Iceberg extension's catalog included (it takes `ATTACH`'s access mode,
/// duckdb-iceberg `IcebergCatalog`). A `DuckDB` commit would carry none of the summary properties
/// every reader of a table depends on (root ADR-0559 §4); the publisher commits (root ADR-0560 §2).
pub const LAKEHOUSE_ACCESS: &str = "READ_ONLY";

/// How httpfs widens the wait between two R2 requests: it doubles, as ADR-0102's reads do
/// (`R2ReadPolicy::delay` in foundation-outbox, Spark's `S3FileIO`).
const HTTP_RETRY_BACKOFF: f32 = 2.0;

/// The engine contract's R2 read retries (`r2_read`, root ADR-0102) as `DuckDB`'s own httpfs
/// settings, for every request of the stage: the data files' ranged reads and the Iceberg REST
/// catalog's calls (duckdb-iceberg's requests take the same session parameters).
///
/// httpfs (77392a7, the extension pinned for v2.0.0) asks again on a transport error, a 408, 429
/// or 5xx, on a new connection, after `http_retry_wait_ms × http_retry_backoff^(n-1)` with full
/// jitter, capped at 20 s by `DuckDB` (`HTTPRetryBudget::MAX_BACKOFF_MS`, not a setting; the
/// contract's `max_wait_ms` is 30 s). A body cut short by the connection is a transport error and
/// is asked again.
fn r2_read_settings() -> anyhow::Result<String> {
    let policy = crate::lakehouse_engine_contract::r2_read_policy()?;
    Ok(format!(
        "SET http_retries = {}; SET http_retry_wait_ms = {}; SET http_retry_backoff = {HTTP_RETRY_BACKOFF};",
        policy.num_retries, policy.min_wait_ms
    ))
}

/// The statement that attaches `warehouse` at `catalog_uri` as [`LAKEHOUSE_CATALOG`].
fn attach_lakehouse(warehouse: &str, catalog_uri: &str) -> String {
    format!(
        "ATTACH '{warehouse}' AS {LAKEHOUSE_CATALOG} \
         (TYPE iceberg, ENDPOINT '{catalog_uri}', ACCESS_DELEGATION_MODE 'none', {LAKEHOUSE_ACCESS})"
    )
}

/// The statement that attaches the Lakekeeper `warehouse` at `catalog_uri` as [`LAKEHOUSE_CATALOG`]:
/// access delegation is duckdb-iceberg's default, the credential the catalog vends per table on load
/// (root ADR-0624 P7, T9), so the stage holds no storage key.
fn attach_lakehouse_vended(warehouse: &str, catalog_uri: &str) -> String {
    format!(
        "ATTACH '{warehouse}' AS {LAKEHOUSE_CATALOG} \
         (TYPE iceberg, ENDPOINT '{catalog_uri}', SECRET lakehouse_catalog, {LAKEHOUSE_ACCESS})"
    )
}

mod line_parts;
pub use line_parts::text_bytes;

/// `table` of the lakehouse catalog as it was at `snapshot`: a relation a query may alias, join and
/// filter like any other.
///
/// The one place that writes a time travel read. `DuckDB` takes no alias after
/// `AT (VERSION => n)`, so the bare form read `... AT (VERSION => n) r` as a
/// syntax error: 2026-11-21 the first release compares failed on `... g`, 2026-11-23 the court
/// auction load's first compare against a table that already held editions failed on `... r`, each
/// in a query whose every earlier run had read a table with no snapshot. Inside a subquery the
/// clause is closed, whatever follows.
#[must_use]
pub fn table_at(table: &str, snapshot: i64) -> String {
    table_at_in(LAKEHOUSE_CATALOG, table, snapshot)
}

/// [`table_at`] in the catalog a stage attached under another name.
#[must_use]
pub fn table_at_in(catalog: &str, table: &str, snapshot: i64) -> String {
    format!("(SELECT * FROM {catalog}.{table} AT (VERSION => {snapshot}))")
}
/// The Python Silver loads' readers of a JSON value (`py_text`, `py_truthy`, `py_json_dumps`, …).
///
/// Every stage that reads a Bronze JSON object runs it first, so the loads that leave Spark read
/// values as one Python did (root ADR-0559).
pub const PYTHON_VALUES_SQL: &str = include_str!("python_values.sql");
const QUOTE: char = '\'';

/// What a stage needs to read the lakehouse on R2 (root ADR-0559 §6): reader credentials only.
///
/// The catalog and its token, and the key pair the data files are read with. `DuckDB` never holds
/// a writer credential: the data files are read with the read-only pair (root ADR-0152), the catalog
/// is attached `READ_ONLY`, and the commit stays the publisher's own (iceberg-rust `fast_append`,
/// root ADR-0560 §2).
pub struct LakehouseRead {
    /// Where the release keeps its extension files (`duckdb_extensions::release_directory`).
    pub extensions: std::path::PathBuf,
    /// The Iceberg REST catalog's URI.
    pub catalog_uri: String,
    /// The warehouse name.
    pub warehouse: String,
    /// How the catalog and its data files are reached.
    pub access: LakehouseReadAccess,
}

/// How a stage reaches the catalog and its tables' data files.
pub enum LakehouseReadAccess {
    /// The current catalog: its read-only token, and the read-only key pair the data files are read
    /// with (access delegation off).
    Keys {
        /// The catalog token.
        catalog_token: String,
        /// The R2 endpoint of the lakehouse bucket.
        s3_endpoint: String,
        /// The access key id the data files are read with.
        s3_access_key_id: String,
        /// Its secret.
        s3_secret_access_key: String,
    },
    /// The Lakekeeper catalog (root ADR-0624 P7): the lane's `OAuth2` client, and each table's data
    /// files through the read credential the catalog vends for it on load. No storage key.
    Client(lakehouse_infrastructure::LakehouseCatalogClient),
}

impl LakehouseRead {
    /// What [`Self::from_env`] needs: the catalog with its read-only token, and the read-only key
    /// pair. No writer credential is named, so a task whose secrets are made from this answer (the
    /// AWS task definitions, `describe-requirements`) holds the read-only token and the fallback of
    /// [`read_catalog_config`] is never taken there.
    #[must_use]
    pub fn requirements() -> Vec<foundation_shared_kernel::env_source::Requirement> {
        use foundation_shared_kernel::env_source::{merged, Requirement};
        let token = [Requirement::Required(
            lakehouse_infrastructure::LakehouseCatalogConfig::READER_TOKEN_ENV,
        )];
        merged(&[
            lakehouse_infrastructure::LakehouseCatalogConfig::READER_REQUIREMENTS,
            token.as_slice(),
            foundation_outbox::object_storage::LAKEHOUSE_READER_REQUIREMENTS,
        ])
    }

    /// The catalog of [`read_catalog_config`] and the read-only key pair, with this release's
    /// extensions.
    ///
    /// # Errors
    /// Returns an error when a setting is missing; the error never carries a credential.
    pub fn from_env() -> anyhow::Result<Self> {
        let settings =
            foundation_publisher_core::lakehouse_catalog::CatalogSettings::reader_from_env()
                .context("failed to configure the lakehouse catalog for a DuckDB read")?;
        if let Some(client) = settings.client {
            return Ok(Self {
                extensions: crate::duckdb_extensions::release_directory()?,
                catalog_uri: settings.config.catalog_uri,
                warehouse: settings.config.warehouse,
                access: LakehouseReadAccess::Client(client),
            });
        }
        Self::with_catalog(&read_catalog_config()?)
    }

    /// `catalog` and the read-only key pair (`R2ObjectStorageConfig::lakehouse_reader_from_env`),
    /// for a reader that has loaded its catalog settings itself.
    ///
    /// # Errors
    /// Returns an error when a setting is missing; the error never carries a credential.
    pub fn with_catalog(
        catalog: &lakehouse_infrastructure::LakehouseCatalogConfig,
    ) -> anyhow::Result<Self> {
        let storage =
            foundation_outbox::object_storage::R2ObjectStorageConfig::lakehouse_reader_from_env()
                .context(
                "failed to configure lakehouse R2 reads with the reader pair for a DuckDB read",
            )?;
        Ok(Self {
            extensions: crate::duckdb_extensions::release_directory()?,
            catalog_uri: catalog.catalog_uri.clone(),
            warehouse: catalog.warehouse.clone(),
            access: LakehouseReadAccess::Keys {
                catalog_token: catalog
                    .catalog_token
                    .clone()
                    .context("the lakehouse catalog token is required")?,
                s3_endpoint: storage.endpoint,
                s3_access_key_id: storage.access_key_id,
                s3_secret_access_key: storage.secret_access_key,
            },
        })
    }
}

/// The catalog settings a stage reads with: the read-only token when the environment holds one,
/// the unit's own token otherwise.
///
/// The fallback is for the ai-server units that load no read-only catalog token (it lives in the
/// `operations-map` group of `config/runtime-secrets.contract.json`); there `DuckDB` still cannot
/// write, for the catalog is attached `READ_ONLY` and the data files are read with the read-only
/// pair. An AWS task holds the read-only token, because [`LakehouseRead::requirements`] names it.
///
/// # Errors
/// Returns an error when a setting is missing or invalid.
pub fn read_catalog_config() -> anyhow::Result<lakehouse_infrastructure::LakehouseCatalogConfig> {
    use lakehouse_infrastructure::LakehouseCatalogConfig;
    Ok(
        if std::env::var_os(LakehouseCatalogConfig::READER_TOKEN_ENV).is_some() {
            LakehouseCatalogConfig::reader_from_env()?
        } else {
            LakehouseCatalogConfig::from_env()?
        },
    )
}

/// One transform's `DuckDB` and the directory its staged files and spill live in.
///
/// Dropping it removes the directory, so a staged file is committed (uploaded) before that.
pub struct DuckDbParquetStage {
    connection: Connection,
    directory: tempfile::TempDir,
}

/// Says, as the stage closes, how many bytes its directory holds (its database, the staged files
/// it has not dropped, its spill): the measurement a job's `disk_measured` is taken from, by a pull
/// request that copies the logged value into its definition (nothing is kept between runs).
impl Drop for DuckDbParquetStage {
    fn drop(&mut self) {
        use std::io::Write as _;
        if let Ok(bytes) = directory_bytes(self.directory.path()) {
            // The journal keeps it whatever the log filter (the publisher logs errors only by default).
            let _ = writeln!(
                std::io::stderr().lock(),
                "duckdb stage closed: disk_bytes={bytes} ({})",
                self.directory.path().display()
            );
        }
    }
}

impl DuckDbParquetStage {
    /// Opens an in-memory `DuckDB` whose only file access is a new directory under `parent`.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be made or `DuckDB` refuses the configuration.
    pub fn open(parent: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(".duckdb-stage-")
            .tempdir_in(parent)?;
        let root = sql_path(directory.path())?;
        let connection = open_engine(None, Some(&directory.path().join("spill")))?;
        // The stage directory is the only place a statement may read or write.
        connection.execute_batch(&format!(
            "SET allowed_directories=['{root}']; SET enable_external_access=false"
        ))?;
        Ok(Self {
            connection,
            directory,
        })
    }

    /// The stage's own directory: the only place an [`Self::open`] stage may read, so a test puts the
    /// files it reads here.
    #[cfg(test)]
    pub(crate) fn directory(&self) -> &Path {
        self.directory.path()
    }

    /// Opens a `DuckDB` that reads the lakehouse on R2 through its own Iceberg reader, attached as
    /// `LAKEHOUSE_CATALOG`.
    ///
    /// The release's extensions are loaded by path (`duckdb_extensions`, root ADR-0559 §6) and
    /// nothing is installed or auto-loaded. Unlike [`Self::open`] the database is a file in the
    /// stage directory, so a group's intermediate tables spill to disk instead of the buffer
    /// (root ADR-0559 §7), and external access stays on: reading R2 is the point. The reads are
    /// by the keys given, never by credentials the catalog vends (a read token is refused them),
    /// and the catalog is attached [`LAKEHOUSE_ACCESS`]: no statement of the stage can write it.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be made, an extension does not load, a setting
    /// holds a quote, or the catalog refuses the token. The error never carries a credential.
    pub fn open_reading_lakehouse(parent: &Path, read: &LakehouseRead) -> anyhow::Result<Self> {
        std::fs::create_dir_all(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(".duckdb-stage-")
            .tempdir_in(parent)?;
        let connection = open_engine(
            Some(&directory.path().join("stage.duckdb")),
            Some(&directory.path().join("spill")),
        )?;
        crate::duckdb_extensions::load(&connection, &read.extensions)?;
        connection.execute_batch(&r2_read_settings()?)?;
        for value in [&read.catalog_uri, &read.warehouse] {
            ensure!(
                !value.is_empty() && !value.contains(QUOTE),
                "a lakehouse read setting is empty or holds a quote"
            );
        }
        // DuckDB's message for a failed statement repeats the statement: drop it.
        match &read.access {
            LakehouseReadAccess::Keys {
                catalog_token,
                s3_endpoint,
                s3_access_key_id,
                s3_secret_access_key,
            } => {
                for value in [
                    catalog_token,
                    s3_endpoint,
                    s3_access_key_id,
                    s3_secret_access_key,
                ] {
                    ensure!(
                        !value.is_empty() && !value.contains(QUOTE),
                        "a lakehouse read setting is empty or holds a quote"
                    );
                }
                let endpoint = s3_endpoint
                    .trim_start_matches("https://")
                    .trim_end_matches('/');
                connection
                    .execute_batch(&format!(
                        "CREATE SECRET lakehouse_catalog (TYPE iceberg, TOKEN '{catalog_token}'); \
                         CREATE SECRET lakehouse_objects (TYPE s3, KEY_ID '{s3_access_key_id}', \
                         SECRET '{s3_secret_access_key}', ENDPOINT '{endpoint}', URL_STYLE 'path', REGION 'auto')"
                    ))
                    .map_err(|_| anyhow::anyhow!("DuckDB refused the lakehouse credentials"))?;
                connection
                    .execute_batch(&attach_lakehouse(&read.warehouse, &read.catalog_uri))
                    .map_err(|_| {
                        anyhow::anyhow!("DuckDB could not attach the lakehouse catalog")
                    })?;
            }
            LakehouseReadAccess::Client(client) => {
                for value in [
                    &client.client_id,
                    &client.client_secret,
                    &client.token_uri,
                    &client.scope,
                ] {
                    ensure!(
                        !value.is_empty() && !value.contains(QUOTE),
                        "a lakehouse read setting is empty or holds a quote"
                    );
                }
                connection
                    .execute_batch(&format!(
                        "CREATE SECRET lakehouse_catalog (TYPE iceberg, CLIENT_ID '{}', CLIENT_SECRET '{}', \
                         OAUTH2_SERVER_URI '{}', OAUTH2_SCOPE '{}')",
                        client.client_id, client.client_secret, client.token_uri, client.scope
                    ))
                    .map_err(|_| anyhow::anyhow!("DuckDB refused the lakehouse credentials"))?;
                connection
                    .execute_batch(&attach_lakehouse_vended(&read.warehouse, &read.catalog_uri))
                    .map_err(|_| {
                        anyhow::anyhow!("DuckDB could not attach the lakehouse catalog")
                    })?;
            }
        }
        Ok(Self {
            connection,
            directory,
        })
    }

    /// The connection a transform loads its inputs into and computes on.
    #[must_use]
    pub const fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Lets the next statements reuse the blocks of the tables this stage has replaced or dropped.
    ///
    /// A file-backed stage ([`Self::open_reading_lakehouse`]) gives a replaced table's blocks back
    /// only at a checkpoint (`DuckDB`'s "to reclaim space after deleting rows, use the CHECKPOINT
    /// statement"), and its automatic checkpoint follows the write-ahead log, which a large
    /// `CREATE TABLE … AS` barely grows: its row groups go straight to the database file. A build
    /// that replaces its tables once per part calls this after each part, or the file holds every
    /// part's tables at once (2026-11-22 22:21Z: the served parcel boundaries' stage passed about
    /// 43 GB by its 15th 시도 and the disk's reserve refused the last one, root ADR-0599).
    ///
    /// # Errors
    /// `DuckDB` cannot checkpoint (a transaction is open on the connection).
    pub fn release_replaced_tables(&self) -> anyhow::Result<()> {
        self.connection
            .execute_batch("CHECKPOINT")
            .context("DuckDB could not checkpoint its stage")
    }

    /// The rows `query` answers, one JSON object per row as `DuckDB`'s `to_json` writes it, for a
    /// reader that loads them somewhere else (the Silver-to-Postgres loads, root ADR-0565: they read
    /// through Trino's CLI before, one JSON object per row as well).
    ///
    /// # Errors
    /// The query fails or a row cannot be rendered.
    pub fn json_rows(&self, query: &str) -> anyhow::Result<Vec<String>> {
        let mut rows = Vec::new();
        self.each_json_row(query, |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(rows)
    }

    /// Each row `query` answers, as [`Self::json_rows`] renders it, handed to `each` as it is read:
    /// the result streams a vector at a time (`stream_arrow`, `DuckDB`'s streaming execution), so a
    /// load of millions of rows never holds them all. The first error `each` returns stops the read.
    ///
    /// # Errors
    /// The query fails, a row cannot be rendered, or `each` refused one.
    pub fn each_json_row(
        &self,
        query: &str,
        mut each: impl FnMut(String) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        use duckdb::arrow::{array::Array as _, compute::cast, datatypes::DataType};

        let mut statement = self
            .connection
            .prepare(&format!("SELECT to_json(q)::VARCHAR FROM ({query}) AS q"))
            .context("DuckDB refused the query")?;
        let batches = statement
            .stream_arrow([])
            .context("DuckDB failed to run the query")?;
        for batch in batches {
            let batch = batch.context("DuckDB failed to read a row")?;
            // VARCHAR arrives as a string or a string view, by DuckDB's settings: one form here.
            let column = cast(batch.column(0), &DataType::Utf8)
                .context("DuckDB answered a row that is not text")?;
            let text = column
                .as_any()
                .downcast_ref::<duckdb::arrow::array::StringArray>()
                .context("DuckDB answered a row that is not text")?;
            for index in 0..text.len() {
                if text.is_null(index) {
                    bail!("DuckDB answered a row with no value");
                }
                each(text.value(index).to_owned())?;
            }
        }
        Ok(())
    }

    /// Writes the rows of `query` as one Parquet file carrying the table's field ids.
    ///
    /// `query` returns one column per column of `shape`, by name; `name` names the file for the
    /// commit.
    ///
    /// A query that lacks a column of the table fails in `DuckDB`; whether the rows are fit to be
    /// appended (types, nulls in required columns, one partition per file) is the commit's to
    /// refuse.
    ///
    /// # Errors
    /// Returns an error when a column name is not a plain identifier or `DuckDB` fails the copy.
    pub fn write(&self, shape: &TableShape, query: &str, name: &str) -> anyhow::Result<StagedFile> {
        let (columns, field_ids) = copy_columns(shape, name)?;
        let local_path = self.directory.path().join(format!("{name}.parquet"));
        let target = sql_path(&local_path)?;
        self.connection
            .execute_batch(&format!(
                "COPY (SELECT {columns} FROM ({query})) TO '{target}' \
                 (FORMAT parquet, COMPRESSION zstd, FIELD_IDS {{{field_ids}}})"
            ))
            .with_context(|| format!("DuckDB failed to stage {name}.parquet"))?;
        Ok(StagedFile {
            local_path,
            name: name.to_owned(),
        })
    }
}

impl DuckDbParquetStage {
    /// [`Self::write`] into files of about `max_bytes` each, named `name-0000`, `name-0001`, …:
    /// one pass over `query`, for rows too many for one file (the commit reads a file whole
    /// into memory, root ADR-0599 §4). The split falls on row-group boundaries, so a file may
    /// run over `max_bytes` by one group. A query of no row writes no file.
    ///
    /// # Errors
    /// What [`Self::write`] refuses, or the files cannot be listed.
    pub fn write_split(
        &self,
        shape: &TableShape,
        query: &str,
        name: &str,
        max_bytes: u64,
    ) -> anyhow::Result<Vec<StagedFile>> {
        let (columns, field_ids) = copy_columns(shape, name)?;
        let directory = self.directory.path().join(format!("{name}.parts"));
        let target = sql_path(&directory)?;
        self.connection
            .execute_batch(&format!(
                "COPY (SELECT {columns} FROM ({query})) TO '{target}' \
                 (FORMAT parquet, COMPRESSION zstd, FIELD_IDS {{{field_ids}}}, FILE_SIZE_BYTES {max_bytes})"
            ))
            .with_context(|| format!("DuckDB failed to stage {name}"))?;
        let mut parts = std::fs::read_dir(&directory)
            .with_context(|| format!("cannot list {}", directory.display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        parts.sort();
        let mut files = Vec::with_capacity(parts.len());
        for local_path in parts {
            let rows: i64 = self.connection.query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet('{}')",
                    sql_path(&local_path)?
                ),
                [],
                |row| row.get(0),
            )?;
            // A query of no row still leaves one empty file: it is not a file to commit.
            if rows > 0 {
                files.push(StagedFile {
                    name: format!("{name}-{:04}", files.len()),
                    local_path,
                });
            }
        }
        Ok(files)
    }

    /// [`Self::write_split`] of the table `rows`, one set of files per hash bucket of its text
    /// column `key` (a table partitioned by `bucket(buckets, key)`): named `name-pNN-0000`, ….
    ///
    /// The bucket of each key is Iceberg's own transform, computed here and joined back, for
    /// `DuckDB` has no such function; the committer asks the same transform of every value.
    ///
    /// # Errors
    /// What [`Self::write_split`] refuses, a null key, or `DuckDB` fails the mapping.
    pub fn write_bucketed(
        &self,
        shape: &TableShape,
        rows: &str,
        key: &str,
        buckets: u32,
        name: &str,
        max_bytes: u64,
    ) -> anyhow::Result<Vec<StagedFile>> {
        let bucket_of = foundation_publisher_core::lakehouse_data_commit::text_bucket(buckets)?;
        self.connection.execute_batch(
            "CREATE OR REPLACE TABLE stage_bucket_of (stage_key VARCHAR, stage_bucket INTEGER)",
        )?;
        // A second connection appends while this one reads the keys.
        let writer = self.connection.try_clone()?;
        let mut found = std::collections::BTreeSet::new();
        {
            let mut appender = writer.appender("stage_bucket_of")?;
            let mut statement = self
                .connection
                .prepare(&format!("SELECT DISTINCT {key} FROM {rows}"))?;
            let mut keys = statement.query([])?;
            while let Some(row) = keys.next()? {
                let text: Option<String> = row.get(0)?;
                let text = text.with_context(|| format!("{rows} holds a null {key}"))?;
                let bucket = bucket_of(&text)?;
                found.insert(bucket);
                appender.append_row(duckdb::params![text, bucket])?;
            }
            appender.flush()?;
        }
        let mut files = Vec::new();
        for bucket in found {
            files.extend(self.write_split(
                shape,
                &format!(
                    "SELECT r.* FROM {rows} r JOIN stage_bucket_of b ON r.{key} = b.stage_key \
                     WHERE b.stage_bucket = {bucket}"
                ),
                &format!("{name}-p{bucket:02}"),
                max_bytes,
            )?);
        }
        self.connection
            .execute_batch("DROP TABLE stage_bucket_of")?;
        Ok(files)
    }
}

/// The projection and the field ids of a staged file of `shape`, after `name` and the columns are
/// checked to be plain identifiers.
fn copy_columns(shape: &TableShape, name: &str) -> anyhow::Result<(String, String)> {
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        "staged file name {name:?} must be ASCII letters, digits, '-' or '_'"
    );
    for field in &shape.fields {
        ensure!(
            !field.name.is_empty()
                && field
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "column {:?} is not a plain identifier",
            field.name
        );
    }
    let columns = shape
        .fields
        .iter()
        .map(|field| format!("\"{}\"", field.name))
        .collect::<Vec<_>>()
        .join(", ");
    let field_ids = shape
        .fields
        .iter()
        .map(|field| {
            field.element_id.map_or_else(
                || format!("\"{}\": {}", field.name, field.id),
                // A list: its own id, and its elements' (Parquet's three-level list).
                |element| {
                    format!(
                        "\"{}\": {{__duckdb_field_id: {}, element: {element}}}",
                        field.name, field.id
                    )
                },
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok((columns, field_ids))
}

/// A pinned table reference with the nullable contract columns it lacks read as NULL.
///
/// `source` itself when it lacks none. Every pinned read that runs a contract's current-rows query
/// reads through it (root ADR-0564 §4).
///
/// A read pinned by snapshot id has that snapshot's schema, so a table that gained a nullable
/// column after its last data commit is read without it, and the contract's current-rows query
/// names a column the read does not have. On 2026-11-19 the first Onbid load here refused the
/// register titles for lacking their removal column. Iceberg reads such a column as null from every file
/// written before it; this is the same answer (Spark: `platform_contracts.with_contract_columns`).
/// A required column is never invented: a query that names one the read lacks fails in `DuckDB`.
///
/// # Errors
/// `DuckDB` cannot describe `source`, or it lacks a nullable column whose type has no null reader
/// here.
pub fn with_contract_columns(
    connection: &Connection,
    contract: &LakehouseTableContract,
    source: &str,
) -> anyhow::Result<String> {
    let have = connection
        .prepare(&format!("DESCRIBE SELECT * FROM {source}"))
        .with_context(|| format!("DuckDB could not describe {source}"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    let mut added = Vec::new();
    for column in contract.columns {
        if column.required || have.contains(column.name) {
            continue;
        }
        let Some(sql_type) = duckdb_type(column.logical_type) else {
            anyhow::bail!(
                "{} ({source}) lacks {}, and a {} column has no null reader",
                contract.table_name,
                column.name,
                column.logical_type
            );
        };
        added.push(format!("CAST(NULL AS {sql_type}) AS {}", column.name));
    }
    if added.is_empty() {
        return Ok(source.to_owned());
    }
    Ok(format!("(SELECT *, {} FROM {source})", added.join(", ")))
}

/// The `DuckDB` type of a contract column's logical type; `None` for one it has no reader for.
fn duckdb_type(logical_type: &str) -> Option<&str> {
    Some(match logical_type {
        "string" => "VARCHAR",
        "int" => "INTEGER",
        "long" => "BIGINT",
        "double" => "DOUBLE",
        "boolean" => "BOOLEAN",
        "date" => "DATE",
        "timestamp" => "TIMESTAMPTZ",
        "binary" => "BLOB",
        decimal if decimal.starts_with("decimal(") => decimal,
        _ => return None,
    })
}

/// A test's table `name` with exactly the columns of `contract`, each of its type, filled by
/// `rows` (a query naming any of them; every other column is NULL): a stand-in for the lakehouse
/// table, whose shape a lane's SQL must not assume beyond its contract.
///
/// # Errors
/// A column of a type `DuckDB` has no reader for here, or a `DuckDB` failure.
#[cfg(test)]
pub(crate) fn contract_table(
    connection: &Connection,
    contract: &LakehouseTableContract,
    name: &str,
    rows: &str,
) -> anyhow::Result<()> {
    let columns = contract
        .columns
        .iter()
        .map(|column| {
            let sql_type = duckdb_type(column.logical_type)
                .with_context(|| format!("a {} column has no reader", column.logical_type))?;
            Ok(format!("{} {sql_type}", column.name))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {name} ({}); INSERT INTO {name} BY NAME {rows};",
        columns.join(", ")
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests;

/// A path as `DuckDB` SQL text: forward slashes, and no quote that would end the literal.
pub(crate) fn sql_path(path: &Path) -> anyhow::Result<String> {
    let text = path
        .to_str()
        .context("the stage path must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !text.contains('\''),
        "the stage path must not contain a quote"
    );
    Ok(text)
}
