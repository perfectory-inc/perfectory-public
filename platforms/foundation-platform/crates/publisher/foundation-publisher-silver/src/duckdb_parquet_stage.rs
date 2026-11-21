//! `DuckDB` stages a transform's rows as Parquet files an Iceberg table can adopt.
//!
//! `DuckDB` inside the publisher computes every transform (root ADR-0559 §1); this is where its
//! rows leave it (root ADR-0560 §2). The file is written by `DuckDB` itself (`COPY … FIELD_IDS`):
//! every column carries the table's Iceberg field id, so any engine reads it without a name
//! mapping, and the footer carries the counts and bounds the manifest entry is built from
//! (`foundation_publisher_core::lakehouse_data_commit`). Nothing here commits.
//!
//! The process envelope is the floor stage's (root ADR-0127): half the execution profile's memory
//! for the buffer manager, a spill directory the call owns, no extension installed or loaded at
//! run time, and no file access outside the stage directory.

use std::path::Path;

use anyhow::{bail, ensure, Context};
use duckdb::{Config, Connection};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use lakehouse_domain::LakehouseTableContract;

use crate::{lakehouse_engine_contract::execution_profile, serving_scratch};

/// The name a stage that reads the lakehouse attaches its catalog under.
pub const LAKEHOUSE_CATALOG: &str = "lakehouse";
/// The Python Silver loads' readers of a JSON value (`py_text`, `py_truthy`, `py_json_dumps`, …).
///
/// Every stage that reads a Bronze JSON object runs it first, so the loads that leave Spark read
/// values as one Python did (root ADR-0559).
pub const PYTHON_VALUES_SQL: &str = include_str!("python_values.sql");
const QUOTE: char = '\'';

/// What a stage needs to read the lakehouse on R2 (root ADR-0559 §6).
///
/// The catalog and its token, and the key pair the data files are read with: the values
/// `LakehouseCatalogConfig` and `R2ObjectStorageConfig` hold for the publisher's own reads.
pub struct LakehouseRead {
    /// Where the release keeps its extension files (`duckdb_extensions::release_directory`).
    pub extensions: std::path::PathBuf,
    /// The Iceberg REST catalog's URI.
    pub catalog_uri: String,
    /// The warehouse name.
    pub warehouse: String,
    /// The catalog token.
    pub catalog_token: String,
    /// The R2 endpoint of the lakehouse bucket.
    pub s3_endpoint: String,
    /// The access key id the data files are read with.
    pub s3_access_key_id: String,
    /// Its secret.
    pub s3_secret_access_key: String,
}

impl LakehouseRead {
    /// The publisher's own lakehouse settings: the catalog and token `LakehouseCatalogConfig`
    /// reads, and the key pair `R2ObjectStorageConfig` reads, with this release's extensions.
    ///
    /// # Errors
    /// Returns an error when a setting is missing; the error never carries a credential.
    pub fn from_env() -> anyhow::Result<Self> {
        let catalog = lakehouse_infrastructure::LakehouseCatalogConfig::from_env()
            .context("failed to configure the lakehouse catalog for a DuckDB read")?;
        let storage = foundation_outbox::object_storage::R2ObjectStorageConfig::from_env()
            .context("failed to configure lakehouse R2 reads for a DuckDB read")?;
        Ok(Self {
            extensions: crate::duckdb_extensions::release_directory()?,
            catalog_uri: catalog.catalog_uri,
            warehouse: catalog.warehouse,
            catalog_token: catalog
                .catalog_token
                .context("the lakehouse catalog token is required")?,
            s3_endpoint: storage.endpoint,
            s3_access_key_id: storage.access_key_id,
            s3_secret_access_key: storage.secret_access_key,
        })
    }

    /// [`Self::from_env`] with the read-only key pair for the data files
    /// (`R2ObjectStorageConfig::lakehouse_reader_from_env`, root ADR-0152): a command that only
    /// reads never needs the writer pair beside it. The catalog token is the one
    /// `LakehouseCatalogConfig` reads, as for every other catalog read of the unit.
    ///
    /// # Errors
    /// Returns an error when a setting is missing; the error never carries a credential.
    pub fn with_reader_keys_from_env() -> anyhow::Result<Self> {
        let catalog = lakehouse_infrastructure::LakehouseCatalogConfig::from_env()
            .context("failed to configure the lakehouse catalog for a DuckDB read")?;
        let storage =
            foundation_outbox::object_storage::R2ObjectStorageConfig::lakehouse_reader_from_env()
                .context(
                "failed to configure lakehouse R2 reads with the reader pair for a DuckDB read",
            )?;
        Ok(Self {
            extensions: crate::duckdb_extensions::release_directory()?,
            catalog_uri: catalog.catalog_uri,
            warehouse: catalog.warehouse,
            catalog_token: catalog
                .catalog_token
                .context("the lakehouse catalog token is required")?,
            s3_endpoint: storage.endpoint,
            s3_access_key_id: storage.access_key_id,
            s3_secret_access_key: storage.secret_access_key,
        })
    }
}

/// One transform's `DuckDB` and the directory its staged files and spill live in.
///
/// Dropping it removes the directory, so a staged file is committed (uploaded) before that.
pub struct DuckDbParquetStage {
    connection: Connection,
    directory: tempfile::TempDir,
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
        let profile = execution_profile()?;
        let config = Config::default()
            .max_memory(&format!("{}MiB", profile.memory_mib / 2))?
            .threads(i64::from(profile.cpu_slots))?
            .with("temp_directory", format!("{root}/spill"))?
            .with("autoload_known_extensions", "false")?
            .with("autoinstall_known_extensions", "false")?;
        let connection = Connection::open_in_memory_with_flags(config)?;
        // Applied to the live buffer manager, as the floor stage does (root ADR-0127).
        connection.execute(
            "SET max_temp_directory_size = ?",
            [format!("{}B", serving_scratch::configured_maximum_bytes()?)],
        )?;
        // The stage directory is the only place a statement may read or write.
        connection.execute_batch(&format!(
            "SET allowed_directories=['{root}']; SET enable_external_access=false"
        ))?;
        Ok(Self {
            connection,
            directory,
        })
    }

    /// Opens a `DuckDB` that reads the lakehouse on R2 through its own Iceberg reader, attached as
    /// `LAKEHOUSE_CATALOG`.
    ///
    /// The release's extensions are loaded by path (`duckdb_extensions`, root ADR-0559 §6) and
    /// nothing is installed or auto-loaded. Unlike [`Self::open`] the database is a file in the
    /// stage directory, so a group's intermediate tables spill to disk instead of the buffer
    /// (root ADR-0559 §7), and external access stays on: reading R2 is the point. The reads are
    /// by the keys given, never by credentials the catalog vends (a read token is refused them).
    ///
    /// # Errors
    /// Returns an error when the directory cannot be made, an extension does not load, a setting
    /// holds a quote, or the catalog refuses the token. The error never carries a credential.
    pub fn open_reading_lakehouse(parent: &Path, read: &LakehouseRead) -> anyhow::Result<Self> {
        std::fs::create_dir_all(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(".duckdb-stage-")
            .tempdir_in(parent)?;
        let root = sql_path(directory.path())?;
        let profile = execution_profile()?;
        let config = Config::default()
            .max_memory(&format!("{}MiB", profile.memory_mib / 2))?
            .threads(i64::from(profile.cpu_slots))?
            .with("temp_directory", format!("{root}/spill"))?
            .with("autoload_known_extensions", "false")?
            .with("autoinstall_known_extensions", "false")?;
        let connection =
            Connection::open_with_flags(directory.path().join("stage.duckdb"), config)?;
        connection.execute(
            "SET max_temp_directory_size = ?",
            [format!("{}B", serving_scratch::configured_maximum_bytes()?)],
        )?;
        crate::duckdb_extensions::load(&connection, &read.extensions)?;
        for value in [
            &read.catalog_uri,
            &read.warehouse,
            &read.catalog_token,
            &read.s3_endpoint,
            &read.s3_access_key_id,
            &read.s3_secret_access_key,
        ] {
            ensure!(
                !value.is_empty() && !value.contains(QUOTE),
                "a lakehouse read setting is empty or holds a quote"
            );
        }
        let endpoint = read
            .s3_endpoint
            .trim_start_matches("https://")
            .trim_end_matches('/');
        // DuckDB's message for a failed statement repeats the statement: drop it.
        connection
            .execute_batch(&format!(
                "CREATE SECRET lakehouse_catalog (TYPE iceberg, TOKEN '{}'); \
                 CREATE SECRET lakehouse_objects (TYPE s3, KEY_ID '{}', SECRET '{}', ENDPOINT '{endpoint}', \
                 URL_STYLE 'path', REGION 'auto')",
                read.catalog_token, read.s3_access_key_id, read.s3_secret_access_key
            ))
            .map_err(|_| anyhow::anyhow!("DuckDB refused the lakehouse credentials"))?;
        connection
            .execute_batch(&format!(
                "ATTACH '{}' AS {LAKEHOUSE_CATALOG} (TYPE iceberg, ENDPOINT '{}', ACCESS_DELEGATION_MODE 'none')",
                read.warehouse, read.catalog_uri
            ))
            .map_err(|_| anyhow::anyhow!("DuckDB could not attach the lakehouse catalog"))?;
        Ok(Self {
            connection,
            directory,
        })
    }

    /// Sets the buffer manager's limit to `share_numerator / share_denominator` of the memory this
    /// process may still take (its unit's `MemoryMax`, `process_memory::available_bytes`), instead
    /// of the execution profile's half: a transform whose unit declares its own bound (root
    /// ADR-0455) uses it. The rest is for what the buffer manager does not count (`DuckDB`'s own
    /// structures, the allocator, the commit's uploads). Returns the limit set, or `None` when the
    /// platform publishes no limit and the profile's stays.
    ///
    /// # Errors
    /// Returns `DuckDB`'s refusal.
    pub fn use_share_of_available_memory(
        &self,
        share_numerator: u64,
        share_denominator: u64,
    ) -> anyhow::Result<Option<u64>> {
        ensure!(
            share_denominator > 0 && share_numerator < share_denominator,
            "a share of the memory is less than all of it"
        );
        let Some(available) = crate::process_memory::available_bytes() else {
            return Ok(None);
        };
        let bytes = available / share_denominator * share_numerator;
        self.connection
            .execute("SET memory_limit = ?", [format!("{bytes}B")])?;
        Ok(Some(bytes))
    }

    /// A gzip JSON Lines object as `table(line_number BIGINT, line VARCHAR)`: the non-blank lines,
    /// numbered from 1, as Python's text reader split them (a `\r\n` ends a line like `\n`).
    ///
    /// # Errors
    /// Returns an error when the file is neither gzip nor plain UTF-8 text, `table` is not a plain identifier, or
    /// `DuckDB` refuses a row.
    pub fn load_lines(&self, path: &Path, table: &str) -> anyhow::Result<u64> {
        use std::io::Read as _;

        ensure!(
            !table.is_empty()
                && table
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "{table:?} is not a plain table name"
        );
        let raw = std::fs::read(path).with_context(|| format!("cannot open {}", path.display()))?;
        // gzip by its magic bytes, plain text otherwise (a run file an image wrote).
        let text = if raw.starts_with(&[0x1f, 0x8b]) {
            let mut text = String::new();
            flate2::read::GzDecoder::new(raw.as_slice())
                .read_to_string(&mut text)
                .with_context(|| format!("{} is not gzip UTF-8 text", path.display()))?;
            text
        } else {
            String::from_utf8(raw)
                .with_context(|| format!("{} is not UTF-8 text", path.display()))?
        };
        self.connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {table} (line_number BIGINT NOT NULL, line VARCHAR NOT NULL)"
        ))?;
        let mut appender = self.connection.appender(table)?;
        let mut number = 0_i64;
        for line in text.split('\n') {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.trim().is_empty() {
                continue;
            }
            number += 1;
            appender.append_row(duckdb::params![number, line])?;
        }
        appender.flush()?;
        Ok(u64::try_from(number)?)
    }

    /// The connection a transform loads its inputs into and computes on.
    #[must_use]
    pub const fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Refuses work that would eat the reserve of the stage's disk (`config/host-layout.contract.json`
    /// `duckdb_stage_reserve_bytes`: the disk also holds the runtime Postgres).
    ///
    /// `expected_bytes` is what the caller is about to write beside the stage (a release copy, its
    /// staged files); the stage's own spill may reach its ceiling on top of it.
    ///
    /// # Errors
    /// The free space, less the spill ceiling and `expected_bytes`, is under the reserve, or it
    /// cannot be measured.
    pub fn require_room(&self, expected_bytes: u64) -> anyhow::Result<()> {
        let free = foundation_publisher_core::free_space::free_bytes(self.directory.path())?;
        let spill = serving_scratch::configured_maximum_bytes()?;
        ensure!(
            leaves_reserve(free, spill, expected_bytes, stage_reserve_bytes()?),
            "the disk of {} has {free} bytes free; this stage may spill {spill} and expects to write {expected_bytes} more, which would leave less than the {} bytes reserved on that disk (host-layout duckdb_stage_reserve_bytes). Free space or wait for the other stage to finish",
            self.directory.path().display(),
            stage_reserve_bytes()?
        );
        Ok(())
    }

    /// Lowers the memory `DuckDB` may hold before it spills to `mib`, for a reader whose unit is capped
    /// below the engine contract's execution profile (half of it is the default): `DuckDB` must spill
    /// before the kernel's limit, not after.
    ///
    /// # Errors
    /// `DuckDB` refuses the setting.
    pub fn limit_memory(&self, mib: u64) -> anyhow::Result<()> {
        self.connection
            .execute_batch(&format!("SET memory_limit = '{mib}MiB'"))
            .context("DuckDB refused the memory limit")
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
/// register titles for lacking `row_state`. Iceberg reads such a column as null from every file
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
        let sql_type = match column.logical_type {
            "string" => "VARCHAR",
            "int" => "INTEGER",
            "long" => "BIGINT",
            "double" => "DOUBLE",
            "boolean" => "BOOLEAN",
            "date" => "DATE",
            "timestamp" => "TIMESTAMPTZ",
            "binary" => "BLOB",
            decimal if decimal.starts_with("decimal(") => decimal,
            other => anyhow::bail!(
                "{} ({source}) lacks {}, and a {other} column has no null reader",
                contract.table_name,
                column.name
            ),
        };
        added.push(format!("CAST(NULL AS {sql_type}) AS {}", column.name));
    }
    if added.is_empty() {
        return Ok(source.to_owned());
    }
    Ok(format!("(SELECT *, {} FROM {source})", added.join(", ")))
}

/// The bytes a stage's disk keeps free of stage work, from the host layout contract.
fn stage_reserve_bytes() -> anyhow::Result<u64> {
    const HOST_LAYOUT: &str = include_str!("../../../../config/host-layout.contract.json");
    serde_json::from_str::<serde_json::Value>(HOST_LAYOUT)?["duckdb_stage_reserve_bytes"]
        .as_u64()
        .context("host-layout.contract.json declares no duckdb_stage_reserve_bytes")
}

/// Whether `free` bytes, less a stage's spill ceiling and what it expects to write, keep `reserve`.
const fn leaves_reserve(free: u64, spill: u64, expected: u64, reserve: u64) -> bool {
    free.saturating_sub(spill).saturating_sub(expected) >= reserve
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
