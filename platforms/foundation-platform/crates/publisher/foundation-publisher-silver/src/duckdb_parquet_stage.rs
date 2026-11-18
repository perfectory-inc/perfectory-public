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

use anyhow::{ensure, Context};
use duckdb::{Config, Connection};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};

use crate::{lakehouse_engine_contract::execution_profile, serving_scratch};

/// The name a stage that reads the lakehouse attaches its catalog under.
pub const LAKEHOUSE_CATALOG: &str = "lakehouse";
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

    /// The connection a transform loads its inputs into and computes on.
    #[must_use]
    pub const fn connection(&self) -> &Connection {
        &self.connection
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
            .map(|field| format!("\"{}\": {}", field.name, field.id))
            .collect::<Vec<_>>()
            .join(", ");
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

#[cfg(test)]
mod tests;

/// A path as `DuckDB` SQL text: forward slashes, and no quote that would end the literal.
fn sql_path(path: &Path) -> anyhow::Result<String> {
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
