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
