//! The `DuckDB` extensions the publisher loads from its own release, by path (root ADR-0559 §6).
//!
//! `DuckDB` inside the publisher reads Bronze objects and Iceberg tables on R2 through `httpfs`,
//! `avro` and `iceberg`. They are not downloaded at run time: automatic install and load stay off
//! (root ADR-0127), and the files travel in the release beside the executable, pinned by
//! `infra/lakehouse/contracts/duckdb-extensions.contract.json`. The release build runs
//! [`verify`] in this release's publisher with no network before the release is signed, so an
//! extension built for another `DuckDB`, a missing file or a load that reaches for the network
//! fails the build and not a running job.
//!
//! Embedded at compile time like the engine contract: the names and versions a publisher expects
//! are the ones it was built with.

use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context};
use duckdb::{Config, Connection};
use serde::{Deserialize, Serialize};

const CONTRACT_JSON: &str =
    include_str!("../../../../infra/lakehouse/contracts/duckdb-extensions.contract.json");
const CONTRACT_SCHEMA_VERSION: u32 = 1;

#[derive(Deserialize)]
struct Contract {
    schema_version: u32,
    duckdb_version: String,
    platform: String,
    directory: String,
    load_order: Vec<String>,
    distributions: BTreeMap<String, Distribution>,
}

#[derive(Deserialize)]
struct Distribution {
    extension_version: String,
    platforms: BTreeMap<String, PlatformPin>,
}

#[derive(Deserialize)]
struct PlatformPin {
    file: PinnedFile,
}

#[derive(Deserialize)]
struct PinnedFile {
    name: String,
}

/// One extension as the running `DuckDB` reports it after the load.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LoadedExtension {
    /// The extension's name (`httpfs`, `avro`, `iceberg`).
    pub name: String,
    /// The build the file carries, which is the one the contract pins.
    pub version: String,
}

fn contract() -> anyhow::Result<Contract> {
    let contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
    ensure!(
        contract.schema_version == CONTRACT_SCHEMA_VERSION,
        "unsupported DuckDB extension contract schema {}",
        contract.schema_version
    );
    ensure!(
        !contract.load_order.is_empty()
            && contract.load_order.len() == contract.distributions.len()
            && contract
                .load_order
                .iter()
                .all(|name| contract.distributions.contains_key(name)),
        "the DuckDB extension contract's load order and distributions differ"
    );
    ensure!(
        is_plain(&contract.directory),
        "the DuckDB extension directory must be one plain path segment"
    );
    Ok(contract)
}

fn is_plain(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.'
        })
        && !text.starts_with('.')
}

/// Where the running release keeps its extension files: beside the executable.
///
/// Nothing names it from the environment, as nothing names the executable (root ADR-0134 §3): a
/// job loads the extensions of the release whose publisher it runs.
///
/// # Errors
/// Returns an error when the executable's own path cannot be resolved.
pub fn release_directory() -> anyhow::Result<PathBuf> {
    let executable = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .context("cannot resolve the publisher executable")?;
    let release = executable
        .parent()
        .context("the publisher executable has no directory")?;
    Ok(release.join(contract()?.directory))
}

/// Loads every pinned extension from `directory` into `connection`, in the contract's order.
///
/// Call it before the connection gives up external access: `DuckDB` refuses to load an extension
/// file after `enable_external_access` is off.
///
/// # Errors
/// Refuses a `DuckDB` that is not the version the extensions were built for, a platform the
/// contract pins no file for, a missing file, a file `DuckDB` will not load (another build, a
/// broken signature), and a loaded extension whose build is not the pinned one.
pub fn load(connection: &Connection, directory: &Path) -> anyhow::Result<Vec<LoadedExtension>> {
    let contract = contract()?;
    let library: String =
        connection.query_row("SELECT library_version FROM pragma_version()", [], |row| {
            row.get(0)
        })?;
    ensure!(
        library == contract.duckdb_version,
        "the bundled DuckDB is {library} but the release's extensions are builds for {}",
        contract.duckdb_version
    );
    let platform = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let mut loaded = Vec::with_capacity(contract.load_order.len());
    for name in &contract.load_order {
        let distribution = &contract.distributions[name];
        let pin = distribution.platforms.get(&platform).with_context(|| {
            format!(
                "the DuckDB extension {name} is not pinned for {platform} (only {})",
                contract.platform
            )
        })?;
        ensure!(
            is_plain(&pin.file.name),
            "the DuckDB extension file name {:?} must be one plain path segment",
            pin.file.name
        );
        let path = directory.join(&pin.file.name);
        ensure!(
            path.is_file(),
            "the release carries no DuckDB extension file {}",
            path.display()
        );
        let text = path
            .to_str()
            .context("the extension path must be UTF-8")?
            .replace('\\', "/");
        ensure!(
            !text.contains('\''),
            "the extension path must not contain a quote"
        );
        connection
            .execute_batch(&format!("LOAD '{text}'"))
            .with_context(|| format!("DuckDB did not load {}", path.display()))?;
        let (is_loaded, version): (bool, String) = connection
            .query_row(
                "SELECT loaded, extension_version FROM duckdb_extensions() WHERE extension_name = ?",
                [name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| format!("DuckDB does not list the extension {name}"))?;
        ensure!(
            is_loaded && version == distribution.extension_version,
            "DuckDB loaded {name} as build {version:?} (loaded={is_loaded}); the contract pins {}",
            distribution.extension_version
        );
        loaded.push(LoadedExtension {
            name: name.clone(),
            version,
        });
    }
    Ok(loaded)
}

#[derive(Serialize)]
struct Report<'a> {
    schema_version: &'static str,
    duckdb_version: String,
    directory: &'a Path,
    extensions: Vec<LoadedExtension>,
}

/// `verify-lakehouse-engine-extensions`: proves the release's extensions load offline.
///
/// Loads them into a `DuckDB` that may neither install nor auto-load anything, and prints what
/// was loaded as one JSON line.
///
/// # Errors
/// Returns whatever [`load`] refuses.
pub fn verify() -> anyhow::Result<()> {
    let directory = release_directory()?;
    let config = Config::default()
        .with("autoload_known_extensions", "false")?
        .with("autoinstall_known_extensions", "false")?;
    let connection = Connection::open_in_memory_with_flags(config)?;
    let extensions = load(&connection, &directory)?;
    let report = serde_json::to_string(&Report {
        schema_version: "foundation-platform.lakehouse_engine_extensions_verify.v1",
        duckdb_version: contract()?.duckdb_version,
        directory: &directory,
        extensions,
    })?;
    writeln!(std::io::stdout().lock(), "{report}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_contract_pins_a_file_for_every_extension_it_loads() -> anyhow::Result<()> {
        let contract = contract()?;
        assert_eq!(contract.load_order, ["httpfs", "avro", "iceberg"]);
        for name in &contract.load_order {
            let pin = &contract.distributions[name].platforms["linux-x86_64"];
            assert_eq!(pin.file.name, format!("{name}.duckdb_extension"));
            assert!(!contract.distributions[name].extension_version.is_empty());
        }
        Ok(())
    }

    #[test]
    fn the_extensions_are_builds_for_the_bundled_duckdb() -> anyhow::Result<()> {
        // A raised `duckdb` crate with the old pins would only fail in the release build's
        // offline load; this fails beside the change.
        let connection = Connection::open_in_memory()?;
        let library: String =
            connection.query_row("SELECT library_version FROM pragma_version()", [], |row| {
                row.get(0)
            })?;
        assert_eq!(library, contract()?.duckdb_version);
        Ok(())
    }

    #[test]
    fn a_release_without_the_file_is_refused_before_anything_loads() -> anyhow::Result<()> {
        let empty = tempfile::tempdir()?;
        let connection = Connection::open_in_memory()?;
        let Err(refused) = load(&connection, empty.path()) else {
            anyhow::bail!("an empty directory must be refused");
        };
        let message = format!("{refused:#}");
        if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            assert!(
                message.contains("carries no DuckDB extension file"),
                "{message}"
            );
            assert!(message.contains("httpfs.duckdb_extension"), "{message}");
        } else {
            assert!(message.contains("is not pinned for"), "{message}");
        }
        Ok(())
    }
}
