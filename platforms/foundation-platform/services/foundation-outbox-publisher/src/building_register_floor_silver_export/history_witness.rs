//! Invocation-owned historical evidence, supplied privately rather than compiled into a release.
use std::{
    fs::{File, Metadata},
    io::Read as _,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};

use super::committed_inputs::HistoricalBinding;

/// Private historical witness selected by the host operator.
pub const HISTORY_PATH_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH";
/// Raw file digest pinned by the parent for every child process.
pub const HISTORY_SHA256_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_SHA256";
/// Read-only single-file mount shared by the native and Spark children.
pub const HISTORY_CONTAINER_PATH: &str =
    "/run/foundation-inputs/building-register-floor-history.json";
const MAX_BYTES: u64 = 64 * 1024;

/// Derives a receipt identity using the producer's semantic contract and invocation witness.
/// This does not authenticate physical input bytes or ledger membership.
/// # Errors
/// Rejects malformed evidence or an unknown historical source.
pub fn source_identity_from_semantic(
    value: serde_json::Value,
    history: &HistoryWitness,
) -> anyhow::Result<String> {
    super::committed_inputs::source_identity_from_semantic(value, history)
}

/// One validated private witness and its exact bytes' digest for an entire invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryWitness {
    path: PathBuf,
    sha256: String,
    binding: HistoricalBinding,
}

impl HistoryWitness {
    /// Loads one bounded regular file without allowing path aliases or symlinks.
    /// The top-level selector computes its digest; children must supply that digest.
    /// # Errors
    /// Rejects unsafe paths, changed files, mismatched pins or invalid historical evidence.
    pub fn load(path: &Path, expected_sha256: Option<&str>) -> anyhow::Result<Self> {
        if let Some(expected) = expected_sha256 {
            ensure!(valid_sha256(expected), "invalid FLOOR history SHA-256 pin");
        }
        ensure!(path.is_absolute(), "FLOOR history path must be absolute");
        for component in path.ancestors() {
            ensure!(
                !std::fs::symlink_metadata(component)?
                    .file_type()
                    .is_symlink(),
                "FLOOR history path must not contain symlinks"
            );
        }
        ensure!(
            !path.components().any(|part| matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )),
            "FLOOR history path must be normalized"
        );
        let before = std::fs::symlink_metadata(path)?;
        ensure!(
            before.is_file() && before.len() <= MAX_BYTES,
            "FLOOR history must be a bounded regular file"
        );
        let mut file = open_regular(path)?;
        ensure!(
            same_file(&before, &file.metadata()?),
            "FLOOR history file changed before reading"
        );
        let mut bytes = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_BYTES
                && bytes.len() as u64 == before.len()
                && same_file(&before, &file.metadata()?)
                && same_file(&before, &std::fs::symlink_metadata(path)?),
            "FLOOR history file changed while reading"
        );
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        ensure!(
            expected_sha256.is_none_or(|expected| expected == sha256),
            "FLOOR history SHA-256 pin differs"
        );
        // Typed serde structs reject unknown, missing and duplicate fields at every level.
        let binding: HistoricalBinding = serde_json::from_slice(&bytes)?;
        binding.validate()?;
        Ok(Self {
            path: path.to_owned(),
            sha256,
            binding,
        })
    }

    /// Resolves the required private path and optionally requires the parent's digest.
    /// # Errors
    /// Rejects missing configuration or any invalid witness; absence never means a new source.
    pub fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
        require_pin: bool,
    ) -> anyhow::Result<Self> {
        let path = lookup(HISTORY_PATH_ENV).context("FLOOR history path is required")?;
        let pin = lookup(HISTORY_SHA256_ENV);
        ensure!(
            !require_pin || pin.is_some(),
            "FLOOR history child requires parent SHA-256 pin"
        );
        Self::load(Path::new(&path), pin.as_deref())
    }

    /// Exact host path to bind read-only into both children.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Digest of the original file bytes, independent of JSON formatting.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Typed evidence for the existing retained-outcome contract.
    /// # Errors
    /// Returns an error if the typed evidence cannot be serialized.
    pub fn value(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(&self.binding)?)
    }

    pub(super) const fn binding(&self) -> &HistoricalBinding {
        &self.binding
    }
}

pub(super) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn same_file(left: &Metadata, right: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if left.dev() != right.dev() || left.ino() != right.ino() {
            return false;
        }
    }
    right.is_file()
        && !right.file_type().is_symlink()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

fn open_regular(path: &Path) -> anyhow::Result<File> {
    #[cfg(unix)]
    let file = {
        use rustix::fs::{open, Mode, OFlags};
        File::from(open(
            path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?)
    };
    #[cfg(not(unix))]
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "FLOOR history opened object is not a regular file"
    );
    Ok(file)
}

#[cfg(test)]
pub(super) fn fixture() -> anyhow::Result<HistoryWitness> {
    HistoryWitness::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../infra/lakehouse/spark/tests/fixtures/building_register_floor_history.json")
            .canonicalize()?,
        None,
    )
}

#[cfg(test)]
mod tests;
