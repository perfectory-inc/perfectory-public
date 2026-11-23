//! The work root a bake writes in (config/tile-bake-containers.contract.json work_disk): the one
//! run of a unit that may use it at a time, what earlier runs of the unit left there, and the free
//! space a run needs before it starts (root ADR-0611).
//!
//! A run makes `<unit>-<uuid>` under the work root and removes it when it ends. A run that systemd
//! killed at its time limit cannot, and one that ended while a tool container still wrote into its
//! directory could not: on 2026-11-23 two parcel directories from 2026-11-19 held 32 GB of the data
//! disk. So a run first takes its unit's lock, `<unit>.lock` in the work root, and with it held
//! removes every `<unit>-<uuid>` directory, since no other run of the unit is alive. The unit's
//! caches (`<unit>-partitions`, `<unit>-feature-index`) are not named so and stay.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context as _};
use serde::Deserialize;
use uuid::Uuid;

pub use foundation_publisher_core::free_space::free_bytes;

/// Free space the work root must have before a bake starts: the larger of a floor and a multiple
/// of the handoff's bytes (the GDAL CSV, the GeoJSON sequence, tippecanoe's temporary files and the
/// archive all live there at once).
#[derive(Debug, Deserialize)]
pub struct WorkDisk {
    pub min_free_bytes: u64,
    pub min_free_bytes_per_handoff_byte: u64,
}

/// Refuses a work disk with less free space than the contract asks for this handoff.
pub fn ensure_work_disk(free: u64, handoff_bytes: u64, disk: &WorkDisk) -> anyhow::Result<()> {
    let needed = handoff_bytes
        .saturating_mul(disk.min_free_bytes_per_handoff_byte)
        .max(disk.min_free_bytes);
    ensure!(
        free >= needed,
        "the work disk has {free} bytes free; a {handoff_bytes}-byte handoff needs {needed} (config/tile-bake-containers.contract.json work_disk)"
    );
    Ok(())
}

/// Takes the unit's lock in the work root without waiting. It is held until the returned file is
/// dropped or the process ends, so a killed run leaves no lock behind.
pub fn lock_unit(work_root: &Path, unit_key: &str) -> anyhow::Result<File> {
    let path = work_root.join(format!("{unit_key}.lock"));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("unit lock {}", path.display()))?;
    file.try_lock()
        .with_context(|| format!("another bake of unit {unit_key} holds {}", path.display()))?;
    Ok(file)
}

/// Removes the work directories earlier runs of the unit left (`<unit>-<uuid>`) and returns each
/// with the bytes its files held. Call it only while holding [`lock_unit`]'s lock. A directory that
/// cannot be removed is logged and left; the free-space check after it decides whether the run fits.
pub fn remove_stale_work(work_root: &Path, unit_key: &str) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    let prefix = format!("{unit_key}-");
    let mut removed = Vec::new();
    let entries = std::fs::read_dir(work_root)
        .with_context(|| format!("work root {}", work_root.display()))?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(run) = name.to_str().and_then(|name| name.strip_prefix(&prefix)) else {
            continue;
        };
        if Uuid::parse_str(run).is_err() || !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        let bytes = tree_bytes(&path)?;
        match std::fs::remove_dir_all(&path) {
            Ok(()) => removed.push((path, bytes)),
            Err(error) => {
                tracing::warn!(path = %path.display(), error = %error, "failed to remove a work directory an earlier run left");
            }
        }
    }
    removed.sort();
    Ok(removed)
}

/// The bytes of every file under a directory, symbolic links not followed.
fn tree_bytes(path: &Path) -> anyhow::Result<u64> {
    let mut bytes = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        bytes += if entry.file_type()?.is_dir() {
            tree_bytes(&entry.path())?
        } else {
            entry.metadata()?.len()
        };
    }
    Ok(bytes)
}
