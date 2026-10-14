//! How much room a writer still has on a filesystem. The tile bake and the collection spool both
//! refuse to start a write that cannot fit (root ADR-0200 moved it out of the tile bake so the
//! collection lane does not depend on the bake).

use std::path::Path;

#[cfg(not(unix))]
use anyhow::bail;
#[cfg(unix)]
use anyhow::Context;

/// The bytes an unprivileged writer may still use on the filesystem holding `path`.
#[cfg(unix)]
pub fn free_bytes(path: &Path) -> anyhow::Result<u64> {
    let stat =
        rustix::fs::statvfs(path).with_context(|| format!("free space of {}", path.display()))?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
pub fn free_bytes(path: &Path) -> anyhow::Result<u64> {
    bail!(
        "free space of {} can only be measured on the Linux bake host",
        path.display()
    )
}
