//! Exact committed Bronze staging through the existing bounded storage transport.
use std::{
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use anyhow::{ensure, Context};
use foundation_outbox::R2ObjectStorage;
use tempfile::NamedTempFile;

use super::{
    blocking::{self, Cancellation},
    input_evidence::{self, FileEvidence},
    ExportConfig,
};
use crate::silver_handoff_io::{open_source, InputSource};

type PendingInputs = Vec<(PathBuf, NamedTempFile)>;

pub(super) async fn run(config: ExportConfig) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Handle::current();
    let pending =
        blocking::prepare(move |cancellation| prepare(&config, &cancellation, &runtime)).await?;
    // Only the live caller publishes; detached workers can merely drop temporary files.
    publish(pending)
}

fn validate(config: &ExportConfig) -> anyhow::Result<Vec<FileEvidence>> {
    let committed = config
        .committed_inputs
        .as_ref()
        .context("staging requires committed Bronze inputs")?;
    let exact = config
        .exact_inputs
        .as_ref()
        .context("staging requires exact FLOOR/title inputs")?;
    let (floor, title) = exact.destination_paths(config)?;
    let expected = committed.expected_evidence(config);
    ensure!(
        expected[0].path == floor && expected[1].path == title,
        "staging exact input differs from ledger"
    );
    committed.verify(config, &expected)?;
    ensure!(
        config.source_snapshot_id == committed.source_snapshot_id()?,
        "staging source identity mismatch"
    );
    super::output_layout::validate(config, &[floor], &[title])?;
    for item in &expected {
        safe_parent(&item.path)?;
    }
    Ok(expected)
}

fn prepare(
    config: &ExportConfig,
    cancellation: &Cancellation,
    runtime: &tokio::runtime::Handle,
) -> anyhow::Result<PendingInputs> {
    let mut storage = None;
    prepare_with(config, cancellation, |key| {
        if storage.is_none() {
            storage = Some(R2ObjectStorage::from_env()?);
        }
        runtime.block_on(open_source(
            &InputSource::R2Object(key.to_owned()),
            storage.as_ref(),
        ))
    })
}

pub(super) fn prepare_with<R: Read>(
    config: &ExportConfig,
    cancellation: &Cancellation,
    mut open: impl FnMut(&str) -> anyhow::Result<R>,
) -> anyhow::Result<PendingInputs> {
    cancellation.check()?;
    let expected = validate(config)?;
    // Validate both existing destinations before creating directories or opening storage.
    let missing = expected
        .iter()
        .map(|item| existing_matches(item, cancellation).map(|exists| !exists))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if missing.iter().all(|missing| !missing) {
        return Ok(Vec::new());
    }
    let mut pending = Vec::new();
    for (item, missing) in expected.iter().zip(missing) {
        if !missing {
            continue;
        }
        cancellation.check()?;
        let key = item
            .path
            .strip_prefix(&config.bronze_local_object_root)?
            .to_str()
            .context("non-Unicode Bronze key")?
            .replace('\\', "/");
        let mut reader = open(&key)?;
        cancellation.check()?;
        let parent = item
            .path
            .parent()
            .context("staging destination has no parent")?;
        fs::create_dir_all(parent)?;
        safe_parent(&item.path)?;
        pending.push((
            item.path.clone(),
            copy_verified(item, &mut reader, cancellation)?,
        ));
    }
    cancellation.check()?;
    Ok(pending)
}

fn safe_parent(path: &Path) -> anyhow::Result<()> {
    ensure!(
        !path.components().any(|part| part == Component::ParentDir),
        "staging rejects parent traversal"
    );
    for parent in path
        .ancestors()
        .skip(1)
        .filter(|path| !path.as_os_str().is_empty())
    {
        match fs::symlink_metadata(parent) {
            Ok(metadata) => ensure!(
                metadata.file_type().is_dir(),
                "staging parent must be a real directory: {}",
                parent.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn existing_matches(expected: &FileEvidence, cancellation: &Cancellation) -> anyhow::Result<bool> {
    match fs::symlink_metadata(&expected.path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file() && metadata.len() == expected.size_bytes,
                "existing staged input is not a regular file of the committed size"
            );
            ensure!(
                input_evidence::capture(&expected.path, cancellation)? == *expected,
                "existing staged input differs from committed bytes"
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn copy_verified(
    expected: &FileEvidence,
    reader: &mut impl Read,
    cancellation: &Cancellation,
) -> anyhow::Result<NamedTempFile> {
    cancellation.check()?;
    let mut pending = NamedTempFile::new_in(expected.path.parent().context("staging parent")?)?;
    let mut buffer = vec![0u8; 64 * 1024].into_boxed_slice();
    let mut remaining = expected.size_bytes;
    loop {
        cancellation.check()?;
        // One extra byte detects an oversized response without staging it.
        let limit = usize::try_from(remaining.min(buffer.len() as u64)).context("copy bound")?;
        let read = reader.read(&mut buffer[..limit.max(1)])?;
        cancellation.check()?;
        if read == 0 {
            break;
        }
        ensure!(
            read as u64 <= remaining,
            "Bronze stream exceeds committed size"
        );
        pending.write_all(&buffer[..read])?;
        remaining -= read as u64;
    }
    ensure!(
        remaining == 0,
        "Bronze stream is shorter than committed size"
    );
    pending.flush()?;
    let observed = input_evidence::capture(pending.path(), cancellation)?;
    ensure!(
        observed.size_bytes == expected.size_bytes && observed.sha256 == expected.sha256,
        "staged bytes differ from committed Bronze checksum"
    );
    cancellation.check()?;
    Ok(pending)
}

pub(super) fn publish(pending: PendingInputs) -> anyhow::Result<()> {
    for (path, temporary) in pending {
        temporary
            .persist_noclobber(&path)
            .with_context(|| format!("cannot publish staged input {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
