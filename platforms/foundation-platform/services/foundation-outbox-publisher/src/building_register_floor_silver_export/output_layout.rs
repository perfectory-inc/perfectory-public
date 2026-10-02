//! Validate the complete layout before a writer can truncate or clear any path.
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, ensure, Context};

use super::ExportConfig;

pub(super) fn validate(
    config: &ExportConfig,
    objects: &[PathBuf],
    titles: &[PathBuf],
) -> anyhow::Result<()> {
    let outputs = std::iter::once(&config.output_path)
        .chain(config.proposal_input_path.iter())
        .chain(config.summary_path.iter())
        .cloned()
        .chain(
            config
                .summary_path
                .iter()
                .map(|path| super::completed_handoff::lock_path(path)),
        )
        .map(|path| resolve(&path))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let bronze = resolve(&config.bronze_local_object_root.join("bronze"))?;
    let inputs = objects
        .iter()
        .chain(titles)
        .map(|path| resolve(path))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (index, output) in outputs.iter().enumerate() {
        ensure!(
            !overlap(output, &bronze),
            "floor output layout overlap with Bronze: {}",
            output.display()
        );
        for input in &inputs {
            ensure!(
                !overlap(output, input) && !same_existing_file(output, input)?,
                "floor output layout overlap with selected input: {}",
                output.display()
            );
        }
        for other in &outputs[..index] {
            ensure!(
                !overlap(output, other) && !same_existing_file(output, other)?,
                "floor output layout overlap between outputs: {} and {}",
                output.display(),
                other.display()
            );
        }
    }
    Ok(())
}

pub(super) fn validate_inspection(
    config: &ExportConfig,
    receipt: &Path,
    objects: &[PathBuf],
    titles: &[PathBuf],
) -> anyhow::Result<()> {
    validate(config, objects, titles)?;
    let receipt = resolve(receipt)?;
    let bronze = resolve(&config.bronze_local_object_root.join("bronze"))?;
    ensure!(
        !overlap(&receipt, &bronze),
        "floor inspection receipt overlaps Bronze: {}",
        receipt.display()
    );
    for path in objects
        .iter()
        .chain(titles)
        .chain(std::iter::once(&config.output_path))
        .chain(config.proposal_input_path.iter())
        .chain(config.summary_path.iter())
    {
        let other = resolve(path)?;
        ensure!(
            !overlap(&receipt, &other) && !same_existing_file(&receipt, &other)?,
            "floor inspection receipt overlaps input or output: {}",
            receipt.display()
        );
    }
    if let Some(summary) = &config.summary_path {
        let lock = resolve(&super::completed_handoff::lock_path(summary))?;
        ensure!(
            !overlap(&receipt, &lock) && !same_existing_file(&receipt, &lock)?,
            "floor inspection receipt overlaps summary lock: {}",
            receipt.display()
        );
    }
    Ok(())
}

fn overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn resolve(path: &Path) -> anyhow::Result<PathBuf> {
    // Missing parents cannot be canonicalized. Reject parent traversal rather than
    // pretending lexical `..` processing is equivalent to filesystem resolution.
    ensure!(
        !path.components().any(|part| part == Component::ParentDir),
        "floor output layout rejects parent traversal: {}",
        path.display()
    );
    let mut prefix = std::path::absolute(path)?;
    let mut suffix = Vec::new();
    loop {
        match fs::canonicalize(&prefix) {
            Ok(mut resolved) => {
                for part in suffix.iter().rev() {
                    resolved.push(part);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if fs::symlink_metadata(&prefix).is_ok() {
                    bail!(
                        "floor output layout cannot resolve alias: {}",
                        prefix.display()
                    );
                }
                suffix.push(
                    prefix
                        .file_name()
                        .context("output layout has no existing ancestor")?
                        .to_os_string(),
                );
                ensure!(prefix.pop(), "output layout has no existing ancestor");
            }
            Err(error) => return Err(error).context("cannot resolve floor output layout"),
        }
    }
}

fn same_existing_file(left: &Path, right: &Path) -> anyhow::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = |path: &Path| match fs::metadata(path) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        };
        if let (Some(left), Some(right)) = (metadata(left)?, metadata(right)?) {
            return Ok(left.dev() == right.dev() && left.ino() == right.ino());
        }
    }
    #[cfg(not(unix))]
    let _ = (left, right);
    Ok(false)
}
