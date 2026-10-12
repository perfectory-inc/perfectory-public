//! Streaming size and checksum evidence for each selected physical input file.
use std::{
    collections::BTreeSet,
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
};

use super::blocking::Cancellation;
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct FileEvidence {
    pub(super) path: PathBuf,
    pub(super) size_bytes: u64,
    pub(super) sha256: String,
}

#[cfg(test)]
pub(super) async fn capture_selected(
    floors: &[PathBuf],
    titles: &[PathBuf],
) -> anyhow::Result<Vec<FileEvidence>> {
    capture_selected_cancellable(floors, titles, &Cancellation::default()).await
}

pub(super) async fn capture_selected_cancellable(
    floors: &[PathBuf],
    titles: &[PathBuf],
    cancellation: &Cancellation,
) -> anyhow::Result<Vec<FileEvidence>> {
    let paths = floors
        .iter()
        .chain(titles)
        .cloned()
        .collect::<BTreeSet<_>>();
    let cancellation = cancellation.clone();
    tokio::task::spawn_blocking(move || {
        paths
            .iter()
            .map(|path| capture(path, &cancellation))
            .collect()
    })
    .await
    .context("floor input evidence worker failed")?
}

pub(super) fn unchanged(before: &[FileEvidence], after: &[FileEvidence]) -> anyhow::Result<()> {
    ensure!(
        before == after,
        "selected floor/title input size or SHA-256 changed during export"
    );
    Ok(())
}

pub(super) fn capture(path: &Path, cancellation: &Cancellation) -> anyhow::Result<FileEvidence> {
    cancellation.check()?;
    ensure!(
        std::fs::symlink_metadata(path)?.file_type().is_file(),
        "evidence requires a regular file: {}",
        path.display()
    );
    let mut file = File::open(path)
        .with_context(|| format!("cannot open selected floor/title input {}", path.display()))?;
    let mut digest = Sha256::new();
    let size_bytes = hash_stream(&mut file, &mut digest, || cancellation.check())
        .with_context(|| format!("cannot hash evidence file {}", path.display()))?;
    Ok(FileEvidence {
        path: path.to_owned(),
        size_bytes,
        sha256: format!("{:x}", digest.finalize()),
    })
}

fn hash_stream(
    reader: &mut impl std::io::Read,
    digest: &mut Sha256,
    check: impl Fn() -> anyhow::Result<()>,
) -> anyhow::Result<u64> {
    let mut buffer = vec![0u8; 64 * 1024].into_boxed_slice();
    let mut size_bytes = 0u64;
    loop {
        check()?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.write_all(&buffer[..read])?;
        size_bytes += read as u64;
    }
    Ok(size_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashing_observes_cancellation_between_reads() {
        struct CancellingReader(Cancellation, usize);
        impl std::io::Read for CancellingReader {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                self.1 += 1;
                bytes.fill(b'x');
                self.0.cancel();
                Ok(bytes.len())
            }
        }
        let cancellation = Cancellation::default();
        let mut reader = CancellingReader(cancellation.clone(), 0);
        assert!(hash_stream(&mut reader, &mut Sha256::new(), || cancellation.check()).is_err());
        assert_eq!(reader.1, 1);
    }

    #[cfg(unix)]
    #[test]
    fn fifo_is_rejected_without_opening_it() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("fifo");
        ensure!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()?
                .success(),
            "mkfifo fixture failed"
        );
        let error = capture(&path, &Cancellation::default())
            .err()
            .context("FIFO must fail")?;
        assert!(error.to_string().contains("regular file"));
        Ok(())
    }

    #[tokio::test]
    async fn selected_input_evidence_detects_same_size_replacement() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let floor = root.path().join("floor.zip");
        let title = root.path().join("title.zip");
        std::fs::write(&floor, b"first")?;
        std::fs::write(&title, b"title")?;
        let first =
            capture_selected(std::slice::from_ref(&floor), std::slice::from_ref(&title)).await?;
        assert_eq!(first.len(), 2);
        std::fs::write(&floor, b"other")?;
        let second = capture_selected(&[floor], &[title]).await?;
        assert!(unchanged(&first, &second).is_err());
        Ok(())
    }
}
