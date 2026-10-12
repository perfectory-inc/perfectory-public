use super::*;
use sha2::{Digest, Sha256};
use std::io::Cursor;

fn evidence(path: &Path, bytes: &[u8]) -> FileEvidence {
    FileEvidence {
        path: path.to_owned(),
        size_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
    }
}

#[test]
fn verified_stage_publishes_once_and_reuses_without_transport() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let expected = evidence(&root.path().join("input.zip"), b"source");
    let cancellation = Cancellation::default();
    let pending = copy_verified(&expected, &mut Cursor::new(b"source"), &cancellation)?;
    assert!(!expected.path.exists());
    publish(vec![(expected.path.clone(), pending)])?;
    assert!(existing_matches(&expected, &cancellation)?);
    assert_eq!(fs::read(&expected.path)?, b"source");
    Ok(())
}

#[test]
fn invalid_streams_leave_no_published_or_temporary_file() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let expected = evidence(&root.path().join("input.zip"), b"source");
    for bytes in [b"short".as_slice(), b"source-extra", b"wrong!"] {
        assert!(
            copy_verified(&expected, &mut Cursor::new(bytes), &Cancellation::default()).is_err()
        );
        assert_eq!(fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn corruption_and_concurrent_destination_are_never_overwritten() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let expected = evidence(&root.path().join("input.zip"), b"source");
    let cancellation = Cancellation::default();
    let pending = copy_verified(&expected, &mut Cursor::new(b"source"), &cancellation)?;
    fs::write(&expected.path, b"wrong!")?;
    assert!(existing_matches(&expected, &cancellation).is_err());
    assert!(publish(vec![(expected.path.clone(), pending)]).is_err());
    assert_eq!(fs::read(&expected.path)?, b"wrong!");
    assert_eq!(fs::read_dir(root.path())?.count(), 1);
    Ok(())
}

#[test]
fn cancellation_during_copy_discards_temporary_file() -> anyhow::Result<()> {
    struct CancelReader(Cancellation);
    impl Read for CancelReader {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.0.cancel();
            bytes[0] = b'x';
            Ok(1)
        }
    }
    let root = tempfile::tempdir()?;
    let expected = evidence(&root.path().join("input.zip"), b"source");
    let cancellation = Cancellation::default();
    assert!(copy_verified(
        &expected,
        &mut CancelReader(cancellation.clone()),
        &cancellation
    )
    .is_err());
    assert_eq!(fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn aborted_caller_cannot_publish_prepared_inputs() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let expected = evidence(&root.path().join("input.zip"), b"source");
    let destination = expected.path.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        let pending = blocking::prepare(move |cancellation| {
            let temporary = copy_verified(&expected, &mut Cursor::new(b"source"), &cancellation)?;
            let _ = started_tx.send(());
            release_rx.recv()?;
            let cancelled = cancellation.check().is_err();
            drop(temporary);
            let _ = finished_tx.send(cancelled);
            cancellation.check()?;
            Ok(Vec::new())
        })
        .await?;
        publish(pending)
    });
    started_rx.await?;
    caller.abort();
    assert!(caller.await.is_err());
    release_tx.send(())?;
    assert!(tokio::time::timeout(std::time::Duration::from_secs(5), finished_rx).await??);
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

#[cfg(unix)]
#[test]
fn special_files_and_symlink_parents_fail_before_open() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let fifo = root.path().join("fifo");
    ensure!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()?
            .success(),
        "fixture failed"
    );
    assert!(existing_matches(&evidence(&fifo, b"source"), &Cancellation::default()).is_err());
    let alias = root.path().join("alias");
    std::os::unix::fs::symlink(root.path(), &alias)?;
    assert!(safe_parent(&alias.join("input.zip")).is_err());
    Ok(())
}
