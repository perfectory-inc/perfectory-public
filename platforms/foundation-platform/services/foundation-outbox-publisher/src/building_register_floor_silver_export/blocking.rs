//! A cancelled caller cannot publish a detached blocking worker's ready summary.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use anyhow::{ensure, Context};
use duckdb::InterruptHandle;

use super::{ExportReport, PreparedExport};

#[derive(Clone, Default)]
pub(super) struct Cancellation(Arc<State>);

#[derive(Default)]
struct State {
    cancelled: AtomicBool,
    interrupt: Mutex<Vec<Arc<InterruptHandle>>>,
}

impl Cancellation {
    pub(super) fn check(&self) -> anyhow::Result<()> {
        ensure!(
            !self.0.cancelled.load(Ordering::Acquire),
            "floor export cancelled"
        );
        Ok(())
    }

    pub(super) fn attach(&self, handle: Arc<InterruptHandle>) -> anyhow::Result<()> {
        let mut slot = self
            .0
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.push(handle);
        drop(slot);
        self.check()
    }

    pub(super) fn detach(&self, handles: &[Arc<InterruptHandle>]) {
        // Wait for any in-flight interrupt before disconnecting its C connection.
        let mut slot = self
            .0
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.retain(|registered| !handles.iter().any(|handle| Arc::ptr_eq(registered, handle)));
    }

    pub(super) fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        let slot = self
            .0
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for handle in slot.iter() {
            handle.interrupt();
        }
    }
}

struct CancelOnDrop(Cancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn run(
    worker: impl FnOnce(Cancellation) -> anyhow::Result<PreparedExport> + Send + 'static,
) -> anyhow::Result<ExportReport> {
    let prepared = prepare(worker).await?;
    // No await between accepting the worker result and writing completion evidence.
    if let Some((path, payload)) = prepared.summary {
        super::write_file_create_new(&path, &payload)?;
    }
    Ok(prepared.report)
}

pub(super) async fn prepare<T: Send + 'static>(
    worker: impl FnOnce(Cancellation) -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    let cancellation = Cancellation::default();
    let _guard = CancelOnDrop(cancellation.clone());
    let prepared = tokio::task::spawn_blocking(move || worker(cancellation))
        .await
        .context("floor export blocking worker failed")??;
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aborted_caller_cannot_publish_a_completed_workers_summary() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("ready.json");
        let summary_path = path.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(run(move |cancellation| {
            let lock = super::super::completed_handoff::lock(&summary_path)?;
            let _ = started_tx.send(());
            release_rx.recv()?;
            let cancelled = cancellation.check().is_err();
            let _ = finished_tx.send(cancelled);
            Ok(PreparedExport {
                report: ExportReport {
                    input_object_count: 0,
                    row_count: 0,
                    proposal_required_count: 0,
                    normalization_proposal_count: 0,
                },
                summary: Some((summary_path, b"must not be published".to_vec())),
                _lock: Some(lock),
            })
        }));
        started_rx.await?;
        caller.abort();
        assert!(caller.await.is_err());
        assert!(super::super::completed_handoff::lock(&path).is_err());
        release_tx.send(())?;
        assert!(tokio::time::timeout(std::time::Duration::from_secs(5), finished_rx).await??);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if super::super::completed_handoff::lock(&path).is_ok() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(!path.exists());
        Ok(())
    }
}
