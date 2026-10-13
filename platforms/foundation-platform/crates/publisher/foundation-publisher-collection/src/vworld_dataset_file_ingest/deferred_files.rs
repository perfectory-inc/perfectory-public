//! Listed files a run does not download because the provider gives them only through its RAON
//! download agent (root ADR-0170). The regular lane has no byte budget (ADR-0172).
//!
//! A run that excludes RAON selection archives (`SelectionArchive`, over about 500 MB) still says
//! which of them Bronze lacks. The daily sweep hands exactly those to the large-file lane
//! (`scripts/ops/raon-large-files.sh` → `plan-provider-acquisition-jobs`), so the held check is the
//! same one the download path makes and lives in one place.

use collection_application::ports::{BronzeIngestRepository, BronzeIngestUnitOfWork};
use collection_infrastructure::{VWorldDatasetFileInventoryItem, VWorldDatasetFileKind};

use super::{
    partition_held_files, SelectedVWorldDatasetFile, VWorldDatasetFileIngestItemEvidence,
    VWorldDatasetFileJob,
};

/// File status of a listed RAON selection archive Bronze does not hold (root ADR-0170).
pub(super) const DEFERRED_SELECTION_ARCHIVE_STATUS: &str = "deferred_selection_archive";

/// The selection archives a run that excludes them leaves out, in listing order, within the
/// run's job limit. Without the exclusion there are none: the download path then meets them and
/// reports each as `provider_acquisition_blocked`, as before.
pub(super) fn excluded_selection_archives(
    jobs: &[VWorldDatasetFileJob],
    max_jobs: Option<usize>,
    exclude_selection_archives: bool,
) -> Vec<SelectedVWorldDatasetFile> {
    if !exclude_selection_archives {
        return Vec::new();
    }
    jobs.iter()
        .take(max_jobs.unwrap_or(jobs.len()))
        .flat_map(|job| {
            job.files
                .iter()
                .filter(|file| file.download_kind == VWorldDatasetFileKind::SelectionArchive)
                .map(move |file| SelectedVWorldDatasetFile {
                    job: job.clone(),
                    file: file.clone(),
                })
        })
        .collect()
}

/// Without the ledger (a run that writes nothing) every excluded archive is reported deferred.
pub(super) fn deferred_selection_archive_reports(
    archives: &[SelectedVWorldDatasetFile],
) -> Vec<VWorldDatasetFileIngestItemEvidence> {
    archives
        .iter()
        .map(|selected| {
            deferred_report(
                &selected.job,
                &selected.file,
                DEFERRED_SELECTION_ARCHIVE_STATUS,
            )
        })
        .collect()
}

/// With the ledger, an archive Bronze holds (same file id, the listed provider update date, a
/// known checksum: `existing_file_report`) is `skipped_existing` with its key, and the rest are
/// deferred. The held check always reads the ledger: `FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH`
/// is a choice about this lane's downloads, not a request to fetch every large file again.
pub(super) async fn held_or_deferred_selection_archive_reports<Repo, Uow>(
    archives: Vec<SelectedVWorldDatasetFile>,
    repo: &Repo,
    uow: &Uow,
) -> anyhow::Result<Vec<VWorldDatasetFileIngestItemEvidence>>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let (mut reports, pending) =
        partition_held_files(archives.into_iter().enumerate().collect(), false, repo, uow).await?;
    reports.extend(pending.into_iter().map(|(index, selected)| {
        (
            index,
            deferred_report(
                &selected.job,
                &selected.file,
                DEFERRED_SELECTION_ARCHIVE_STATUS,
            ),
        )
    }));
    reports.sort_by_key(|(index, _)| *index);
    Ok(reports.into_iter().map(|(_, report)| report).collect())
}

/// A listed file reported with its listed size (`size_kib` × 1024); its body was not opened.
fn deferred_report(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
    status: &str,
) -> VWorldDatasetFileIngestItemEvidence {
    VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: status.to_owned(),
        object_key: None,
        size_bytes: Some(file.size_kib.saturating_mul(1024)),
        error_message: None,
        duration_ms: 0,
    }
}
