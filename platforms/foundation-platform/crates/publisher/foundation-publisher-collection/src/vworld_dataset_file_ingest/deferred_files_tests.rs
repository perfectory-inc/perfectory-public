//! Root ADR-0170: the excluded RAON selection archives are reported apart from the run's files, and
//! only those Bronze does not hold are handed to the large-file lane.

use collection_infrastructure::VWorldDatasetFileKind;

use super::deferred_files::{
    deferred_selection_archive_reports, excluded_selection_archives,
    held_or_deferred_selection_archive_reports, within_new_bytes_cap, DEFERRED_RUN_CAP_STATUS,
    DEFERRED_SELECTION_ARCHIVE_STATUS,
};
use super::tests::{
    existing_bronze_object, selected, test_config, test_job, RecordingRepo, RecordingUow,
    CONTENT_BASE_KEY,
};
use super::{ingest_evidence, VWorldDatasetFileJob};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn job_with_two_archives_and_a_plain_file() -> VWorldDatasetFileJob {
    let mut plain = selected("9006", 1, "2026-05-13").file;
    plain.download_kind = VWorldDatasetFileKind::SingleResourceFile;
    let mut held = selected("9007", 600 * 1024, "2026-05-13").file;
    held.download_kind = VWorldDatasetFileKind::SelectionArchive;
    let mut new = selected("9008", 700 * 1024, "2026-05-13").file;
    new.download_kind = VWorldDatasetFileKind::SelectionArchive;
    VWorldDatasetFileJob {
        files: vec![plain, held, new],
        ..test_job()
    }
}

#[test]
fn only_selection_archives_are_excluded_and_only_when_asked() {
    let jobs = [job_with_two_archives_and_a_plain_file()];
    let excluded = excluded_selection_archives(&jobs, None, true);
    assert_eq!(
        excluded
            .iter()
            .map(|s| s.file.file_no.as_str())
            .collect::<Vec<_>>(),
        ["9007", "9008"]
    );
    assert!(
        excluded_selection_archives(&jobs, None, false).is_empty(),
        "without the exclusion the download path meets them itself"
    );
    assert!(excluded_selection_archives(&jobs, Some(0), true).is_empty());
}

/// The held archive (9007: its listed release, a known checksum) is skipped with its key; the
/// other is deferred with its listed size, which the large-file lane's budget counts.
#[tokio::test]
async fn a_held_archive_is_skipped_and_the_rest_are_deferred() -> TestResult {
    let repo = RecordingRepo::with_existing(existing_bronze_object(
        "operation=boundary_census_emd/provider_file_id=20991231DS99994-9007",
        &format!("{CONTENT_BASE_KEY}.zip"),
        600 * 1024 * 1024,
    )?);
    let archives =
        excluded_selection_archives(&[job_with_two_archives_and_a_plain_file()], None, true);
    let reports =
        held_or_deferred_selection_archive_reports(archives, &repo, &RecordingUow::default())
            .await?;
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].file_no, "9007");
    assert_eq!(reports[0].status, "skipped_existing");
    assert_eq!(
        reports[0].object_key.as_deref(),
        Some(format!("{CONTENT_BASE_KEY}.zip").as_str())
    );
    assert_eq!(reports[1].file_no, "9008");
    assert_eq!(reports[1].status, DEFERRED_SELECTION_ARCHIVE_STATUS);
    assert_eq!(reports[1].size_bytes, Some(700 * 1024 * 1024));
    assert_eq!(reports[1].object_key, None);
    Ok(())
}

/// Root ADR-0606: a run takes the newest files Bronze lacks while their listed bytes fit its cap,
/// as a prefix (the small older file after the one that did not fit waits too), and the first file
/// goes even when it alone is larger than the cap.
#[test]
fn a_run_takes_the_newest_files_up_to_its_cap_of_new_bytes() {
    let kib = |file_no: &str, size_kib: u64| selected(file_no, size_kib, "2026-05-13");
    let pending = vec![
        (0, kib("9101", 60)),
        (1, kib("9102", 30)),
        (2, kib("9103", 20)),
        (3, kib("9104", 1)),
    ];
    let (taken, deferred) = within_new_bytes_cap(pending, 100 * 1024);
    assert_eq!(
        taken.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        deferred.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(deferred[0].1.file_no, "9103");
    assert_eq!(deferred[0].1.status, DEFERRED_RUN_CAP_STATUS);
    assert_eq!(deferred[0].1.size_bytes, Some(20 * 1024));
    assert_eq!(deferred[0].1.object_key, None);

    let (taken, deferred) =
        within_new_bytes_cap(vec![(0, kib("9105", 500)), (1, kib("9106", 1))], 100 * 1024);
    assert_eq!(
        taken.len(),
        1,
        "a file larger than the cap still lands, on its own"
    );
    assert_eq!(deferred.len(), 1);
}

#[test]
fn deferred_archives_stay_out_of_the_runs_files_and_are_counted_apart() {
    let archives =
        excluded_selection_archives(&[job_with_two_archives_and_a_plain_file()], None, true);
    let evidence = ingest_evidence(
        &test_config(),
        Vec::new(),
        deferred_selection_archive_reports(&archives),
        false,
        "ready",
    );
    assert_eq!(
        evidence.selected_file_count, 0,
        "the run's own count is unchanged"
    );
    assert!(evidence.files.is_empty());
    assert_eq!(evidence.deferred_selection_archive_file_count, 2);
    assert_eq!(evidence.selection_archives.len(), 2);
    let value = serde_json::to_value(&evidence).expect("evidence serializes");
    assert_eq!(value["deferred_selection_archive_file_count"], 2);
    assert_eq!(
        value["selection_archives"][1]["status"],
        DEFERRED_SELECTION_ARCHIVE_STATUS
    );
}
