import json
from pathlib import Path

from foundation_provider_acquisition.raon_batch import (
    RaonBatchAcquireResult,
    RaonBatchJob,
    load_selection,
    run_batch,
)


def test_run_batch_imports_replay_directly_to_bronze_and_deletes_private_request(
    tmp_path: Path,
) -> None:
    job = RaonBatchJob(
        source_slug="vworldkr__boundary_census_emd",
        operation="boundary_census_emd",
        download_ds_id="20991231DS99991",
        file_no="9001",
        provider_file_id="20991231DS99991-9001",
        provider_file_name="boundary.zip",
        dataset_name="V-World boundary census emd",
        base_ym="2026-05-17",
        updated_at="2026-06-01",
    )
    seen_env: dict[str, str] = {}
    seen_landing_key: list[str] = []

    def acquire(job: RaonBatchJob, paths) -> RaonBatchAcquireResult:
        seen_landing_key.append(paths.landing_object_key)
        paths.private_replay_request_path.write_text(
            json.dumps({"secret": "must-not-survive"}), encoding="utf-8"
        )
        paths.public_proof_path.write_text(
            json.dumps({"replay_looks_zip": True}), encoding="utf-8"
        )
        return RaonBatchAcquireResult(observed_filename="provider-observed.zip")

    def import_replay(job: RaonBatchJob, env: dict[str, str], paths) -> dict[str, object]:
        seen_env.update(env)
        paths.import_report_path.write_text(
            json.dumps(
                {
                    "validation_status": "committed_without_landing",
                    "object_key": paths.landing_object_key,
                    "size_bytes": 42,
                    "checksum_sha256": "a" * 64,
                    "bronze_object_key": "bronze/source=vworldkr__boundary_census_emd/20991231DS99991-9001.zip",
                    "bronze_object_id": "00000000-0000-0000-0000-000000000001",
                }
            ),
            encoding="utf-8",
        )
        return json.loads(paths.import_report_path.read_text(encoding="utf-8"))

    summary = run_batch(
        jobs=[job],
        batch_id="batch-001",
        output_root=tmp_path,
        acquire=acquire,
        import_replay=import_replay,
        base_env={"DATABASE_URL": "postgres://example"},
    )

    assert summary["selected_count"] == 1
    assert summary["attempted_count"] == 1
    assert summary["committed_count"] == 1
    assert summary["failed_count"] == 0
    assert summary["direct_to_bronze"] is True
    assert seen_landing_key == [
        "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
        "job_id=batch-001-20991231DS99991-9001/"
        "download_ds_id=20991231DS99991/file_no=9001/download.zip"
    ]
    assert seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE"] == "1"
    assert seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE"] == "1"
    assert seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH"].endswith(
        "private-replay-request.json"
    )
    assert seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_SNAPSHOT_DATE"] == "2026-05-17"
    assert seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_UPDATED_AT"] == "2026-06-01"
    assert seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_NAME"] == "provider-observed.zip"
    assert "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_LANDING_PAYLOAD_PATH" not in seen_env
    assert not Path(seen_env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH"]).exists()


def test_run_batch_records_failures_and_continues(tmp_path: Path) -> None:
    jobs = [
        RaonBatchJob(
            source_slug="vworldkr__a",
            operation="a",
            download_ds_id="ds",
            file_no="1",
            provider_file_id="ds-1",
            provider_file_name="a.zip",
            dataset_name="A",
        ),
        RaonBatchJob(
            source_slug="vworldkr__b",
            operation="b",
            download_ds_id="ds",
            file_no="2",
            provider_file_id="ds-2",
            provider_file_name="b.zip",
            dataset_name="B",
        ),
    ]

    def acquire(job: RaonBatchJob, paths) -> RaonBatchAcquireResult:
        if job.provider_file_id == "ds-1":
            raise RuntimeError("provider_timeout")
        paths.private_replay_request_path.write_text("{}", encoding="utf-8")
        return RaonBatchAcquireResult(observed_filename="ok.zip")

    def import_replay(job: RaonBatchJob, env: dict[str, str], paths) -> dict[str, object]:
        return {
            "validation_status": "committed_without_landing",
            "bronze_object_key": f"bronze/source={job.source_slug}/{job.provider_file_id}.zip",
            "bronze_object_id": job.provider_file_id,
            "size_bytes": 1,
            "checksum_sha256": "b" * 64,
        }

    summary = run_batch(
        jobs=jobs,
        batch_id="batch-002",
        output_root=tmp_path,
        acquire=acquire,
        import_replay=import_replay,
        base_env={},
    )

    assert summary["attempted_count"] == 2
    assert summary["committed_count"] == 1
    assert summary["failed_count"] == 1
    assert [result["status"] for result in summary["results"]] == ["failed", "committed"]
    assert summary["results"][0]["error_kind"] == "provider_timeout"


def test_load_selection_filters_limits_and_shards_jobs(tmp_path: Path) -> None:
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "source_slug": "vworldkr__a",
                        "operation": "a",
                        "download_ds_id": "ds",
                        "file_no": "1",
                        "provider_file_id": "ds-1",
                        "provider_file_name": "a.zip",
                        "dataset_name": "A",
                    },
                    {
                        "source_slug": "vworldkr__a",
                        "operation": "a",
                        "download_ds_id": "ds",
                        "file_no": "2",
                        "provider_file_id": "ds-2",
                        "provider_file_name": "a.zip",
                        "dataset_name": "A",
                    },
                    {
                        "source_slug": "vworldkr__b",
                        "operation": "b",
                        "download_ds_id": "ds",
                        "file_no": "3",
                        "provider_file_id": "ds-3",
                        "provider_file_name": "b.zip",
                        "dataset_name": "B",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    jobs = load_selection(
        selection_path,
        source_slugs={"vworldkr__a"},
        shard_index=1,
        shard_count=2,
        limit=1,
    )

    assert [job.provider_file_id for job in jobs] == ["ds-2"]
