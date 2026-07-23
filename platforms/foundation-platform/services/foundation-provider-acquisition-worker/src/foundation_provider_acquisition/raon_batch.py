from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from foundation_provider_acquisition.raon import (
    DEFAULT_BROWSER_USER_AGENT,
    acquire_raon_replay,
    fetch_vworld_cookie_header,
    load_env_file,
    write_private_replay_request,
    write_replay_proof,
)


@dataclass(frozen=True)
class RaonBatchJob:
    source_slug: str
    operation: str
    download_ds_id: str
    file_no: str
    provider_file_id: str
    provider_file_name: str
    dataset_name: str
    base_ym: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class RaonBatchPaths:
    job_root: Path
    public_proof_path: Path
    private_replay_request_path: Path
    import_report_path: Path
    staging_dir: Path
    landing_object_key: str


@dataclass(frozen=True)
class RaonBatchAcquireResult:
    observed_filename: str


AcquireFn = Callable[[RaonBatchJob, RaonBatchPaths], RaonBatchAcquireResult]
ImportFn = Callable[[RaonBatchJob, dict[str, str], RaonBatchPaths], dict[str, object]]


def load_selection(
    path: Path,
    *,
    source_slugs: set[str] | None = None,
    provider_file_ids: set[str] | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
    limit: int | None = None,
) -> list[RaonBatchJob]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get("items") or payload.get("jobs")
    if not isinstance(items, list):
        raise ValueError("selection must contain an items or jobs array")

    jobs = [_job_from_item(item) for item in items]
    if source_slugs is not None:
        jobs = [job for job in jobs if job.source_slug in source_slugs]
    if provider_file_ids is not None:
        jobs = [job for job in jobs if job.provider_file_id in provider_file_ids]
    if shard_count is not None:
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        if shard_index is None or shard_index < 0 or shard_index >= shard_count:
            raise ValueError("shard_index must be in [0, shard_count)")
        jobs = [job for index, job in enumerate(jobs) if index % shard_count == shard_index]
    if limit is not None:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        jobs = jobs[:limit]
    return jobs


def run_batch(
    *,
    jobs: Sequence[RaonBatchJob],
    batch_id: str,
    output_root: Path,
    acquire: AcquireFn,
    import_replay: ImportFn,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    batch_root = output_root / batch_id
    batch_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []
    env = dict(base_env or os.environ)

    for job in jobs:
        paths = _paths_for_job(batch_root, batch_id, job)
        paths.job_root.mkdir(parents=True, exist_ok=True)
        paths.staging_dir.mkdir(parents=True, exist_ok=True)
        try:
            acquired = acquire(job, paths)
            import_env = _build_import_env(
                env,
                job=job,
                paths=paths,
                observed_filename=acquired.observed_filename,
            )
            report = import_replay(job, import_env, paths)
            result = _committed_result(job, paths, report, acquired.observed_filename)
        except Exception as error:
            result = _failed_result(job, paths, error)
        finally:
            _remove_private_artifacts(paths)

        results.append(result)
        _write_summary(batch_root / "summary.json", batch_id, len(jobs), results)

    return _summary(batch_id, len(jobs), results)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a V-World RAON direct-to-Bronze batch")
    parser.add_argument("--selection", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--env-file")
    parser.add_argument("--source-slug", action="append", dest="source_slugs")
    parser.add_argument("--provider-file-id", action="append", dest="provider_file_ids")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int)
    parser.add_argument("--rust-binary", default="foundation-outbox-publisher")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--no-vworld-login", action="store_true")
    args = parser.parse_args(argv)

    env = dict(os.environ)
    if args.env_file:
        env.update(load_env_file(Path(args.env_file)))

    jobs = load_selection(
        Path(args.selection),
        source_slugs=set(args.source_slugs) if args.source_slugs else None,
        provider_file_ids=set(args.provider_file_ids) if args.provider_file_ids else None,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        limit=args.limit,
    )
    acquire = _default_acquire_fn(
        env,
        headed=args.headed,
        use_vworld_login=not args.no_vworld_login,
    )
    import_replay = _default_import_fn(args.rust_binary)
    summary = run_batch(
        jobs=jobs,
        batch_id=args.batch_id,
        output_root=Path(args.output_root),
        acquire=acquire,
        import_replay=import_replay,
        base_env=env,
    )
    return 0 if summary["failed_count"] == 0 else 2


def _job_from_item(item: object) -> RaonBatchJob:
    if not isinstance(item, dict):
        raise ValueError("selection item must be an object")
    return RaonBatchJob(
        source_slug=_required(item, "source_slug"),
        operation=_required(item, "operation"),
        download_ds_id=_required(item, "download_ds_id"),
        file_no=_required(item, "file_no"),
        provider_file_id=_required(item, "provider_file_id"),
        provider_file_name=_required(item, "provider_file_name"),
        dataset_name=_required(item, "dataset_name"),
        base_ym=_optional(item, "base_ym"),
        updated_at=_optional(item, "updated_at"),
    )


def _paths_for_job(batch_root: Path, batch_id: str, job: RaonBatchJob) -> RaonBatchPaths:
    job_id = _safe_job_id(f"{batch_id}-{job.provider_file_id}")
    job_root = batch_root / job_id
    landing_object_key = (
        "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
        f"job_id={job_id}/download_ds_id={job.download_ds_id}/file_no={job.file_no}/download.zip"
    )
    return RaonBatchPaths(
        job_root=job_root,
        public_proof_path=job_root / "raon-agent-proof.json",
        private_replay_request_path=job_root / "private-replay-request.json",
        import_report_path=job_root / "rust-import-bronze-report.json",
        staging_dir=job_root / "staging",
        landing_object_key=landing_object_key,
    )


def _build_import_env(
    base_env: Mapping[str, str],
    *,
    job: RaonBatchJob,
    paths: RaonBatchPaths,
    observed_filename: str,
) -> dict[str, str]:
    env = dict(base_env)
    env.pop("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_LANDING_PAYLOAD_PATH", None)
    env["FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER"] = "r2"
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH"] = str(
        paths.private_replay_request_path
    )
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_IMPORT_OUTPUT_PATH"] = str(paths.import_report_path)
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_STAGING_DIR"] = str(paths.staging_dir)
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE"] = "1"
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE"] = "1"
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_SLUG"] = job.source_slug
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_NAME"] = (
        f"VWorld {job.operation} dataset file"
    )
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER"] = "VWorld"
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DATASET_NAME"] = job.dataset_name
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BASE_URI"] = "https://www.vworld.kr"
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_TERMS_URL"] = (
        "https://www.vworld.kr/dtmk/dtmk_ntads_s001.do"
    )
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_OPERATION"] = job.operation
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_ID"] = job.provider_file_id
    env["FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_NAME"] = (
        observed_filename or job.provider_file_name
    )
    period, snapshot_date = _snapshot_parts(job.base_ym)
    _set_optional_env(env, "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_PERIOD", period)
    _set_optional_env(
        env,
        "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_SNAPSHOT_DATE",
        snapshot_date,
    )
    _set_optional_env(
        env,
        "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_UPDATED_AT",
        job.updated_at,
    )
    return env


def _default_acquire_fn(
    env: Mapping[str, str],
    *,
    headed: bool,
    use_vworld_login: bool,
) -> AcquireFn:
    cookie_header = env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER")
    user_agent = env.get(
        "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_USER_AGENT",
        env.get("FOUNDATION_PLATFORM_VWORLD_USER_AGENT", DEFAULT_BROWSER_USER_AGENT),
    )
    if use_vworld_login and not cookie_header:
        cookie_header = fetch_vworld_cookie_header(
            username=env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME")
            or env.get("VWORLD_USERNAME", ""),
            password=env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD")
            or env.get("VWORLD_PASSWORD", ""),
            user_agent=user_agent,
        )

    def acquire(job: RaonBatchJob, paths: RaonBatchPaths) -> RaonBatchAcquireResult:
        acquisition = acquire_raon_replay(
            download_ds_id=job.download_ds_id,
            file_no=job.file_no,
            headless=not headed,
            cookie_header=cookie_header,
            user_agent=user_agent,
            landing_object_key=paths.landing_object_key,
        )
        write_replay_proof(paths.public_proof_path, acquisition.proof)
        if acquisition.private_request is None:
            raise RuntimeError("missing_private_replay_request")
        write_private_replay_request(paths.private_replay_request_path, acquisition.private_request)
        if not acquisition.proof.replay_looks_zip:
            raise RuntimeError(acquisition.proof.error_message or "replay_not_zip")
        return RaonBatchAcquireResult(
            observed_filename=acquisition.proof.download_event_filename
            or job.provider_file_name
        )

    return acquire


def _default_import_fn(rust_binary: str) -> ImportFn:
    def import_replay(
        job: RaonBatchJob,
        env: dict[str, str],
        paths: RaonBatchPaths,
    ) -> dict[str, object]:
        completed = subprocess.run(
            [rust_binary, "import-provider-acquisition-landing"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        (paths.job_root / "rust-import.stdout.log").write_text(
            completed.stdout,
            encoding="utf-8",
        )
        (paths.job_root / "rust-import.stderr.log").write_text(
            completed.stderr,
            encoding="utf-8",
        )
        if completed.returncode != 0:
            raise RuntimeError(f"rust_import_exit_{completed.returncode}")
        return json.loads(paths.import_report_path.read_text(encoding="utf-8"))

    return import_replay


def _committed_result(
    job: RaonBatchJob,
    paths: RaonBatchPaths,
    report: Mapping[str, object],
    observed_filename: str,
) -> dict[str, object]:
    return {
        "source_slug": job.source_slug,
        "operation": job.operation,
        "download_ds_id": job.download_ds_id,
        "file_no": job.file_no,
        "provider_file_id": job.provider_file_id,
        "status": "committed",
        "job_id": paths.job_root.name,
        "object_key": report.get("object_key"),
        "bronze_object_key": report.get("bronze_object_key"),
        "bronze_object_id": report.get("bronze_object_id"),
        "size_bytes": report.get("size_bytes"),
        "checksum_sha256": report.get("checksum_sha256"),
        "observed_filename": observed_filename,
        "snapshot_input": job.base_ym,
        "updated_at": job.updated_at,
    }


def _failed_result(job: RaonBatchJob, paths: RaonBatchPaths, error: Exception) -> dict[str, object]:
    return {
        "source_slug": job.source_slug,
        "operation": job.operation,
        "download_ds_id": job.download_ds_id,
        "file_no": job.file_no,
        "provider_file_id": job.provider_file_id,
        "status": "failed",
        "job_id": paths.job_root.name,
        "error_kind": str(error),
        "snapshot_input": job.base_ym,
        "updated_at": job.updated_at,
    }


def _write_summary(
    path: Path,
    batch_id: str,
    selected_count: int,
    results: Sequence[Mapping[str, object]],
) -> None:
    path.write_text(
        json.dumps(_summary(batch_id, selected_count, results), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _summary(
    batch_id: str,
    selected_count: int,
    results: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return {
        "schema_version": "foundation-platform.provider_acquisition_raon_batch.v1",
        "batch_id": batch_id,
        "selected_count": selected_count,
        "attempted_count": len(results),
        "committed_count": sum(1 for result in results if result.get("status") == "committed"),
        "failed_count": sum(1 for result in results if result.get("status") == "failed"),
        "direct_to_bronze": True,
        "full_collection_started": bool(results),
        "full_collection_finished": len(results) == selected_count,
        "results": list(results),
    }


def _remove_private_artifacts(paths: RaonBatchPaths) -> None:
    paths.private_replay_request_path.unlink(missing_ok=True)
    if paths.staging_dir.exists():
        shutil.rmtree(paths.staging_dir)


def _snapshot_parts(value: str | None) -> tuple[str | None, str | None]:
    if value is None or not value.strip() or value == "-":
        return None, None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return None, value
    if re.fullmatch(r"\d{4}-\d{2}", value):
        return value, None
    return None, None


def _safe_job_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "-", value)


def _required(item: Mapping[str, Any], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"selection item missing {key}")
    return value


def _optional(item: Mapping[str, Any], key: str) -> str | None:
    value = item.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"selection item {key} must be a string")
    return value


def _set_optional_env(env: dict[str, str], name: str, value: str | None) -> None:
    if value is None or not value.strip() or value == "-":
        env.pop(name, None)
    else:
        env[name] = value


if __name__ == "__main__":
    raise SystemExit(main())
