use super::*;
use foundation_shared_kernel::ids::StaffId;

fn smoke_config() -> RemoteLakehouseJobConfig {
    RemoteLakehouseJobConfig {
        ssh_target: "perfectory@lakehouse.internal.test".to_owned(),
        remote_root: "/home/perfectory/foundation-platform-compute".to_owned(),
        env_file: ".env.lakehouse".to_owned(),
        ssh_path: "ssh".to_owned(),
        execute: false,
        job: RemoteLakehouseJob::Smoke,
        input_path_override: None,
        input_file_batch_size_override: None,
        source_snapshot: BuildingRegisterSourceSnapshotConfig::NotRequired,
        floor_source: None,
        local: None,
        audit: RemoteLakehouseAuditConfig::Disabled,
        // The real coordinates, not a stand-in: the assertions below pin the exact remote
        // command, so a fixture that invented its own packages would pass while the shipped
        // command loaded something else.
        native_execution_profile: crate::lakehouse_engine_contract::execution_profile()
            .expect("the native engine profile must resolve"),
        iceberg_packages: crate::lakehouse_engine_contract::iceberg_packages()
            .expect("the embedded engine contract must resolve")
            .to_owned(),
    }
}

fn history_fixture() -> anyhow::Result<
    foundation_outbox_publisher::building_register_floor_silver_export::HistoryWitness,
> {
    foundation_outbox_publisher::building_register_floor_silver_export::HistoryWitness::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../infra/lakehouse/spark/tests/fixtures/building_register_floor_history.json")
            .canonicalize()?,
        None,
    )
}

fn synthetic_snapshot_config(job: &str) -> anyhow::Result<RemoteLakehouseJobConfig> {
    let history_path = history_fixture()?.path().display().to_string();
    RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB" => Some(job.to_owned()),
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH" => Some(history_path.clone()),
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_SNAPSHOT_DATE" => {
            Some("2099-12-31".to_owned())
        }
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC" => {
            Some("2099-12-31T12:00:00Z".into())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_FLOOR_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-FLOOR.zip".into())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-UNIT.zip".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-TITLE.zip".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_BASIS_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-BASIS.zip".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_AREA_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-UNIT-AREA.zip".to_owned())
        }
        _ => None,
    })
}

#[test]
fn scalar_handoff_plan_runs_remote_spark_without_secret_values() {
    let plan = build_remote_lakehouse_job_plan(&smoke_config());

    assert_eq!(plan.program, "ssh");
    assert_eq!(plan.args[0], "perfectory@lakehouse.internal.test");
    assert!(plan
        .remote_script
        .contains("silver_scalar_handoff_to_lakehouse.py"));
    assert!(plan
        .remote_script
        .contains("--contract silver.building_register_floors"));
    assert!(plan.remote_script.contains("--write-mode iceberg"));
    assert!(plan
        .remote_script
        .contains("--iceberg-write-mode overwrite"));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_SPARK_SKIP_STOP_ON_SUCCESS=1"));
    assert!(plan
        .remote_script
        .contains("--conf spark.driver.extraJavaOptions=-Xint"));
    assert!(plan
        .remote_script
        .contains("--defer-iceberg-readback-validation"));
    assert!(plan.remote_script.contains("--input-file-batch-size 1"));
    assert!(plan
        .remote_script
        .contains("SELECT count(*) FROM building_register_floors_smoke"));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_floors_smoke"));
    assert!(plan
        .remote_script
        .contains("-e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN"));
    assert!(plan
        .remote_script
        .contains("cat 'target/lakehouse/smoke/building_register_floors-summary.json'"));
    assert!(!plan.remote_script.contains("perfect12"));
    assert!(!plan.remote_script.contains("<catalog-token>"));
}

#[test]
fn scalar_handoff_plan_fails_when_catalog_bucket_does_not_match_r2_bucket() {
    let plan = build_remote_lakehouse_job_plan(&smoke_config());

    assert!(plan
        .remote_script
        .contains("lakehouse catalog bucket mismatch"));
    assert!(plan
        .remote_script
        .contains("catalog_bucket=\"${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI##*/}\""));
    assert!(plan.remote_script.contains(
        "if [ \"$catalog_bucket\" != \"$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET\" ]; then"
    ));
}

#[test]
fn scalar_handoff_plan_renders_trino_catalog_from_remote_env_before_readback() {
    let plan = build_remote_lakehouse_job_plan(&smoke_config());

    let render_pos = plan
        .remote_script
        .find("render_trino_catalog_from_env")
        .expect("remote script should render the Trino catalog from .env.lakehouse");
    let trino_up_pos = plan
        .remote_script
        .find("docker compose -f compose.lakehouse.yml --profile lakehouse-query up")
        .expect("remote script should start Trino for readback validation");
    assert!(render_pos < trino_up_pos);
    assert!(plan
        .remote_script
        .contains("infra/lakehouse/trino/catalog/r2.properties"));
    assert!(plan
        .remote_script
        .contains("iceberg.rest-catalog.uri=${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI}"));
    assert!(plan.remote_script.contains(
        "iceberg.rest-catalog.oauth2.server-uri=${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI%/}/v1/oauth/tokens"
    ));
    assert!(plan
        .remote_script
        .contains("docker compose -f compose.lakehouse.yml --profile lakehouse-query up -d --force-recreate trino"));
    assert!(!plan
        .remote_script
        .contains("docker compose --profile lakehouse"));
}

#[test]
fn handoff_smoke_plan_uses_exported_handoff_without_fixture_row_count() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_floors_handoff_smoke")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan.remote_script.contains(
        "--input '/workspace/target/lakehouse/silver_handoff/building_register_floors.jsonl'"
    ));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_floors_smoke"));
    assert!(plan
        .remote_script
        .contains("find -L 'target/lakehouse/silver_handoff/building_register_floors.jsonl' -type f -size +0c -print -quit"));
    assert!(!plan.remote_script.contains("| grep -q"));
    assert!(!plan
        .remote_script
        .contains("fixtures/silver_handoff/building_register_floors.jsonl"));
    assert!(!plan.remote_script.contains("--expected-count 2"));
    Ok(())
}

#[test]
fn handoff_smoke_plan_allows_chunked_input_directory_override() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_floors_handoff_smoke")?;
    config.input_path_override =
        Some("target/lakehouse/silver_handoff/building_register_floors_hub_chunks".to_owned());

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan.remote_script.contains(
        "--input '/workspace/target/lakehouse/silver_handoff/building_register_floors_hub_chunks'"
    ));
    assert!(plan.remote_script.contains(
        "find -L 'target/lakehouse/silver_handoff/building_register_floors_hub_chunks' -type f -size +0c -print -quit"
    ));
    assert!(!plan.remote_script.contains("| grep -q"));
    assert!(!plan.remote_script.contains(
        "--input '/workspace/target/lakehouse/silver_handoff/building_register_floors.jsonl'"
    ));
    Ok(())
}

#[test]
fn handoff_full_jsonl_job_is_not_supported_for_non_smoke_loads() {
    let error = RemoteLakehouseJob::parse("building_register_floors_handoff_full")
        .expect_err("full JSONL handoff must not remain as an executable final-load job");

    assert!(error
        .to_string()
        .contains("unknown remote lakehouse job: building_register_floors_handoff_full"));
}

#[test]
fn pipeline_smoke_plan_exports_handoff_before_spark_write() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_floors_pipeline_smoke")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    let export_pos = plan
        .remote_script
        .find("export-building-register-floor-silver-handoff")
        .expect("export command should be present");
    let spark_pos = plan
        .remote_script
        .find("silver_scalar_handoff_to_lakehouse.py")
        .expect("spark command should be present");
    assert!(export_pos < spark_pos);
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_BRONZE_ROOT"));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_PATH"));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT='parquet'"
    ));
    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_floors_parquet_smoke"));
    assert!(plan.remote_script.contains("--input-format parquet"));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID"));
    assert!(!plan.remote_script.contains(
        "--input '/workspace/target/lakehouse/silver_handoff/building_register_floors.jsonl'"
    ));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_floors_smoke"));
    assert!(!plan.remote_script.contains("--expected-count 2"));
    Ok(())
}

#[test]
fn pipeline_smoke_plan_uses_containerized_control_runner() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_floors_pipeline_smoke")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("docker compose -f compose.lakehouse.yml -f compose.lakehouse-native.yml --profile lakehouse-control run --rm"));
    assert!(plan.remote_script.contains("--user \"$(id -u):$(id -g)\""));
    assert!(plan.remote_script.contains("lakehouse-control"));
    assert!(!plan.remote_script.contains("command -v cargo"));
    assert!(!plan
        .remote_script
        .contains("cargo run -q -p foundation-outbox-publisher"));
    Ok(())
}

#[test]
fn pipeline_full_plan_uses_parquet_handoff_without_jsonl_full_bloat() -> anyhow::Result<()> {
    let config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    let plan = build_remote_lakehouse_job_plan(&config);
    let script = &plan.remote_script;
    assert!(script.contains("export-building-register-floor-silver-handoff"));
    assert!(script.contains("FLOOR_VERIFY_BRONZE_LEDGER=1"));
    assert!(script.contains("FLOOR_REUSE_COMPLETED_HANDOFF=1"));
    assert!(script.contains("SOURCE_OBJECT='OPN20991231SYNTHETIC-FLOOR.zip'"));
    assert!(script.contains("TITLE_SOURCE_OBJECT='OPN20991231SYNTHETIC-TITLE.zip'"));
    assert!(script.contains("RETAINED_INGESTED_AT_UTC='2099-12-31T12:00:00+00:00'"));
    assert!(script.contains("--input-format parquet"));
    assert!(script.contains("--iceberg-write-mode append"));
    assert!(script.contains("--input-file-batch-size 0"));
    assert!(!script.contains("--defer-iceberg-readback-validation"));
    assert!(!script.contains("--allow-non-smoke-overwrite"));
    assert!(!script.contains("SELECT count(*) FROM"));
    assert!(!script.contains("$(date"));
    assert!(!script.contains("--derivation"));
    assert!(script.contains("-e DATABASE_URL "));
    assert!(!script.contains("-e DATABASE_URL="));
    let root = config.floor_source.as_ref().context("floor config")?.root();
    assert!(script.contains(&format!("--input '/workspace/{root}/handoff'")));
    assert!(script.contains(&format!("OUTPUT_PATH='{root}/handoff'")));
    Ok(())
}

#[test]
fn manual_whole_floor_execution_is_retired_but_review_and_other_jobs_remain() -> anyhow::Result<()>
{
    let mut config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    config.execute = false;
    validate_remote_execution(&config)?;
    assert!(!build_remote_lakehouse_job_plan(&config)
        .remote_script
        .is_empty());
    config.execute = true;
    let error = validate_remote_execution(&config).expect_err("manual FLOOR must be rejected");
    assert!(error
        .to_string()
        .contains("foundation-building-register-floor.service"));
    assert!(error
        .to_string()
        .contains("run-building-register-floor-cycle"));
    for job in [
        RemoteLakehouseJob::Smoke,
        RemoteLakehouseJob::HandoffSmoke,
        RemoteLakehouseJob::PipelineSmoke,
        RemoteLakehouseJob::PipelineHubSmoke,
        RemoteLakehouseJob::UnitPipelineFull,
        RemoteLakehouseJob::UnitAreaPipelineFull,
    ] {
        config.job = job;
        validate_remote_execution(&config)?;
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn manual_whole_floor_rejection_never_starts_ssh() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir()?;
    let ssh = dir.path().join("ssh");
    let marker = dir.path().join("called");
    std::fs::write(
        &ssh,
        format!(
            "#!/bin/sh\ntouch {}\n",
            shell_quote(&marker.to_string_lossy())
        ),
    )?;
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755))?;
    let mut config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    config.execute = true;
    config.ssh_path = ssh.to_string_lossy().into_owned();
    let result = run_config(config)
        .await
        .expect_err("manual FLOOR must be rejected");
    assert!(result
        .to_string()
        .contains("run-building-register-floor-cycle"));
    assert!(!marker.exists());
    Ok(())
}

#[test]
fn pipeline_hub_smoke_uses_hub_bronze_but_smoke_iceberg_table() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_floors_pipeline_hub_smoke")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG='hubgokr__building_register_floor_overview'"));
    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_floors_hub_parquet_smoke"));
    assert!(plan.remote_script.contains("--input-format parquet"));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_floors_smoke"));
    assert!(plan.remote_script.contains("--master local[*]"));
    assert!(plan.remote_script.contains("--driver-memory 12g"));
    assert!(!plan.remote_script.contains("-Xint"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_CHUNK_ROWS='250000'"
    ));
    assert!(!plan.remote_script.contains("--allow-non-smoke-overwrite"));
    assert!(plan.remote_script.contains("--input-file-batch-size 128"));
    assert!(!plan
        .remote_script
        .contains("--iceberg-table building_register_floors\n"));
    Ok(())
}

#[test]
fn unit_pipeline_smoke_exports_units_before_spark_write() -> anyhow::Result<()> {
    let config = synthetic_snapshot_config("building_register_units_pipeline_smoke")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    let export_pos = plan
        .remote_script
        .find("export-building-register-unit-silver-handoff")
        .expect("unit export command should be present");
    let stage_unit_pos = plan
        .remote_script
        .find("stage_bronze_object 'hubgokr__building_register_exclusive_unit' 'OPN20991231SYNTHETIC-UNIT.zip'")
        .expect("pinned unit Bronze zip should be staged from R2");
    let stage_title_pos = plan
        .remote_script
        .find("stage_bronze_object 'hubgokr__building_register_main' 'OPN20991231SYNTHETIC-TITLE.zip'")
        .expect("pinned title Bronze zip should be staged from R2");
    let spark_pos = plan
        .remote_script
        .find("silver_scalar_handoff_to_lakehouse.py")
        .expect("spark command should be present");
    assert!(stage_unit_pos < export_pos);
    assert!(stage_title_pos < export_pos);
    assert!(export_pos < spark_pos);
    assert!(plan.remote_script.contains("amazon/aws-cli"));
    assert!(plan
        .remote_script
        .contains("s3://$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET/bronze/source=${source_slug}/"));
    assert!(plan.remote_script.contains(
        "--user \"${FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}:${FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}\""
    ));
    assert!(!plan.remote_script.contains("FOUNDATION_PLATFORM_SPARK_UID"));
    assert!(!plan.remote_script.contains("FOUNDATION_PLATFORM_SPARK_GID"));
    assert!(plan.remote_script.contains("chown -R"));
    assert!(plan
        .remote_script
        .contains("$PWD/target/remote-lakehouse:/remote-lakehouse"));
    assert!(plan.remote_script.contains("--only-show-errors"));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_BRONZE_ROOT"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_SLUG='hubgokr__building_register_exclusive_unit'"
    ));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_TITLE_SOURCE_SLUG='hubgokr__building_register_main'"
    ));
    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_units_smoke.jsonl"));
    assert!(plan
        .remote_script
        .contains("--contract silver.building_register_units"));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_units_smoke"));
    assert!(plan
        .remote_script
        .contains("SELECT count(*) FROM building_register_units_smoke"));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_MAX_ROWS='10000'"));
    assert!(plan
        .remote_script
        .contains("DATABASE_URL is required for building-register unit Silver override load"));
    assert!(plan
        .remote_script
        .contains("-e DATABASE_URL=\"$DATABASE_URL\""));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_APPLY_APPROVED_OVERRIDES='1'"
    ));
    assert!(!plan.remote_script.contains("--allow-non-smoke-overwrite"));
    Ok(())
}

#[test]
fn unit_pipeline_full_exports_all_units_to_canonical_table() -> anyhow::Result<()> {
    let config = synthetic_snapshot_config("building_register_units_pipeline_full")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("stage_bronze_object 'hubgokr__building_register_exclusive_unit' 'OPN20991231SYNTHETIC-UNIT.zip'"));
    assert!(plan.remote_script.contains(
        "stage_bronze_object 'hubgokr__building_register_main' 'OPN20991231SYNTHETIC-TITLE.zip'"
    ));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_OBJECT='OPN20991231SYNTHETIC-UNIT.zip'"
    ));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_TITLE_SOURCE_OBJECT='OPN20991231SYNTHETIC-TITLE.zip'"
    ));
    // valid_from = 스냅샷 날짜 (실행일 위조 금지) + snapshot id에 날짜 각인.
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_VALID_FROM_UTC='2099-12-31T00:00:00Z'"
    ));
    assert!(!plan
        .remote_script
        .contains("UNIT_SILVER_HANDOFF_VALID_FROM_UTC=\"$(date"));
    assert!(plan
        .remote_script
        .contains("remote-building-register-unit-pipeline-full-20991231-"));
    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_units_parquet"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_OUTPUT_FORMAT='parquet'"
    ));
    assert!(plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_CHUNK_ROWS='250000'"));
    assert!(plan.remote_script.contains("--input-format parquet"));
    assert!(plan.remote_script.contains("--input-file-batch-size 128"));
    assert!(plan.remote_script.contains(
        "target/remote-lakehouse/summaries/building_register_units-export-full-summary.json"
    ));
    assert!(plan
        .remote_script
        .contains("--contract silver.building_register_units"));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_units"));
    assert!(plan
        .remote_script
        .contains("SELECT count(*) FROM building_register_units"));
    assert!(plan.remote_script.contains("--allow-non-smoke-overwrite"));
    assert!(!plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_MAX_ROWS"));
    assert!(plan
        .remote_script
        .contains("DATABASE_URL is required for building-register unit Silver override load"));
    assert!(plan
        .remote_script
        .contains("-e DATABASE_URL=\"$DATABASE_URL\""));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_APPLY_APPROVED_OVERRIDES='1'"
    ));
    assert!(!plan.remote_script.contains("building_register_units.jsonl"));
    assert!(!plan.remote_script.contains("building_register_units_smoke"));
    Ok(())
}

#[test]
fn unit_area_pipeline_smoke_writes_smoke_table_with_row_cap() -> anyhow::Result<()> {
    let config = synthetic_snapshot_config("building_register_unit_areas_pipeline_smoke")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_unit_areas_smoke.jsonl"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_FORMAT='jsonl'"
    ));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_MAX_ROWS='10000'"
    ));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_unit_areas_smoke"));
    assert!(plan
        .remote_script
        .contains("--contract silver.building_register_unit_areas"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_OBJECT='OPN20991231SYNTHETIC-UNIT-AREA.zip'"
    ));
    Ok(())
}

#[test]
fn unit_area_pipeline_full_exports_all_areas_to_canonical_table() -> anyhow::Result<()> {
    let config = synthetic_snapshot_config("building_register_unit_areas_pipeline_full")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_unit_areas_parquet"));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_FORMAT='parquet'"
    ));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_CHUNK_ROWS='250000'"
    ));
    assert!(plan.remote_script.contains("--input-format parquet"));
    assert!(plan.remote_script.contains("--input-file-batch-size 128"));
    assert!(plan.remote_script.contains(
        "target/remote-lakehouse/summaries/building_register_unit_areas-export-full-summary.json"
    ));
    assert!(plan
        .remote_script
        .contains("--contract silver.building_register_unit_areas"));
    assert!(plan
        .remote_script
        .contains("--iceberg-table building_register_unit_areas"));
    assert!(plan
        .remote_script
        .contains("SELECT count(*) FROM building_register_unit_areas"));
    assert!(plan.remote_script.contains("--allow-non-smoke-overwrite"));
    // High-fanout writes use explicit concurrency and heap bounds instead of
    // inheriting host-wide parallelism.
    assert!(plan.remote_script.contains("--master local[8]"));
    assert!(plan.remote_script.contains("--driver-memory 28g"));
    assert!(!plan
        .remote_script
        .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_MAX_ROWS"));
    // 면적 파이프라인은 승인 override가 없어 DB 접속이 필요 없고 (자격증명
    // env 파일 불요), PK 직결이라 표제부 스테이징도 없다.
    assert!(!plan.remote_script.contains("DATABASE_URL"));
    assert!(!plan
        .remote_script
        .contains("hubgokr__building_register_main"));
    assert!(!plan
        .remote_script
        .contains("building_register_unit_areas_smoke"));
    Ok(())
}

#[test]
fn unit_area_pipeline_pins_snapshot_and_stages_only_that_object() -> anyhow::Result<()> {
    // Guard against implicit prefix selection, run-date substitution, unbounded
    // staging, and output-directory ownership drift.
    let config = synthetic_snapshot_config("building_register_unit_areas_pipeline_full")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    // Exactly one configured object is staged; accumulated prefix contents can
    // never silently choose a different snapshot.
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_OBJECT='OPN20991231SYNTHETIC-UNIT-AREA.zip'"
    ));
    assert!(plan
        .remote_script
        .contains("--include 'OPN20991231SYNTHETIC-UNIT-AREA.zip'"));
    assert!(plan.remote_script.contains("--exclude '*'"));

    // valid_from = 스냅샷 날짜 (실행일 아님) — zip 이름의 날짜와 일치해야 한다.
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_VALID_FROM_UTC='2099-12-31T00:00:00Z'"
    ));
    assert!(!plan
        .remote_script
        .contains("UNIT_AREA_SILVER_HANDOFF_VALID_FROM_UTC=\"$(date"));

    // snapshot id 에 스냅샷 날짜가 실린다.
    assert!(plan
        .remote_script
        .contains("remote-building-register-unit-area-pipeline-full-20991231-"));

    // 신규 호스트에서도 uid 185 export 가 출력 디렉터리를 만들 수 있다.
    assert!(plan.remote_script.contains("/lakehouse/silver_handoff"));

    // 스펙 자체 정합: 핀 객체명에 박힌 날짜 == valid_from 날짜.
    let source = match &config.source_snapshot {
        BuildingRegisterSourceSnapshotConfig::UnitArea(source) => source,
        BuildingRegisterSourceSnapshotConfig::NotRequired
        | BuildingRegisterSourceSnapshotConfig::Unit(_) => {
            bail!("unit-area job must carry unit-area source metadata")
        }
    };
    assert_eq!(source.metadata.compact_date(), "20991231");
    assert_eq!(source.metadata.valid_from_utc(), "2099-12-31T00:00:00Z");
    Ok(())
}

#[test]
fn unit_floor_resolutions_full_exports_table_without_touching_silver() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_unit_floor_resolutions_full")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("building_register_unit_floor_resolution_export.py"));
    assert!(plan.remote_script.contains(
        "--units-parquet /workspace/target/lakehouse/silver_handoff/building_register_units_parquet"
    ));
    assert!(plan.remote_script.contains(
        "--areas-parquet /workspace/target/lakehouse/silver_handoff/building_register_unit_areas_parquet"
    ));
    assert!(plan
        .remote_script
        .contains("target/remote-lakehouse/resolutions/building_register_unit_floor_resolutions"));
    assert!(plan.remote_script.contains("--resolved-at"));
    // 교정표는 원본 Silver 불변 — Iceberg 쓰기 경로가 없어야 한다.
    assert!(!plan.remote_script.contains("--iceberg-table"));
    assert!(!plan.remote_script.contains("--write-mode iceberg"));
    Ok(())
}

#[test]
fn unit_floor_resolution_summary_validation_checks_count_consistency() {
    let valid = serde_json::json!({
        "schema_version": "foundation-platform.building_register_unit_floor_resolution.v1",
        "conflict_rows": 84101,
        "resolved_count": 75659,
        "unresolved_count": 8442,
    })
    .to_string();
    assert_eq!(
        validate_unit_floor_resolution_summary(&valid).ok(),
        Some((84101, 75659))
    );

    let inconsistent = serde_json::json!({
        "schema_version": "foundation-platform.building_register_unit_floor_resolution.v1",
        "conflict_rows": 84101,
        "resolved_count": 75659,
        "unresolved_count": 1,
    })
    .to_string();
    assert!(validate_unit_floor_resolution_summary(&inconsistent).is_err());

    let wrong_schema = serde_json::json!({
        "schema_version": "v0",
        "conflict_rows": 1,
        "resolved_count": 1,
        "unresolved_count": 0,
    })
    .to_string();
    assert!(validate_unit_floor_resolution_summary(&wrong_schema).is_err());
}

#[test]
fn unit_proposal_context_full_exports_ai_input_without_rewriting_silver() -> anyhow::Result<()> {
    let mut config = smoke_config();
    config.job = RemoteLakehouseJob::parse("building_register_units_proposals_full")?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("building_register_unit_proposal_context_export.py"));
    assert!(plan
        .remote_script
        .contains("target/remote-lakehouse/ai/building_register_unit_proposals-full"));
    assert!(plan
        .remote_script
        .contains("target/lakehouse/silver_handoff/building_register_units_parquet"));
    assert!(plan
        .remote_script
        .contains("missing or empty building-register unit Silver Parquet handoff"));
    assert!(plan.remote_script.contains("--input-parquet /workspace/target/lakehouse/silver_handoff/building_register_units_parquet"));
    assert!(plan
        .remote_script
        .contains("$PWD/target/remote-lakehouse:/remote-lakehouse"));
    assert!(plan
        .remote_script
        .contains("$PWD/target/remote-lakehouse:/workspace/target/remote-lakehouse"));
    assert!(plan.remote_script.contains("chown -R"));
    assert!(!plan
        .remote_script
        .contains("--table building_register_units"));
    assert!(!plan.remote_script.contains("--packages org.apache.iceberg"));
    assert!(!plan
        .remote_script
        .contains("export-building-register-unit-silver-handoff"));
    assert!(!plan
        .remote_script
        .contains("silver_scalar_handoff_to_lakehouse.py"));
    assert!(!plan
        .remote_script
        .contains("--iceberg-write-mode overwrite"));
    Ok(())
}

#[test]
fn unit_proposal_context_summary_requires_expected_schema() -> anyhow::Result<()> {
    let summary = r#"{
      "schema_version":"foundation-platform.building_register_unit_proposal_context_export_summary.v1",
      "proposal_count":29259,
      "target":{"kind":"ai_proposal_input_jsonl"}
    }"#;

    let proposal_count = validate_unit_proposal_context_summary(summary)?;

    assert_eq!(proposal_count, 29_259);
    Ok(())
}

#[test]
fn extracts_marked_spark_summary_json_from_remote_output() {
    let output = "\
noise
__FOUNDATION_PLATFORM_SPARK_SUMMARY_BEGIN__
{
  \"schema_version\": \"foundation-platform.spark_run_summary.v1\",
  \"contract\": \"silver.building_register_floors\"
}
__FOUNDATION_PLATFORM_SPARK_SUMMARY_END__
more noise
";

    let summary = extract_marked_summary_json(output).expect("summary json");

    assert!(summary.contains("\"contract\": \"silver.building_register_floors\""));
}

#[test]
fn rejects_remote_output_without_marked_summary() {
    let error = extract_marked_summary_json("spark succeeded but summary marker is absent")
        .expect_err("missing marker should fail");

    assert!(error.to_string().contains("summary marker"));
}

#[test]
fn config_from_lookup_is_dry_run_by_default() -> anyhow::Result<()> {
    let config = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some(" perfectory@lakehouse.internal.test ".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some(" /home/perfectory/foundation-platform-compute ".to_owned())
        }
        _ => None,
    })?;

    assert_eq!(config.ssh_target, "perfectory@lakehouse.internal.test");
    assert_eq!(
        config.remote_root,
        "/home/perfectory/foundation-platform-compute"
    );
    assert_eq!(config.env_file, ".env.lakehouse");
    assert_eq!(config.ssh_path, "ssh");
    assert!(!config.execute);
    assert_eq!(config.job, RemoteLakehouseJob::Smoke);
    assert_eq!(config.audit, RemoteLakehouseAuditConfig::Disabled);
    Ok(())
}

#[test]
fn config_from_lookup_rejects_unknown_job() {
    let error = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB" => Some("unknown".to_owned()),
        _ => None,
    })
    .expect_err("unknown job should fail");

    assert!(error.to_string().contains("unknown remote lakehouse job"));
}

#[test]
fn config_from_lookup_requires_snapshot_metadata_for_unit_pipeline() {
    let error = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB" => {
            Some("building_register_units_pipeline_full".to_owned())
        }
        _ => None,
    })
    .expect_err("unit pipeline without private snapshot metadata should fail closed");

    assert!(error
        .to_string()
        .contains("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_SNAPSHOT_DATE"));
}

#[test]
fn config_from_lookup_rejects_invalid_snapshot_date() {
    let error = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB" => {
            Some("building_register_unit_areas_pipeline_full".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_SNAPSHOT_DATE" => {
            Some("2099-02-30".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_AREA_SOURCE_OBJECT" => {
            Some("OPN20990230SYNTHETIC-UNIT-AREA.zip".to_owned())
        }
        _ => None,
    })
    .expect_err("invalid calendar date should fail closed");

    assert!(error
        .to_string()
        .contains("must be a valid YYYY-MM-DD date"));
}

#[test]
fn config_from_lookup_rejects_source_object_date_mismatch() {
    let error = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB" => {
            Some("building_register_unit_areas_pipeline_full".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_SNAPSHOT_DATE" => {
            Some("2099-12-31".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_AREA_SOURCE_OBJECT" => {
            Some("OPN20991230SYNTHETIC-UNIT-AREA.zip".to_owned())
        }
        _ => None,
    })
    .expect_err("object metadata from another snapshot should fail closed");

    assert!(error
        .to_string()
        .contains("must embed snapshot date 20991231"));
}

#[test]
fn valid_synthetic_unit_snapshot_metadata_is_rendered_into_plan() -> anyhow::Result<()> {
    let config = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB" => {
            Some("building_register_units_pipeline_full".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_SNAPSHOT_DATE" => {
            Some("2099-12-31".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-UNIT.zip".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-TITLE.zip".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_BASIS_SOURCE_OBJECT" => {
            Some("OPN20991231SYNTHETIC-BASIS.zip".to_owned())
        }
        _ => None,
    })?;

    let plan = build_remote_lakehouse_job_plan(&config);

    assert!(plan
        .remote_script
        .contains("stage_bronze_object 'hubgokr__building_register_exclusive_unit' 'OPN20991231SYNTHETIC-UNIT.zip'"));
    assert!(plan.remote_script.contains(
        "stage_bronze_object 'hubgokr__building_register_main' 'OPN20991231SYNTHETIC-TITLE.zip'"
    ));
    assert!(plan.remote_script.contains(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_VALID_FROM_UTC='2099-12-31T00:00:00Z'"
    ));
    assert!(plan
        .remote_script
        .contains("remote-building-register-unit-pipeline-full-20991231-"));
    Ok(())
}

#[test]
fn config_from_lookup_requires_staff_id_when_audit_is_enabled() {
    let error = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT" => Some("1".to_owned()),
        _ => None,
    })
    .expect_err("audit recording without staff attribution should fail");

    assert!(error
        .to_string()
        .contains("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID"));
}

#[test]
fn config_from_lookup_parses_audit_staff_id_and_request_id() -> anyhow::Result<()> {
    let staff_uuid = uuid::Uuid::parse_str("018f1111-1111-7111-8111-111111111111")?;
    let config = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT" => Some("true".to_owned()),
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID" => {
            Some(format!(" {staff_uuid} "))
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_REQUEST_ID" => {
            Some(" remote-smoke-2026-07-01 ".to_owned())
        }
        _ => None,
    })?;

    assert_eq!(
        config.audit,
        RemoteLakehouseAuditConfig::Record {
            recorded_by_staff_id: StaffId::new(staff_uuid),
            request_id: Some("remote-smoke-2026-07-01".to_owned())
        }
    );
    Ok(())
}

#[test]
fn config_from_lookup_rejects_nil_audit_staff_id() {
    let error = RemoteLakehouseJobConfig::from_lookup(|name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET" => {
            Some("perfectory@lakehouse.internal.test".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT" => {
            Some("/home/perfectory/foundation-platform-compute".to_owned())
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT" => Some("1".to_owned()),
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID" => {
            Some(uuid::Uuid::nil().to_string())
        }
        _ => None,
    })
    .expect_err("nil staff id should not be accepted for audit attribution");

    assert!(error.to_string().contains("must not be nil"));
}

#[test]
fn audit_record_input_is_absent_when_audit_is_disabled() {
    let input = build_audit_record_input(
        "summary-json".to_owned(),
        &RemoteLakehouseAuditConfig::Disabled,
    );

    assert!(input.is_none());
}

#[test]
fn audit_record_input_preserves_summary_staff_and_request_id() {
    let staff_id = StaffId::new(uuid::Uuid::now_v7());
    let input = build_audit_record_input(
        "summary-json".to_owned(),
        &RemoteLakehouseAuditConfig::Record {
            recorded_by_staff_id: staff_id,
            request_id: Some("lakehouse-smoke".to_owned()),
        },
    )
    .expect("audit input should be built");

    assert_eq!(input.summary_json, "summary-json");
    assert_eq!(input.recorded_by_staff_id, staff_id);
    assert_eq!(input.request_id, Some("lakehouse-smoke".to_owned()));
}

#[test]
fn unit_pipeline_requires_and_stages_same_snapshot_parent_basis() {
    for job in [
        "building_register_units_pipeline_smoke",
        "building_register_units_pipeline_full",
    ] {
        let mut config = synthetic_snapshot_config(job).expect("valid inputs");
        assert!(config.floor_source.is_none());
        config.execute = true;
        validate_remote_execution(&config)
            .expect("the FLOOR-only execution restriction excludes UNIT");
        let script = build_remote_lakehouse_job_plan(&config).remote_script;
        let stage = script
            .find("stage_bronze_object 'hubgokr__building_register_basis_outline' 'OPN20991231SYNTHETIC-BASIS.zip'")
            .expect("the UNIT parent BASIS must be staged");
        let export = script
            .find("export-building-register-unit-silver-handoff")
            .expect("the canonical UNIT exporter must remain selected");
        assert!(stage < export);
        assert!(script.contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_BASIS_SOURCE_OBJECT='OPN20991231SYNTHETIC-BASIS.zip'"));
    }
}

#[test]
fn unit_pipeline_refuses_missing_or_different_snapshot_parent_basis() {
    for basis in [
        None,
        Some("OPN20991230SYNTHETIC-BASIS.zip"),
        Some("OPN20991130SYNTHETIC-BASIS.zip"),
    ] {
        let error = BuildingRegisterSourceSnapshotConfig::from_lookup(
            RemoteLakehouseJob::UnitPipelineFull,
            &mut |name| match name {
                BUILDING_REGISTER_SNAPSHOT_DATE_ENV => Some("2099-12-31".to_owned()),
                BUILDING_REGISTER_UNIT_SOURCE_OBJECT_ENV => {
                    Some("OPN20991231SYNTHETIC-UNIT.zip".to_owned())
                }
                BUILDING_REGISTER_TITLE_SOURCE_OBJECT_ENV => {
                    Some("OPN20991231SYNTHETIC-TITLE.zip".to_owned())
                }
                BUILDING_REGISTER_BASIS_SOURCE_OBJECT_ENV => basis.map(str::to_owned),
                _ => None,
            },
        )
        .expect_err("parent evidence must name the same snapshot");
        assert!(error
            .to_string()
            .contains(BUILDING_REGISTER_BASIS_SOURCE_OBJECT_ENV));
    }
}

#[test]
fn floor_native_invocation_enforces_the_embedded_execution_profile() -> anyhow::Result<()> {
    let config = smoke_config();
    let profile = crate::lakehouse_engine_contract::execution_profile()?;
    let script = build_building_register_floor_pipeline_script(
        &config,
        BuildingRegisterFloorPipelineSpec::full(),
    );
    for (name, value) in [
        ("MEMORY_MIB", u64::from(profile.memory_mib)),
        ("CPU_SLOTS", u64::from(profile.cpu_slots)),
        ("PIDS_LIMIT", u64::from(profile.pids_limit)),
        (
            "MEMORY_SWAP_MIB",
            u64::from(profile.memory_mib) + u64::from(profile.swap_mib),
        ),
    ] {
        assert!(script.contains(&format!(
            "export FOUNDATION_PLATFORM_NATIVE_{name}='{value}'"
        )));
    }
    assert!(script.contains("-f compose.lakehouse.yml -f compose.lakehouse-native.yml --profile lakehouse-control run --rm"));
    let limits = include_str!("../../../../compose.lakehouse-native.yml");
    for setting in ["mem_limit:", "memswap_limit:", "cpus:", "pids_limit:"] {
        assert!(limits.contains(setting));
    }
    assert!(
        !limits.contains(":-"),
        "native limits must not have fallback defaults"
    );
    Ok(())
}

#[test]
fn floor_full_requires_exact_inputs_retained_time_and_history() -> anyhow::Result<()> {
    let history_path = history_fixture()?.path().display().to_string();
    for missing in [
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_FLOOR_SOURCE_OBJECT",
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT",
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC",
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH",
    ] {
        let result = floor_source::FloorSource::from_lookup(&mut |name| {
            if name == missing {
                return None;
            }
            match name {
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_FLOOR_SOURCE_OBJECT" => {
                    Some("OPN20991231FLOOR.zip".into())
                }
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT" => {
                    Some("OPN20991231TITLE.zip".into())
                }
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC" => {
                    Some("2099-12-31T12:00:00Z".into())
                }
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH" => {
                    Some(history_path.clone())
                }
                _ => None,
            }
        });
        assert!(result.is_err(), "accepted missing {missing}");
    }
    Ok(())
}

#[test]
fn floor_derivation_uses_separate_artifacts_and_is_shell_quoted() -> anyhow::Result<()> {
    let mut config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    let original = config.floor_source.as_ref().context("floor")?.root();
    config.floor_source.as_mut().context("floor")?.ingested_at += chrono::Duration::days(1);
    assert_eq!(
        config.floor_source.as_ref().context("floor")?.root(),
        original
    );
    config.floor_source.as_mut().context("floor")?.derivation = Some("floor 'fix' $(false)".into());
    assert_ne!(
        config.floor_source.as_ref().context("floor")?.root(),
        original
    );
    let plan = build_remote_lakehouse_job_plan(&config);
    assert!(plan.remote_script.contains(&format!(
        "--derivation {}",
        shell_quote("floor 'fix' $(false)")
    )));
    assert!(!plan
        .remote_script
        .contains("--iceberg-write-mode overwrite"));
    Ok(())
}

#[test]
fn floor_history_precedes_export_and_propagates_errors() -> anyhow::Result<()> {
    let config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    let script = build_remote_lakehouse_job_plan(&config).remote_script;
    let stage = script
        .find("lakehouse-control stage-building-register-floor-inputs")
        .context("exact input staging")?;
    let inspection = script
        .find("export FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH")
        .context("inspection")?;
    let history = script
        .find("building_register_floor_history.py")
        .context("history")?;
    let exporter = script
        .rfind("lakehouse-control export-building-register-floor-silver-handoff")
        .context("export")?;
    let scalar = script
        .find("silver_scalar_handoff_to_lakehouse.py")
        .context("scalar")?;
    assert!(stage < inspection && inspection < history && history < exporter && exporter < scalar);
    assert!(!script.contains("missing building-register floor Bronze source"));
    assert!(!script.contains("s3 sync"));
    assert!(script.contains("floor_preflight_status=$?"));
    assert!(script.contains("\"$floor_preflight_status\" -ne 10"));
    assert!(script.contains("exit \"$floor_preflight_status\""));
    assert!(script.contains("mktemp -d"));
    assert!(script
        .contains("--source-receipt \"/workspace/$floor_preflight_container_dir/source.json\""));
    assert!(script.contains("chmod 640 \"$floor_preflight_dir/source.json\""));
    Ok(())
}

#[test]
fn local_floor_script_separates_host_state_and_container_paths() -> anyhow::Result<()> {
    let mut config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    let relative = config.floor_source.as_ref().context("floor")?.root();
    let suffix = relative
        .strip_prefix("target/lakehouse/")
        .context("state suffix")?;
    config.local = Some(floor_cycle::LocalExecution {
        project: format!("foundation-floor-{}", "1".repeat(32)),
        state_root: "/var/lib/floor state".into(),
        ivy_cache: "/var/lib/floor state/ivy".into(),
        image: format!("sha256:{}", "a".repeat(64)),
        database_network: "runtime_default".into(),
        database_endpoint: "postgres:5432".into(),
        outcome: "/var/lib/floor state/attempt/outcome.json".into(),
    });
    let script = build_remote_lakehouse_job_plan(&config).remote_script;
    assert!(script.contains(&format!(
        "mktemp -d '/var/lib/floor state/{suffix}'/source-check.XXXXXXXX"
    )));
    assert!(script.contains(&format!(
        "floor_preflight_container_dir='{relative}'/\"${{floor_preflight_dir##*/}}\""
    )));
    assert!(script.contains(&format!("--input '/workspace/{relative}/handoff'")));
    assert!(script.contains(&format!(
        "--summary-output '/workspace/{relative}/spark-summary.json'"
    )));
    assert!(script.contains(&format!(
        "cat '/var/lib/floor state/{suffix}/spark-summary.json'"
    )));
    assert!(!script.contains("/workspace//var/lib/"));
    assert!(!script.contains(". '.env.lakehouse'"));
    assert!(!script.contains("build lakehouse-control"));
    assert!(script.contains(
        "-f '/var/lib/floor state/attempt/native-compose.json' --profile lakehouse-control run"
    ));
    assert_eq!(script.matches("-f '/var/lib/floor state/attempt/native-compose.json' --profile lakehouse-batch run --rm --no-deps --pull never").count(), 2);
    assert!(!script.contains("-e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH"));
    assert!(!script.contains("mkdir -p 'target/lakehouse/smoke'"));
    assert!(script.contains("run --rm --no-deps --pull never"));
    assert!(script.contains("--conf spark.jars.ivy=/home/spark/.ivy2"));
    assert!(script.contains("--user \"${FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}:$(id -g)\""));
    let destination = shell_quote("/var/lib/floor state/attempt/outcome.json");
    let history_capture = script
        .find(&format!(
            "cat \"$floor_preflight_dir/history.json\" > {destination}"
        ))
        .context("retained outcome capture")?;
    let early_exit = script[history_capture..]
        .find("exit 0")
        .context("retained early exit")?
        + history_capture;
    let scalar = script
        .find("silver_scalar_handoff_to_lakehouse.py")
        .context("scalar")?;
    let scalar_capture = script
        .find(&format!(
            "cat '/var/lib/floor state/{suffix}/spark-summary.json' > {destination}"
        ))
        .context("scalar outcome capture")?;
    let final_marker = script
        .rfind(SUMMARY_BEGIN_MARKER)
        .context("summary marker")?;
    assert!(history_capture < early_exit && early_exit < scalar);
    assert!(scalar < scalar_capture && scalar_capture < final_marker);
    assert_eq!(script.matches("(set -C; cat ").count(), 2);
    Ok(())
}

#[test]
fn historical_retention_summary_cannot_claim_a_new_write() -> anyhow::Result<()> {
    let witness = history_fixture()?.value()?;
    let source = floor_source::FloorSource {
        history: history_fixture()?,
        floor: format!(
            "{}.zip",
            witness["inputs"]["floor"]["provider_file_id"]
                .as_str()
                .context("floor ID")?
        ),
        title: format!(
            "{}.zip",
            witness["inputs"]["title"]["provider_file_id"]
                .as_str()
                .context("title ID")?
        ),
        ingested_at: chrono::DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")?
            .with_timezone(&chrono::Utc),
        derivation: None,
    };
    let value = serde_json::json!({
        "schema_version": "foundation-platform.floor-history-check.v1",
        "action": "already_retained", "persisted_row_count": 0,
        "inputs": witness["inputs"], "derivation": null,
        "source_snapshot_id": witness["source_snapshot_id"],
        "table_uuid": witness["table_uuid"], "snapshot_id": witness["snapshot_id"],
        "observed_head_snapshot_id": witness["snapshot_id"],
        "retained_row_count": witness["row_count"],
    });
    assert_eq!(
        floor_history::retained_outcome(&value.to_string(), &source)?,
        witness["row_count"].as_u64()
    );
    for key in [
        "action",
        "persisted_row_count",
        "source_snapshot_id",
        "table_uuid",
        "snapshot_id",
        "retained_row_count",
        "observed_head_snapshot_id",
    ] {
        let mut changed = value.clone();
        changed[key] = serde_json::json!("wrong");
        assert!(
            floor_history::retained_outcome(&changed.to_string(), &source).is_err(),
            "accepted {key}"
        );
    }
    assert_eq!(
        floor_history::retained_outcome("{\"schema_version\":\"scalar\"}", &source)?,
        None
    );
    let mut derived_source = source.clone();
    derived_source.derivation = Some("normalizer-v2".into());
    let mut mislabeled = value.clone();
    mislabeled["derivation"] = serde_json::json!("normalizer-v2");
    assert!(floor_history::retained_outcome(&mislabeled.to_string(), &derived_source).is_err());
    let mut unknown_kind = value.clone();
    unknown_kind["retention_kind"] = serde_json::json!("unknown");
    assert!(floor_history::retained_outcome(&unknown_kind.to_string(), &source).is_err());
    Ok(())
}

#[test]
fn registered_retention_requires_its_commit_and_retained_time() -> anyhow::Result<()> {
    let witness = history_fixture()?.value()?;
    let config = synthetic_snapshot_config("building_register_floors_pipeline_full")?;
    let source = config.floor_source.as_ref().context("floor source")?;
    let mut inputs = witness["inputs"].clone();
    for (role, name) in [("floor", &source.floor), ("title", &source.title)] {
        inputs[role]["provider_file_id"] =
            serde_json::json!(name.strip_suffix(".zip").context("source name")?);
        inputs[role]["provider_month"] = serde_json::json!("2099-12-01");
    }
    let identity = foundation_outbox_publisher::building_register_floor_silver_export::source_identity_from_semantic(inputs.clone(), &source.history)?;
    let value = serde_json::json!({
        "schema_version": "foundation-platform.floor-history-check.v1",
        "retention_kind": "registered_append",
        "action": "already_retained", "persisted_row_count": 0,
        "source_snapshot_id": identity, "inputs": inputs,
        "table_uuid": witness["table_uuid"], "snapshot_id": "123",
        "observed_head_snapshot_id": "124", "retained_row_count": 3,
        "retained_ingested_at_utc": "2026-10-21T10:00:00Z", "derivation": null,
    });
    assert_eq!(
        floor_history::retained_outcome(&value.to_string(), source)?,
        Some(3)
    );
    for (key, invalid) in [
        ("retained_row_count", serde_json::json!(0)),
        ("persisted_row_count", serde_json::json!(3)),
        ("retained_ingested_at_utc", serde_json::json!("2026-10-21")),
        (
            "source_snapshot_id",
            serde_json::json!("building-register-floor-content-v1-other"),
        ),
        ("table_uuid", serde_json::json!("other-table")),
        ("derivation", serde_json::json!("other-derivation")),
        ("snapshot_id", serde_json::json!("0")),
        ("observed_head_snapshot_id", serde_json::json!(-1)),
    ] {
        let mut changed = value.clone();
        changed[key] = invalid;
        assert!(
            floor_history::retained_outcome(&changed.to_string(), source).is_err(),
            "accepted {key}"
        );
    }
    let mut changed = value.clone();
    changed["inputs"]["floor"]["provider_file_id"] = serde_json::json!("OPN20991231OTHER");
    // Even an internally consistent receipt for another source cannot satisfy this request.
    changed["source_snapshot_id"] = serde_json::json!(
        foundation_outbox_publisher::building_register_floor_silver_export::source_identity_from_semantic(changed["inputs"].clone(), &source.history)?
    );
    assert!(floor_history::retained_outcome(&changed.to_string(), source).is_err());
    Ok(())
}
