use super::*;

#[tokio::test]
async fn sealer_rejects_a_key_whose_sha256_does_not_name_the_loaded_bytes() -> TestResult {
    let server = MockServer::start().await;
    let run_id = Uuid::new_v4();
    let execution_bytes = serde_json::to_vec(&serde_json::json!({
        "schema_version": EXECUTION_SCHEMA_VERSION,
        "status": "succeeded",
        "mirror_rebuild_run_id": run_id,
        "source_record_id": Uuid::new_v4(),
        "source_file_asset_id": Uuid::new_v4(),
        "scope": {"kind": "national", "complete": true},
        "limits": {"object_limit": null, "row_limit": null, "shard_limit": null},
        "iceberg_commit": {
            "committed": true,
            "logical_table": "silver.parcel_boundaries",
            "table_uuid": ICEBERG_TABLE_UUID,
            "snapshot_id": CANONICAL_SNAPSHOT_ID
        },
        "production_cutover_allowed": true,
        "national_rollout_allowed": true
    }))?;
    let mismatched_key = evidence_object_key(b"different evidence bytes");
    assert_ne!(mismatched_key, evidence_object_key(&execution_bytes));
    Mock::given(method("GET"))
        .and(path(r2_request_path(&mismatched_key)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(execution_bytes))
        .expect(1)
        .mount(&server)
        .await;
    let env_file = write_remote_only_sealer_env(&server, run_id)?;

    let rejected = sealer_output(&env_file, &mismatched_key).await?;

    assert!(
        !rejected.status.success(),
        "mismatched content address must fail"
    );
    assert!(
        String::from_utf8_lossy(&rejected.stderr)
            .contains("object key SHA-256 does not match the loaded execution evidence bytes"),
        "content-address rejection: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    let requests = server.received_requests().await.unwrap_or_default();
    assert_eq!(
        requests.len(),
        1,
        "rejection must happen before catalog or DB access"
    );
    assert_eq!(requests[0].method.as_str(), "GET");
    assert_eq!(requests[0].url.path(), r2_request_path(&mismatched_key));
    let _ = fs::remove_file(env_file);
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn producer_output_runs_through_the_real_sealer_and_violations_are_rejected() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_real_sealer").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    pool.close().await;

    let server = MockServer::start().await;
    let env_file = write_sealer_env(&fixture, &server)?;
    let approval_path = write_ready_national_approval_check(&fixture)?;

    assert_writer_requires_production_cutover(&fixture, &server, &env_file, &approval_path).await?;
    let (execution, execution_bytes) =
        write_and_seal_valid_evidence(&fixture, &server, &env_file, &approval_path).await?;
    assert_forged_table_uuid_is_rejected(&server, &env_file, &execution).await?;
    assert_non_success_status_is_rejected(&server, &env_file, &execution).await?;
    assert_second_object_for_run_is_rejected(&server, &env_file, &execution, &execution_bytes)
        .await?;

    let pool = fixture.pool().await?;
    let evidence_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.parcel_publication_source_evidence
          WHERE mirror_rebuild_run_id = $1",
    )
    .bind(fixture.first_run_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        evidence_count, 1,
        "one_per_run_key must preserve one sealed row"
    );
    let _ = fs::remove_file(&env_file);
    let _ = fs::remove_file(&approval_path);
    fixture.finish(pool).await
}

async fn assert_writer_requires_production_cutover(
    fixture: &Fixture,
    server: &MockServer,
    env_file: &PathBuf,
    approval_path: &PathBuf,
) -> TestResult {
    let denied = writer_output(env_file, approval_path, fixture.first_run_id, false).await?;
    assert!(
        !denied.status.success(),
        "missing production approval must fail"
    );
    assert!(
        String::from_utf8_lossy(&denied.stderr)
            .contains("FOUNDATION_PLATFORM_PARCEL_PUBLICATION_PRODUCTION_CUTOVER_CONFIRM=1"),
        "missing approval rejection: {}",
        String::from_utf8_lossy(&denied.stderr)
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty(),
        "approval failure must happen before any R2 or catalog request"
    );
    Ok(())
}

async fn write_and_seal_valid_evidence(
    fixture: &Fixture,
    server: &MockServer,
    env_file: &PathBuf,
    approval_path: &PathBuf,
) -> TestResult<(serde_json::Value, Vec<u8>)> {
    mount_catalog(server, ICEBERG_TABLE_UUID, CANONICAL_SNAPSHOT_ID).await;
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/test-bucket/control/evidence/parcel-publication/execution/sha256%3D[0-9a-f]{64}\.json$",
        ))
        .and(header("if-none-match", "*"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(server)
        .await;

    let written = writer_output(env_file, approval_path, fixture.first_run_id, true).await?;
    let observed_paths = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|request| format!("{} {}", request.method, request.url.path()))
        .collect::<Vec<_>>();
    assert!(
        written.status.success(),
        "producer failed: {}; observed requests: {observed_paths:?}",
        String::from_utf8_lossy(&written.stderr),
    );
    let requests = server.received_requests().await.unwrap_or_default();
    let put = requests
        .iter()
        .find(|request| request.method.as_str() == "PUT")
        .expect("producer must issue one R2 PUT");
    let execution_bytes = put.body.clone();
    let object_key = evidence_object_key(&execution_bytes);
    assert_eq!(
        put.url.path(),
        r2_request_path(&object_key),
        "producer must PUT the content-addressed key derived from its exact bytes"
    );
    let execution: serde_json::Value = serde_json::from_slice(&execution_bytes)?;
    assert_eq!(
        execution["mirror_rebuild_run_id"],
        fixture.first_run_id.to_string()
    );
    assert_eq!(
        execution["iceberg_commit"]["snapshot_id"],
        CANONICAL_SNAPSHOT_ID
    );
    assert_eq!(execution["production_cutover_allowed"], true);
    assert_eq!(execution["national_rollout_allowed"], true);

    Mock::given(method("GET"))
        .and(path(r2_request_path(&object_key)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(execution_bytes.clone()))
        .expect(1)
        .mount(server)
        .await;
    let first = run_sealer(env_file, &object_key).await?;
    assert!(
        first.contains("outcome=created"),
        "first seal output: {first}"
    );
    Ok((execution, execution_bytes))
}

async fn assert_forged_table_uuid_is_rejected(
    server: &MockServer,
    env_file: &PathBuf,
    execution: &serde_json::Value,
) -> TestResult {
    let mut forged_value = execution.clone();
    forged_value["iceberg_commit"]["table_uuid"] = serde_json::json!(Uuid::new_v4().to_string());
    let forged_bytes = serde_json::to_vec(&forged_value)?;
    let forged_key = evidence_object_key(&forged_bytes);
    Mock::given(method("GET"))
        .and(path(r2_request_path(&forged_key)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(forged_bytes))
        .expect(1)
        .mount(server)
        .await;
    let forged = sealer_output(env_file, &forged_key).await?;
    assert!(
        !forged.status.success(),
        "a forged table UUID must be rejected"
    );
    assert!(
        String::from_utf8_lossy(&forged.stderr).contains("not present in the loaded Iceberg"),
        "forged catalog binding rejection: {}",
        String::from_utf8_lossy(&forged.stderr)
    );
    Ok(())
}

async fn assert_non_success_status_is_rejected(
    server: &MockServer,
    env_file: &PathBuf,
    execution: &serde_json::Value,
) -> TestResult {
    let mut failed_value = execution.clone();
    failed_value["status"] = serde_json::json!("failed");
    let failed_bytes = serde_json::to_vec(&failed_value)?;
    let failed_key = evidence_object_key(&failed_bytes);
    Mock::given(method("GET"))
        .and(path(r2_request_path(&failed_key)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(failed_bytes))
        .expect(1)
        .mount(server)
        .await;
    let failed = sealer_output(env_file, &failed_key).await?;
    assert!(
        !failed.status.success(),
        "non-success evidence must not seal"
    );
    assert!(
        String::from_utf8_lossy(&failed.stderr).contains("status must be succeeded"),
        "non-success rejection: {}",
        String::from_utf8_lossy(&failed.stderr)
    );
    Ok(())
}

async fn assert_second_object_for_run_is_rejected(
    server: &MockServer,
    env_file: &PathBuf,
    execution: &serde_json::Value,
    execution_bytes: &[u8],
) -> TestResult {
    let second_bytes = serde_json::to_vec_pretty(&execution)?;
    assert_ne!(
        second_bytes, execution_bytes,
        "violation fixture must change bytes"
    );
    let second_key = evidence_object_key(&second_bytes);
    Mock::given(method("GET"))
        .and(path(r2_request_path(&second_key)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(second_bytes))
        .expect(1)
        .mount(server)
        .await;
    let conflict = sealer_output(env_file, &second_key).await?;
    assert!(
        !conflict.status.success(),
        "one run must not receive a second evidence object"
    );
    assert!(
        String::from_utf8_lossy(&conflict.stderr).contains("exact tuple match"),
        "one-per-run rejection: {}",
        String::from_utf8_lossy(&conflict.stderr)
    );
    Ok(())
}
