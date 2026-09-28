use super::*;
use std::collections::BTreeMap;

fn valid() -> BTreeMap<&'static str, String> {
    BTreeMap::from([
        (CONFIRM, "1"),
        (UNIT, "parcels"),
        (INPUT, "00000000-0000-7000-8000-000000000001"),
        (PUBLIC, "https://tiles.example.com/v1/"),
        (VERIFY, "http://127.0.0.1:3111"),
        (SOURCE, "http://127.0.0.1:3111"),
        (TILE, "0/0/0"),
        (OPERATOR, "00000000-0000-7000-8000-000000000002"),
        (KEY, "build"),
        (PROMOTE_KEY, "promote"),
        (TIMEOUT, "60"),
    ])
    .into_iter()
    .map(|(key, value)| (key, value.to_owned()))
    .collect()
}

#[test]
fn requires_every_auditable_input_including_source_and_representative_tile() {
    let values = valid();
    let config = Config::from_lookup(|name| values.get(name).cloned()).unwrap();
    assert_eq!(config.public_base, "https://tiles.example.com/v1");
    assert_eq!(config.control_timeout, Duration::from_secs(60));
    for key in values.keys() {
        let mut missing = values.clone();
        missing.remove(key);
        assert!(
            Config::from_lookup(|name| missing.get(name).cloned()).is_err(),
            "missing {key}"
        );
    }
}

#[test]
fn refuses_invalid_confirmation_public_url_tile_and_control_deadline() {
    for (key, value) in [
        (CONFIRM, "true"),
        (PUBLIC, "http://127.0.0.1:3111"),
        (TILE, "0/1/0"),
        (UNIT, "../parcels"),
        (VERIFY, "https://tiles.example.com?token=x"),
        (TIMEOUT, "0"),
        (TIMEOUT, "601"),
        (TIMEOUT, "43200"),
    ] {
        let mut values = valid();
        values.insert(key, value.to_owned());
        assert!(
            Config::from_lookup(|name| values.get(name).cloned()).is_err(),
            "{key}={value}"
        );
    }
}

#[test]
fn validated_and_promoted_retries_skip_copy_proof_and_recording() {
    assert!(should_record("running").unwrap());
    assert!(!should_record("validated").unwrap());
    assert!(!should_record("promoted").unwrap());
    for status in ["failed", "superseded", "cancelled", "queued", ""] {
        assert!(should_record(status).is_err(), "{status}");
    }
}

#[tokio::test]
async fn control_timeout_stops_stalled_control_operations() {
    assert!(control(
        Duration::from_millis(1),
        std::future::pending::<anyhow::Result<()>>()
    )
    .await
    .is_err());
}

#[tokio::test]
#[ignore = "requires an explicitly supplied disposable local PostgreSQL server"]
async fn build_lease_serializes_retries_and_detects_a_lost_session(
) -> foundation_disposable_database::TestResult {
    foundation_disposable_database::run_in_disposable_database(
        "readdress_lease",
        |pool| async move {
            let build = VectorTileBuildJobId::new(Uuid::from_u128(30));
            let mut first = acquire_build_lease(&pool, build).await?;
            assert!(
                control(Duration::from_millis(50), acquire_build_lease(&pool, build))
                    .await
                    .is_err()
            );
            let different = VectorTileBuildJobId::new(Uuid::from_u128(31));
            let mut independent = control(
                Duration::from_secs(2),
                acquire_build_lease(&pool, different),
            )
            .await?;
            release_build_lease(&mut independent, different).await?;
            release_build_lease(&mut first, build).await?;
            let mut retry =
                control(Duration::from_secs(2), acquire_build_lease(&pool, build)).await?;
            verify_build_lease(&mut retry).await?;
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *retry)
                .await?;
            let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&pool)
                .await?;
            assert!(terminated);
            assert!(verify_build_lease(&mut retry).await.is_err());
            drop(retry);
            drop(first);
            drop(independent);
            Ok(())
        },
    )
    .await
}

#[test]
fn evidence_is_repeatable_and_binds_the_source_route_and_decoded_tile() {
    let values = valid();
    let mut config = Config::from_lookup(|name| values.get(name).cloned()).unwrap();
    let build = VectorTileBuildJobId::new(Uuid::from_u128(3));
    let release = static_release_id_for_build(build);
    let input = Input {
        status: "running".to_owned(),
        input_release_id: config.input_release_id,
        generation: ServingGeneration::new(1).unwrap(),
        snapshot: "1".to_owned(),
        revision: Uuid::from_u128(4),
        source_key: static_release_pmtiles_object_key("parcels", config.input_release_id),
        expected: StreamingObjectRehash {
            checksum_sha256: "a".repeat(64),
            size_bytes: 10,
            observed_e_tag: None,
            observed_last_modified: None,
        },
    };
    let destination = static_release_pmtiles_object_key("parcels", release);
    let original = evidence_digest(&config, &input, build, release, &destination, b"tile").unwrap();
    assert_eq!(
        original,
        evidence_digest(&config, &input, build, release, &destination, b"tile").unwrap()
    );
    assert_ne!(
        original,
        evidence_digest(&config, &input, build, release, &destination, b"other").unwrap()
    );
    config.source_base = "https://source.example.com".to_owned();
    assert_ne!(
        original,
        evidence_digest(&config, &input, build, release, &destination, b"tile").unwrap()
    );
}
