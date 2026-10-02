use super::*;

#[test]
fn pinned_image_refuses_tags_and_malformed_digests() {
    for image in [
        format!("sha256:{}", "a".repeat(64)),
        format!("registry/producer@sha256:{}", "1".repeat(64)),
    ] {
        assert!(validate_image(&image).is_ok());
    }
    for image in [
        "latest",
        "producer:v1",
        "sha256:abc",
        "@sha256:abcd",
        "sha256:",
        "repo@sha256:gggg",
        "repo${TAG}@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        assert!(validate_image(image).is_err(), "accepted {image}");
    }
}

#[test]
fn work_directories_must_be_real_absolute_and_outside_release() -> anyhow::Result<()> {
    let base = tempfile::tempdir()?;
    let release = base.path().join("release");
    let state = base.path().join("state with space");
    let ivy = state.join("ivy");
    std::fs::create_dir(&release)?;
    std::fs::create_dir_all(&ivy)?;
    let mut values = std::collections::BTreeMap::from([
        ("INVOCATION_ID", "1".repeat(32)),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK",
            "runtime_default".into(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_ENDPOINT",
            "postgres:5432".into(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT",
            state.display().to_string(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE",
            ivy.display().to_string(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE",
            format!("sha256:{}", "a".repeat(64)),
        ),
    ]);
    assert!(LocalExecution::from_lookup(&mut |key| values.get(key).cloned(), &release).is_ok());
    for invalid in [
        "relative".to_owned(),
        release.display().to_string(),
        base.path().join("absent").display().to_string(),
    ] {
        values.insert("FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT", invalid);
        assert!(
            LocalExecution::from_lookup(&mut |key| values.get(key).cloned(), &release).is_err()
        );
    }
    values.insert(
        "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT",
        ivy.display().to_string(),
    );
    assert!(LocalExecution::from_lookup(&mut |key| values.get(key).cloned(), &release).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlink_into_release_cannot_hide_a_mutable_work_directory() -> anyhow::Result<()> {
    let base = tempfile::tempdir()?;
    let release = base.path().join("release");
    let link = base.path().join("state");
    let ivy = base.path().join("ivy");
    std::fs::create_dir(&release)?;
    std::fs::create_dir(&ivy)?;
    std::os::unix::fs::symlink(&release, &link)?;
    let values = std::collections::BTreeMap::from([
        ("INVOCATION_ID", "1".repeat(32)),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK",
            "runtime_default".into(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_ENDPOINT",
            "postgres:5432".into(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT",
            link.display().to_string(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE",
            ivy.display().to_string(),
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE",
            format!("sha256:{}", "a".repeat(64)),
        ),
    ]);
    assert!(LocalExecution::from_lookup(&mut |key| values.get(key).cloned(), &release).is_err());
    Ok(())
}

#[test]
fn only_historical_base_reuses_witness_time_and_clock_precision_is_persistable(
) -> anyhow::Result<()> {
    let retained = DateTime::parse_from_rfc3339("2026-09-29T17:53:49.231Z")?.with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339("2026-10-01T12:00:00.123456789Z")?.with_timezone(&Utc);
    assert_eq!(execution_time(Some(retained), false, now)?, retained);
    let rounded = DateTime::parse_from_rfc3339("2026-10-01T12:00:00.123456Z")?.with_timezone(&Utc);
    assert_eq!(execution_time(Some(retained), true, now)?, rounded);
    assert_eq!(execution_time(None, false, now)?, rounded);
    Ok(())
}

#[test]
fn outcome_must_be_bounded_utf8_regular_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("outcome.json");
    assert!(read_outcome(&path).is_err());
    assert!(read_outcome(dir.path()).is_err());
    std::fs::write(&path, "{}")?;
    assert_eq!(read_outcome(&path)?, "{}");
    std::fs::write(&path, [255])?;
    assert!(read_outcome(&path).is_err());
    std::fs::write(&path, vec![b' '; MAX_OUTCOME_BYTES as usize + 1])?;
    assert!(read_outcome(&path).is_err());
    #[cfg(unix)]
    {
        let link = dir.path().join("linked.json");
        std::os::unix::fs::symlink(&path, &link)?;
        assert!(read_outcome(&link).is_err());
    }
    Ok(())
}

#[test]
fn outcome_must_match_selected_source_in_both_branches() -> anyhow::Result<()> {
    use serde_json::json;
    let retained = json!({"schema_version": "foundation-platform.floor-history-check.v1", "source_snapshot_id": "chosen"});
    let scalar = json!({"source_snapshot_ids": ["chosen"]});
    validate_selected_outcome(&retained, "chosen")?;
    validate_selected_outcome(&scalar, "chosen")?;
    assert!(validate_selected_outcome(&retained, "other").is_err());
    assert!(validate_selected_outcome(&scalar, "other").is_err());
    assert!(validate_selected_outcome(
        &json!({"source_snapshot_ids": ["chosen", "other"]}),
        "chosen"
    )
    .is_err());
    assert!(validate_selected_outcome(&json!({}), "chosen").is_err());
    Ok(())
}
