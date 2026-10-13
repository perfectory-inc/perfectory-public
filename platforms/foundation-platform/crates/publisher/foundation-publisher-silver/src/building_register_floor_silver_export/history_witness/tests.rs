use super::*;
use serde_json::json;

#[test]
fn parent_digest_binds_child_and_loaded_evidence_does_not_reread_configuration(
) -> anyhow::Result<()> {
    let original = fixture()?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().canonicalize()?.join("witness.json");
    let bytes = std::fs::read(original.path())?;
    std::fs::write(&path, &bytes)?;
    let loaded = HistoryWitness::load(&path, None)?;
    assert_eq!(loaded.sha256(), format!("{:x}", Sha256::digest(&bytes)));
    let child = HistoryWitness::from_lookup(
        &mut |key| match key {
            HISTORY_PATH_ENV => Some(path.display().to_string()),
            HISTORY_SHA256_ENV => Some(loaded.sha256().to_owned()),
            _ => None,
        },
        true,
    )?;
    assert_eq!(child, loaded);
    let mut changed = bytes;
    changed.push(b'\n'); // Same semantic JSON is still different pinned bytes.
    std::fs::write(&path, changed)?;
    assert_eq!(loaded.value()?, original.value()?);
    assert!(HistoryWitness::load(&path, Some(loaded.sha256())).is_err());
    assert_ne!(HistoryWitness::load(&path, None)?.sha256(), loaded.sha256());
    Ok(())
}

#[test]
fn absent_configuration_never_falls_through_to_new_source() -> anyhow::Result<()> {
    assert!(HistoryWitness::from_lookup(&mut |_| None, false).is_err());
    let original = fixture()?;
    assert!(HistoryWitness::from_lookup(
        &mut |key| { (key == HISTORY_PATH_ENV).then(|| original.path().display().to_string()) },
        true
    )
    .is_err());
    for pin in ["", "bad", &"A".repeat(64), &"a".repeat(63), &"b".repeat(64)] {
        assert!(HistoryWitness::load(original.path(), Some(pin)).is_err());
    }
    Ok(())
}

#[test]
fn witness_file_is_absolute_regular_bounded_and_typed() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?;
    let path = root.join("witness.json");
    assert!(HistoryWitness::load(Path::new("relative.json"), None).is_err());
    assert!(HistoryWitness::load(&path, None).is_err());
    assert!(HistoryWitness::load(&root, None).is_err());
    for bytes in [Vec::new(), vec![b' '; 65537], b"{}".to_vec()] {
        std::fs::write(&path, bytes)?;
        assert!(HistoryWitness::load(&path, None).is_err());
    }
    let original = fixture()?;
    let raw = std::fs::read_to_string(original.path())?;
    let duplicate = raw.replacen('"', "\"schema_version\":1,\"", 1);
    std::fs::write(&path, duplicate)?;
    assert!(HistoryWitness::load(&path, None).is_err());
    Ok(())
}

#[test]
fn invalid_external_evidence_is_rejected_before_any_selection() -> anyhow::Result<()> {
    let valid = fixture()?.value()?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().canonicalize()?.join("witness.json");
    for (pointer, invalid) in [
        ("/schema_version", json!(true)),
        ("/schema_version", json!(2)),
        ("/table_name", json!("silver.other")),
        ("/table_uuid", json!(uuid::Uuid::nil())),
        ("/snapshot_id", json!("0")),
        ("/snapshot_id", json!("+10")),
        ("/snapshot_id", json!("010")),
        (
            "/snapshot_id",
            json!((i64::MAX.unsigned_abs() + 1).to_string()),
        ),
        ("/operation", json!("append")),
        (
            "/source_snapshot_id",
            json!("building-register-floor-selection-v2-short"),
        ),
        ("/row_count", json!(0)),
        ("/row_count", json!(true)),
        ("/valid_from_utc", json!("2099-09-20")),
        ("/ingested_at_utc", json!(false)),
        ("/valid_from_utc", json!("2099-09-20T00:00:00.000000001Z")),
        ("/ingested_at_utc", json!("2099-09-29T17:53:49.231000001Z")),
        (
            "/inputs/floor/role_slug",
            json!("hubgokr__building_register_main"),
        ),
        ("/inputs/title/provider_file_id", json!("OPN../unsafe")),
        ("/inputs/title/provider_month", json!("2099-10-01")),
        ("/inputs/floor/provider_month", json!("2099-09-02")),
        ("/inputs/floor/size_bytes", json!(0)),
        ("/inputs/title/size_bytes", json!(false)),
        ("/inputs/title/checksum_sha256", json!("B".repeat(64))),
        (
            "/bronze_object_key",
            json!("bronze/source=wrong/--sha256-fake.zip"),
        ),
    ] {
        let mut value = valid.clone();
        *value.pointer_mut(pointer).context("fixture field")? = invalid;
        std::fs::write(&path, serde_json::to_vec(&value)?)?;
        assert!(
            HistoryWitness::load(&path, None).is_err(),
            "accepted {pointer}"
        );
    }
    for object in ["", "/inputs", "/inputs/floor", "/inputs/title"] {
        let mut value = valid.clone();
        let map = value
            .pointer_mut(object)
            .and_then(serde_json::Value::as_object_mut)
            .context("fixture object")?;
        map.insert("unknown".into(), json!(null));
        std::fs::write(&path, serde_json::to_vec(&value)?)?;
        assert!(HistoryWitness::load(&path, None).is_err());
    }
    Ok(())
}

#[test]
fn typed_witness_canonicalizes_offset_times_and_uuid_without_changing_raw_pin() -> anyhow::Result<()>
{
    let mut value = fixture()?.value()?;
    value["table_uuid"] = json!("AAAAAAAA-1111-4111-8111-BBBBBBBBBBBB");
    value["valid_from_utc"] = json!("2099-09-20T09:00:00+09:00");
    value["ingested_at_utc"] = json!("2099-09-30T02:53:49.231000+09:00");
    let bytes = serde_json::to_vec(&value)?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().canonicalize()?.join("witness.json");
    std::fs::write(&path, &bytes)?;
    let loaded = HistoryWitness::load(&path, None)?;
    let canonical = loaded.value()?;
    assert_eq!(
        canonical["table_uuid"],
        "aaaaaaaa-1111-4111-8111-bbbbbbbbbbbb"
    );
    assert_eq!(canonical["valid_from_utc"], "2099-09-20T00:00:00Z");
    assert_eq!(canonical["ingested_at_utc"], "2099-09-29T17:53:49.231Z");
    assert_eq!(loaded.sha256(), format!("{:x}", Sha256::digest(&bytes)));
    Ok(())
}

#[cfg(unix)]
#[test]
fn witness_rejects_file_and_ancestor_symlinks() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?;
    let original = fixture()?;
    let file_link = root.join("file.json");
    std::os::unix::fs::symlink(original.path(), &file_link)?;
    assert!(open_regular(&file_link).is_err());
    assert!(HistoryWitness::load(&file_link, None).is_err());
    let directory_link = root.join("directory");
    std::os::unix::fs::symlink(original.path().parent().context("parent")?, &directory_link)?;
    assert!(HistoryWitness::load(
        &directory_link.join(original.path().file_name().context("name")?),
        None
    )
    .is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn open_boundary_refuses_fifo_without_waiting_for_a_writer() -> anyhow::Result<()> {
    use rustix::fs::{mkfifoat, Mode, CWD};
    let dir = tempfile::tempdir()?;
    let fifo = dir.path().canonicalize()?.join("witness.fifo");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR)?;
    // Exercise open itself, simulating a regular-file -> FIFO replacement after lstat.
    // O_NONBLOCK must prevent waiting for an absent pipe writer before type rejection.
    assert!(open_regular(&fifo).is_err());
    assert!(HistoryWitness::load(&fifo, None).is_err());
    Ok(())
}
