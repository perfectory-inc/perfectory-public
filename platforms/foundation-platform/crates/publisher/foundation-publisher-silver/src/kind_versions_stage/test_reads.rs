//! What other stages' tests read through the shared spool: one 시도 read back as a run does.

use super::*;

/// What a test reads back: the served parcels and the kind's rows of its one 시도.
pub struct TestRead {
    pub served_parcels: u64,
    pub rows: u64,
}

/// Spools the connection's local tables to a temporary directory and loads their one 시도, as the
/// build does for each.
pub fn read_for_test(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    edition: &str,
    suffix: &str,
) -> anyhow::Result<TestRead> {
    let dir = test_spool_dir()?;
    // This side's spools of an earlier read go: a spool no row is written to leaves no directory.
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        let of_base = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.ends_with(BASE_SUFFIX));
        if of_base == (suffix == BASE_SUFFIX) {
            std::fs::remove_dir_all(&path)?;
        }
    }
    let spooled = spool(stage, connection, &Source::Local, edition, suffix, &dir)?;
    ensure!(
        spooled.sidos.len() == 1,
        "a test's served parcels lie in one 시도, not {:?}",
        spooled.sidos
    );
    let sido = spooled.sidos.iter().next().context("one 시도")?;
    let rows = load_region(stage, connection, &dir, suffix, sido)?;
    Ok(TestRead {
        served_parcels: spooled.served_parcels,
        rows,
    })
}

thread_local! {
    /// The spool directory of a test's reads ([`read_for_test`]): both sides in one, as a run's.
    static TEST_SPOOL: std::cell::RefCell<Option<tempfile::TempDir>> =
        const { std::cell::RefCell::new(None) };
}

/// The test thread's spool directory, made on first use and removed with the thread.
fn test_spool_dir() -> anyhow::Result<std::path::PathBuf> {
    TEST_SPOOL.with(|kept| {
        let mut kept = kept.borrow_mut();
        if kept.is_none() {
            *kept = Some(tempfile::tempdir()?);
        }
        kept.as_ref()
            .map(|dir| dir.path().to_path_buf())
            .context("the test's spool directory")
    })
}

/// Leaves `targets` from the two sides [`read_for_test`] last read (`_old` and none), comparing
/// every spool as a run whose whole base side was read does; returns the PNUs named.
pub fn changed_for_test(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
) -> anyhow::Result<u64> {
    targets_from_spools(stage, connection, &test_spool_dir()?, &every_spool(stage))
}

/// Reads two states of the connection's local tables as a partial run does, and loads the new one.
///
/// `new` leaves the tables as they are now and `base` as the base commit read them; `moved` names
/// the inputs that differ. Returns what the base read compared and how many PNUs `targets` names.
pub fn partial_for_test(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    edition: &str,
    moved: &[&str],
    base: &dyn Fn() -> anyhow::Result<()>,
    new: &dyn Fn() -> anyhow::Result<()>,
) -> anyhow::Result<(BaseSpool, u64)> {
    let dir = tempfile::tempdir()?;
    new()?;
    let spooled = spool(stage, connection, &Source::Local, edition, "", dir.path())?;
    base()?;
    let moved = moved.iter().map(|input| (*input).to_owned()).collect();
    let read = spool_base(
        stage,
        connection,
        &Source::Local,
        edition,
        dir.path(),
        &moved,
    )?;
    let targets = targets_from_spools(stage, connection, dir.path(), &read.compared)?;
    let sido = spooled.sidos.iter().next().context("one 시도")?;
    load_region(stage, connection, dir.path(), "", sido)?;
    Ok((read, targets))
}
