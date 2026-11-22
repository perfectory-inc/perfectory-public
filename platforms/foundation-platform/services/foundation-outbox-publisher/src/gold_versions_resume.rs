//! A `_versions` build that goes on across runs (root ADR-0564): every lane's one way to finish a
//! build longer than its unit's hour.
//!
//! A build splits its work into groups it numbers the same way every run (a lane's regions). The
//! state of one run's inputs, code and edition (`gold_versions_commit::run_id`) is a directory under
//! the lane's resume root (`gold-versions-build.sh` passes it): what the build spooled, and each
//! finished group's rows. Past [`BUILD_DEADLINE`] no group starts and the run ends `partial`; the next
//! run of the same fingerprint skips the finished groups, and the one that finishes the last diffs and
//! commits once and clears the state. A run of another fingerprint clears what an older one left.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use foundation_publisher_silver::duckdb::Connection;
use serde::{de::DeserializeOwned, Serialize};

use crate::public_data_control_support::required_env_value;

/// The lane's resume root.
const RESUME_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_RESUME_DIR";
/// No group starts past this: the unit's hour (`foundation-gold-versions@.service` `TimeoutStartSec`)
/// less the takeover verdict a commit's run goes on to (gold-versions-build.sh TAKEOVER_SECONDS, 30
/// minutes) and the longest group a run may still start (W84b, 11-22: 45 + 30 is past the hour).
pub(crate) const BUILD_DEADLINE: Duration = Duration::from_secs(25 * 60);

/// Whether a run that started at `started` may start another group.
pub(crate) fn may_start(started: Instant) -> bool {
    started.elapsed() <= BUILD_DEADLINE
}

/// One fingerprint's state.
pub(crate) struct Resume {
    root: PathBuf,
    dir: PathBuf,
    /// The groups' directory as `DuckDB` SQL text.
    groups: String,
}

impl Resume {
    /// The state of `run` under the lane's resume root; any other run's state there is removed.
    ///
    /// # Errors
    /// The root is not set, not UTF-8 or holds a quote, or a directory cannot be made or removed.
    pub(crate) fn open(run: &str) -> anyhow::Result<Self> {
        Self::open_in(Path::new(&required_env_value(RESUME_ENV)?), run)
    }

    fn open_in(root: &Path, run: &str) -> anyhow::Result<Self> {
        let name: String = run
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        ensure!(!name.is_empty(), "a run has an id");
        std::fs::create_dir_all(root)?;
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy() != name.as_str() && entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(entry.path())
                    .with_context(|| format!("cannot remove {}", entry.path().display()))?;
            }
        }
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("groups"))?;
        let groups = dir
            .join("groups")
            .to_str()
            .context("the resume root must be UTF-8")?
            .replace('\\', "/");
        ensure!(
            !groups.contains('\''),
            "the resume root must not contain a quote"
        );
        Ok(Self {
            root: root.to_path_buf(),
            dir,
            groups,
        })
    }

    /// The directory a build may keep its own files in (its spool).
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// What an earlier run of this fingerprint stored under `name`, if it did.
    ///
    /// # Errors
    /// A record that cannot be read or parsed.
    pub(crate) fn load<T: DeserializeOwned>(&self, name: &str) -> anyhow::Result<Option<T>> {
        let path = self.dir.join(format!("{name}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let bytes =
            std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?;
        Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
            format!("cannot parse {}", path.display())
        })?))
    }

    /// Stores `value` under `name`, whole or not at all (written aside, then renamed).
    ///
    /// # Errors
    /// The record cannot be written.
    pub(crate) fn store<T: Serialize>(&self, name: &str, value: &T) -> anyhow::Result<()> {
        let path = self.dir.join(format!("{name}.json"));
        let part = self.dir.join(format!("{name}.json.part"));
        std::fs::write(&part, serde_json::to_vec(value)?)?;
        std::fs::rename(&part, &path)?;
        Ok(())
    }

    /// What the lane stored under `name` beside every fingerprint (kept by [`Self::open`] and
    /// [`Self::clear`]: what the last commit was built from), if it did.
    ///
    /// # Errors
    /// A record that cannot be read or parsed.
    pub(crate) fn load_lane<T: DeserializeOwned>(&self, name: &str) -> anyhow::Result<Option<T>> {
        let path = self.root.join(format!("{name}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let bytes =
            std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// Stores `value` under `name` beside every fingerprint, whole or not at all.
    ///
    /// # Errors
    /// The record cannot be written.
    pub(crate) fn store_lane<T: Serialize>(&self, name: &str, value: &T) -> anyhow::Result<()> {
        let part = self.root.join(format!("{name}.json.part"));
        std::fs::write(&part, serde_json::to_vec(value)?)?;
        std::fs::rename(&part, self.root.join(format!("{name}.json")))?;
        Ok(())
    }

    fn group_file(&self, index: usize) -> PathBuf {
        self.dir.join("groups").join(format!("{index:05}.parquet"))
    }

    /// Whether an earlier run finished group `index`.
    pub(crate) fn finished(&self, index: usize) -> bool {
        self.group_file(index).exists()
    }

    /// Moves `table`'s rows into group `index`'s file, whole or not at all, and empties `table`.
    ///
    /// # Errors
    /// `table` is not a plain name, or `DuckDB` or the file system refuses.
    pub(crate) fn finish(
        &self,
        connection: &Connection,
        table: &str,
        index: usize,
    ) -> anyhow::Result<()> {
        ensure!(
            table
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "{table} is not a plain table name"
        );
        connection.execute_batch(&format!(
            "COPY {table} TO '{}/{index:05}.parquet.part' (FORMAT parquet, COMPRESSION zstd); DELETE FROM {table};",
            self.groups
        ))?;
        std::fs::rename(
            self.dir
                .join("groups")
                .join(format!("{index:05}.parquet.part")),
            self.group_file(index),
        )?;
        Ok(())
    }

    /// Appends every finished group's rows to `table`; returns whether there were any.
    ///
    /// # Errors
    /// `table` is not a plain name, or `DuckDB` refuses.
    pub(crate) fn load_finished(
        &self,
        connection: &Connection,
        table: &str,
    ) -> anyhow::Result<bool> {
        ensure!(
            table
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "{table} is not a plain table name"
        );
        let any = std::fs::read_dir(self.dir.join("groups"))?
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".parquet"));
        if any {
            connection.execute_batch(&format!(
                "INSERT INTO {table} SELECT * FROM read_parquet('{}/*.parquet');",
                self.groups
            ))?;
        }
        Ok(any)
    }

    /// Removes the state once the commit it led to stands.
    ///
    /// # Errors
    /// The directory cannot be removed.
    pub(crate) fn clear(self) -> anyhow::Result<()> {
        std::fs::remove_dir_all(&self.dir)
            .with_context(|| format!("cannot remove {}", self.dir.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stopped_build_goes_on_from_its_finished_groups_and_a_new_fingerprint_starts_over(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let connection = Connection::open_in_memory()?;
        connection.execute_batch("CREATE TABLE built (k VARCHAR, v BIGINT);")?;

        let first = Resume::open_in(root.path(), "run:a")?;
        first.store("plan", &vec![3_u32, 4])?;
        connection.execute_batch("INSERT INTO built VALUES ('a', 1), ('b', 2);")?;
        first.finish(&connection, "built", 0)?;
        let left: i64 = connection.query_row("SELECT count(*) FROM built", [], |r| r.get(0))?;
        assert_eq!(left, 0, "a finished group leaves the table empty");

        // The next run of the same fingerprint sees what the first stored and finished.
        let again = Resume::open_in(root.path(), "run:a")?;
        assert_eq!(again.load::<Vec<u32>>("plan")?, Some(vec![3, 4]));
        assert!(again.finished(0) && !again.finished(1));
        connection.execute_batch("INSERT INTO built VALUES ('c', 3);")?;
        again.finish(&connection, "built", 1)?;
        assert!(again.load_finished(&connection, "built")?);
        let rows: i64 = connection.query_row("SELECT count(*) FROM built", [], |r| r.get(0))?;
        assert_eq!(rows, 3, "every finished group comes back once");

        // Another fingerprint (the inputs moved) clears the old state.
        let other = Resume::open_in(root.path(), "run:b")?;
        assert!(!other.finished(0));
        assert_eq!(other.load::<Vec<u32>>("plan")?, None);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
        other.clear()?;
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }
}
