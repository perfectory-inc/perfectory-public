//! Explicit Appender flush and commit; failed batches cannot be finished as success.
use anyhow::{ensure, Context};
use duckdb::{Appender, AppenderParams, Connection};

use crate::serving_scratch::CURSOR_BYTES;

pub(super) struct Batch<'a> {
    connection: &'a Connection,
    table: &'static str,
    appender: Option<Appender<'a>>,
    bytes: usize,
    failed: bool,
    finished: bool,
}

impl<'a> Batch<'a> {
    pub(super) fn new(connection: &'a Connection, table: &'static str) -> anyhow::Result<Self> {
        connection.execute_batch("BEGIN TRANSACTION")?;
        let mut batch = Self {
            connection,
            table,
            appender: None,
            bytes: 0,
            failed: false,
            finished: false,
        };
        batch.appender = Some(connection.appender(table)?);
        Ok(batch)
    }

    pub(super) fn append(
        &mut self,
        values: impl AppenderParams,
        bytes: usize,
    ) -> anyhow::Result<()> {
        ensure!(
            !self.failed && !self.finished,
            "floor appender already failed or finished"
        );
        let result = self.append_inner(values, bytes);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_inner(&mut self, values: impl AppenderParams, bytes: usize) -> anyhow::Result<()> {
        ensure!(
            (1..=CURSOR_BYTES).contains(&bytes),
            "floor appender row exceeds byte bound"
        );
        if self.bytes > 0
            && self
                .bytes
                .checked_add(bytes)
                .context("floor batch overflow")?
                > CURSOR_BYTES
        {
            self.flush_commit()?;
            self.connection.execute_batch("BEGIN TRANSACTION")?;
            self.appender = Some(self.connection.appender(self.table)?);
            self.bytes = 0;
        }
        self.appender
            .as_mut()
            .context("floor appender closed")?
            .append_row(values)?;
        self.bytes += bytes;
        Ok(())
    }

    fn flush_commit(&mut self) -> anyhow::Result<()> {
        let mut appender = self.appender.take().context("floor appender closed")?;
        appender.flush().context("floor appender flush failed")?;
        drop(appender);
        self.connection
            .execute_batch("COMMIT")
            .context("floor appender commit failed")?;
        Ok(())
    }

    pub(super) fn finish(mut self) -> anyhow::Result<()> {
        ensure!(!self.failed, "floor appender already failed");
        self.flush_commit()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Batch<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Drop may attempt an implicit flush. Rollback follows it, never precedes it.
            drop(self.appender.take());
            let _ = self.connection.execute_batch("ROLLBACK");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_target_commits_before_accepting_the_next_row() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        let observer = connection.try_clone()?;
        connection.execute_batch("CREATE TABLE rows (id BIGINT)")?;
        let mut batch = Batch::new(&connection, "rows")?;
        batch.append([1_i64], CURSOR_BYTES)?;
        batch.appender.as_mut().context("open appender")?.flush()?;
        assert_eq!(
            observer.query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))?,
            0
        );
        batch.append([2_i64], 8)?;
        assert_eq!(
            observer.query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))?,
            1
        );
        batch.finish()?;
        assert_eq!(
            observer.query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))?,
            2
        );
        Ok(())
    }

    #[test]
    fn deferred_constraint_failure_is_reported_and_rolled_back() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch("CREATE TABLE rows (id BIGINT PRIMARY KEY)")?;
        let mut batch = Batch::new(&connection, "rows")?;
        batch.append([1_i64], 8)?;
        batch.append([1_i64], 8)?;
        let error = batch
            .finish()
            .err()
            .context("duplicate must fail at flush")?;
        assert!(format!("{error:#}").contains("flush failed"));
        assert_eq!(
            connection.query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))?,
            0
        );
        Ok(())
    }

    #[test]
    fn failed_append_cannot_be_followed_by_successful_finish() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch("CREATE TABLE rows (id BIGINT)")?;
        let mut batch = Batch::new(&connection, "rows")?;
        assert!(batch.append([1_i64], CURSOR_BYTES + 1).is_err());
        assert!(batch.finish().is_err());
        Ok(())
    }
}
