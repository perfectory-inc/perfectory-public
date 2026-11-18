//! The rows of one Trino query, read through Trino's own CLI in its container: the read path of
//! the Silver-to-Postgres loads (`unit_official_price_projection_load`, root ADR-0101, and
//! `real_estate_broker_lookup_load`, root ADR-0550).

use std::process::Stdio;

use anyhow::{bail, Context};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};

/// Trino's supported CLI owns protocol pagination, retries and cancellation.
pub struct TrinoRows {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
}

impl TrinoRows {
    /// Starts `sql` in `container`; its rows arrive one JSON object per line.
    ///
    /// # Errors
    /// The CLI cannot be started.
    pub fn start(container: &str, sql: &str) -> anyhow::Result<Self> {
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                container,
                "trino",
                "--output-format",
                "JSON",
                "--execute",
                sql,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("failed to start the Trino CLI")?;
        let stdout = child.stdout.take().context("Trino stdout is absent")?;
        Ok(Self {
            child,
            lines: BufReader::new(stdout).lines(),
        })
    }

    /// The next row, or none once the query has ended well.
    ///
    /// # Errors
    /// A row is not `T`, or the query failed.
    pub async fn next<T: serde::de::DeserializeOwned>(&mut self) -> anyhow::Result<Option<T>> {
        if let Some(line) = self
            .lines
            .next_line()
            .await
            .context("failed to read Trino output")?
        {
            return serde_json::from_str(&line)
                .context("Trino output is not the expected JSON row")
                .map(Some);
        }
        let status = self
            .child
            .wait()
            .await
            .context("failed to wait for Trino")?;
        if !status.success() {
            bail!("Trino failed with {status}; nothing it read is written");
        }
        Ok(None)
    }
}
