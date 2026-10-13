use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{bail, Context};

use crate::public_data_control_support::resolve_cargo_exe;

use super::Config;

pub(in crate::national_data_collection_ledger_execute) struct Runner {
    mode: RunnerMode,
}

enum RunnerMode {
    DirectExecutable(PathBuf),
    Cargo(PathBuf),
}

impl Runner {
    pub(in crate::national_data_collection_ledger_execute) fn resolve(
        config: &Config,
    ) -> anyhow::Result<Self> {
        if let Some(path) = &config.runner_exe {
            if !path.is_file() {
                bail!("RunnerExe does not exist: {}", path.display());
            }
            return Ok(Self {
                mode: RunnerMode::DirectExecutable(path.clone()),
            });
        }
        // Prefer re-invoking the current binary directly so production never needs the Cargo
        // toolchain; fall back to `cargo run` only when the current executable cannot be resolved.
        if let Ok(exe) = std::env::current_exe() {
            return Ok(Self {
                mode: RunnerMode::DirectExecutable(exe),
            });
        }
        Ok(Self {
            mode: RunnerMode::Cargo(resolve_cargo_exe(config.cargo_exe.clone())?),
        })
    }

    pub(in crate::national_data_collection_ledger_execute) fn invoke(
        &self,
        root: &Path,
        outbox_command: &str,
        child_env: &BTreeMap<String, String>,
    ) -> anyhow::Result<RunnerRun> {
        let started_at = SystemTime::now();
        let started = Instant::now();
        let mut command = match &self.mode {
            RunnerMode::DirectExecutable(path) => {
                let mut command = Command::new(path);
                command.arg(outbox_command);
                command
            }
            RunnerMode::Cargo(path) => {
                let mut command = Command::new(path);
                command
                    .args(["run", "-p", "foundation-outbox-publisher", "--"])
                    .arg(outbox_command);
                command
            }
        };
        let output = command
            .current_dir(root)
            .envs(child_env)
            .output()
            .with_context(|| format!("failed to run outbox command: {outbox_command}"))?;
        let exit_code = output.status.code().unwrap_or(1);
        let mut lines = lines_from_bytes(&output.stdout);
        lines.extend(lines_from_bytes(&output.stderr));
        Ok(RunnerRun {
            started_at,
            duration: started.elapsed(),
            exit_code,
            output: lines,
        })
    }
}

pub(in crate::national_data_collection_ledger_execute) struct RunnerRun {
    pub(in crate::national_data_collection_ledger_execute) started_at: SystemTime,
    pub(in crate::national_data_collection_ledger_execute) duration: Duration,
    pub(in crate::national_data_collection_ledger_execute) exit_code: i32,
    pub(in crate::national_data_collection_ledger_execute) output: Vec<String>,
}

fn lines_from_bytes(bytes: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(ToOwned::to_owned)
        .collect()
}
