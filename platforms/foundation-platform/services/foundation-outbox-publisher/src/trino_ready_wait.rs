use std::{
    process::Command,
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};

use crate::public_data_control_support::optional_env_value;

const PREFIX: &str = "FOUNDATION_PLATFORM_TRINO_READY_WAIT";
const DEFAULT_CONTAINER_NAME: &str = "foundation-platform-trino";
const DEFAULT_DOCKER_PATH: &str = "docker";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let deadline = Instant::now() + Duration::from_secs(config.timeout_seconds);
    loop {
        if trino_is_ready(&config)? {
            println!("trino-ready-ok container={}", config.container_name);
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "Trino did not become healthy within {} seconds",
                config.timeout_seconds
            );
        }
        thread::sleep(Duration::from_secs(config.interval_seconds));
    }
}

struct Config {
    docker_path: String,
    container_name: String,
    timeout_seconds: u64,
    interval_seconds: u64,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let timeout_seconds = optional_u64("TIMEOUT_SECONDS")?.unwrap_or(120);
        let interval_seconds = optional_u64("INTERVAL_SECONDS")?.unwrap_or(2);
        if timeout_seconds < 1 {
            bail!("WaitTimeoutSeconds must be greater than zero");
        }
        if interval_seconds < 1 {
            bail!("WaitIntervalSeconds must be greater than zero");
        }
        Ok(Self {
            docker_path: optional_env_value(&key("DOCKER_PATH"))?
                .unwrap_or_else(|| DEFAULT_DOCKER_PATH.to_owned()),
            container_name: optional_env_value(&key("CONTAINER_NAME"))?
                .unwrap_or_else(|| DEFAULT_CONTAINER_NAME.to_owned()),
            timeout_seconds,
            interval_seconds,
        })
    }
}

fn trino_is_ready(config: &Config) -> anyhow::Result<bool> {
    let status_output = Command::new(&config.docker_path)
        .args([
            "inspect",
            "--format",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
            &config.container_name,
        ])
        .output()
        .with_context(|| format!("failed to run {}", config.docker_path))?;
    if !status_output.status.success() {
        return Ok(false);
    }

    let status = String::from_utf8_lossy(&status_output.stdout)
        .lines()
        .last()
        .unwrap_or_default()
        .trim()
        .to_owned();
    if status == "healthy" {
        return Ok(true);
    }
    if status != "running" {
        return Ok(false);
    }

    let probe = Command::new(&config.docker_path)
        .args([
            "exec",
            &config.container_name,
            "trino",
            "--execute",
            "SELECT 1",
        ])
        .output()
        .with_context(|| format!("failed to run {}", config.docker_path))?;
    Ok(probe.status.success())
}

fn optional_u64(name: &str) -> anyhow::Result<Option<u64>> {
    optional_env_value(&key(name))?
        .map(|raw| {
            raw.parse::<u64>()
                .with_context(|| format!("{name} must be an integer"))
        })
        .transpose()
}

fn key(name: &str) -> String {
    format!("{PREFIX}_{name}")
}
