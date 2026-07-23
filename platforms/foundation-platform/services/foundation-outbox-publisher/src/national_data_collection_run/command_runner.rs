use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Command,
    time::{Duration, Instant, SystemTime},
};

pub(super) struct CommandRun {
    pub(super) started_at: SystemTime,
    pub(super) duration: Duration,
    pub(super) exit_code: i32,
}

pub(super) fn invoke_outbox_command(
    root: &Path,
    cargo: &Path,
    command_name: &str,
    envs: &BTreeMap<String, String>,
    remove_envs: &[&str],
    log_path: &Path,
) -> anyhow::Result<CommandRun> {
    let started_at = SystemTime::now();
    let timer = Instant::now();
    let mut command = outbox_subcommand(cargo, command_name);
    command.current_dir(root);
    for name in remove_envs {
        command.env_remove(name);
    }
    for (name, value) in envs {
        command.env(name, value);
    }
    let output = command.output()?;
    let duration = timer.elapsed();
    let exit_code = output.status.code().unwrap_or(1);
    append_log(log_path, &format!("command={command_name}"))?;
    append_command_output(log_path, output.stdout)?;
    append_command_output(log_path, output.stderr)?;
    Ok(CommandRun {
        started_at,
        duration,
        exit_code,
    })
}

/// Builds the command that runs an outbox-publisher `command_name` subcommand.
///
/// Prefers re-invoking the current binary directly so production never needs the Cargo toolchain;
/// falls back to `cargo run -p foundation-outbox-publisher -- <command_name>` (using the resolved
/// `cargo` path) only when the current executable cannot be resolved.
fn outbox_subcommand(cargo: &Path, command_name: &str) -> Command {
    match std::env::current_exe() {
        Ok(exe) => {
            let mut command = Command::new(exe);
            command.arg(command_name);
            command
        }
        Err(_) => {
            let mut command = Command::new(cargo);
            command.args([
                "run",
                "-p",
                "foundation-outbox-publisher",
                "--",
                command_name,
            ]);
            command
        }
    }
}

pub(super) fn read_last_json_from_command_log(
    path: &Path,
    command_name: &str,
) -> anyhow::Result<String> {
    let lines = fs::read_to_string(path)?;
    let marker = format!("command={command_name}");
    let mut capture = Vec::new();
    let mut in_section = false;
    for line in lines.lines() {
        if line == marker {
            in_section = true;
            capture.clear();
            continue;
        }
        if in_section && line.starts_with("command=") {
            in_section = false;
        }
        if in_section {
            capture.push(line.to_owned());
        }
    }
    let start = capture
        .iter()
        .position(|line| line.trim_start().starts_with('{'))
        .ok_or_else(|| anyhow::anyhow!("{command_name} did not emit a JSON object"))?;
    Ok(capture[start..].join("\n"))
}

fn append_log(path: &Path, line: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut content = fs::read_to_string(path).unwrap_or_default();
    content.push_str(line);
    content.push('\n');
    fs::write(path, content)?;
    Ok(())
}

fn append_command_output(path: &Path, bytes: Vec<u8>) -> anyhow::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&bytes);
    // Child collector output can echo provider request URLs; redact credential query params.
    let text = outbound_http_infrastructure::redact_url_query_secrets(&text);
    let mut content = fs::read_to_string(path).unwrap_or_default();
    content.push_str(&text);
    if !content.ends_with('\n') {
        content.push('\n');
    }
    fs::write(path, content)?;
    Ok(())
}
