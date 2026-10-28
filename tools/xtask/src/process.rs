//! Process, apt and failure-log helpers shared by every xtask command. Moved out of `main.rs`
//! unchanged.

use super::*;

/// The repository root: xtask lives at `<root>/tools/xtask`, so climb two parents
/// from its manifest dir. This makes area paths resolve regardless of the caller's
/// working directory.
pub(crate) fn repo_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| {
            eprintln!("xtask: cannot locate repository root from {manifest:?}");
            exit(1);
        })
        .to_path_buf()
}

/// Like `cargo`, but keeps stdout so libtest's `test result:` summary can be
/// read back — the only way to tell "verified" from "selected nothing".
///
/// stderr stays inherited, so cargo's compile progress still streams live during
/// long builds; the captured stdout is echoed the moment the command ends, so
/// CI logs lose nothing.
pub(crate) fn cargo_capturing_stdout(area_dir: &Path, args: &[&str]) -> Result<String, String> {
    let mut command = cargo_command(area_dir, args);
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    let rendered = format!("{command:?}");
    let output = match command.output() {
        Ok(output) => output,
        Err(error) => return Err(format!("could not spawn {rendered}: {error}")),
    };
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    print!("{stdout}");
    if !output.status.success() {
        return Err(format!("({}) {rendered}", output.status));
    }
    Ok(stdout)
}

/// Install Debian packages needed by an area's complete verification suite. No-op
/// when empty. On the rust Docker image we are root (no sudo); on the CI runner we
/// are not (sudo). apt is idempotent, so re-running is cheap.
pub(crate) fn ensure_apt(deps: &[&str]) {
    if deps.is_empty() {
        return;
    }
    // Already satisfied is the normal case: the verification image bakes these in, and a CI runner
    // that installed them once keeps them for the job. Returning here is what lets the harness run
    // the verification with no network at all — an `apt-get update` would be the only reason it
    // needed one, and a step that reaches the internet is a step that can fail for reasons the
    // thing being verified has nothing to do with.
    let missing: Vec<&str> = deps
        .iter()
        .copied()
        .filter(|dep| !debian_package_installed(dep))
        .collect();
    if missing.is_empty() {
        return;
    }
    if !tool_exists("apt-get") {
        eprintln!(
            "xtask: apt-get not found; install these manually before verifying: {}",
            missing.join(" ")
        );
        return;
    }
    let sudo = !is_root();
    // `apt-get update` exits 0 even when every source failed — measured, seven errors and a zero.
    // Its status therefore proves nothing, and only the install below is a real judgment.
    let _ = run_reporting(apt(sudo).arg("update"));
    run(apt(sudo)
        .args(["install", "-y", "--no-install-recommends"])
        .args(&missing));
}

/// Whether dpkg reports a package as installed.
///
/// `command -v` cannot answer this: half of these deps are `-dev` packages that ship headers and no
/// executable, so a binary probe would call them missing on an image that already has them and
/// reach for the network every run.
pub(crate) fn debian_package_installed(package: &str) -> bool {
    Command::new("dpkg-query")
        .args(["-W", "-f=${db:Status-Status}", package])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim() == "installed")
        .unwrap_or(false)
}

/// apt with a bound on how long one mirror may stall. apt's own default waits on a silent
/// connection for as long as it stays open: on 2026-10-07 two merge-queue runs sat at
/// `Get: https://archive.ubuntu.com/ubuntu noble-security InRelease` for 23 minutes, until the
/// job's timeout cancelled the Postgres lane and the queue dropped both PRs. With a timeout and
/// retries a stalled mirror costs seconds, and the install below still judges the result.
pub(crate) fn apt(sudo: bool) -> Command {
    let mut c = if sudo {
        let mut c = Command::new("sudo");
        c.arg("apt-get");
        c
    } else {
        Command::new("apt-get")
    };
    c.args([
        "-o",
        "Acquire::http::Timeout=30",
        "-o",
        "Acquire::https::Timeout=30",
        "-o",
        "Acquire::Retries=3",
    ]);
    c
}

pub(crate) fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim() == "0")
        .unwrap_or(false)
}

pub(crate) fn tool_exists(name: &str) -> bool {
    Command::new(name)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run a command; on failure print it and exit with its code. This is a gate, not
/// a library — failing fast with the exact command is the right behaviour.
pub(crate) fn run(command: &mut Command) {
    let mut log = FailureLog::default();
    log.absorb(run_reporting(command));
    log.finish("command");
}

/// Collects every failure of an authoritative run instead of stopping at the first.
///
/// [ADR-0012](../../../docs/adr/0012-verification-results-must-mean-what-they-say.md) rule 1: an
/// authoritative gate reports *all* failures, because an auditor who is shown one failure cannot
/// tell whether it is the only one. Fail-fast is useful for a local loop and is what running the
/// individual `cargo` commands gives; `verify` and the lanes are the gate, so they are exhaustive.
///
/// The cost is real — a broken build means clippy and every test command still run and still fail —
/// and it is the point. The alternative is what this session kept paying: fix, wait for a full
/// rebuild, discover the next failure, repeat.
#[derive(Default)]
pub(crate) struct FailureLog {
    pub(crate) failures: Vec<String>,
}

impl FailureLog {
    /// Records a failure and reports it immediately, so a long run still streams its bad news.
    pub(crate) fn record(&mut self, message: String) {
        eprintln!("xtask: FAILED: {message}");
        self.failures.push(message);
    }

    /// Records the error half of a reporting runner's result, keeping the success half.
    pub(crate) fn absorb<T>(&mut self, result: Result<T, String>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(message) => {
                self.record(message);
                None
            }
        }
    }

    /// Exits non-zero after listing everything that failed, or returns if nothing did.
    pub(crate) fn finish(self, what: &str) {
        if self.failures.is_empty() {
            return;
        }
        eprintln!(
            "\nxtask: {what} failed — {} failure(s), all of them:",
            self.failures.len()
        );
        for (index, failure) in self.failures.iter().enumerate() {
            eprintln!("  {}. {failure}", index + 1);
        }
        exit(1);
    }
}

pub(crate) fn run_reporting(command: &mut Command) -> Result<(), String> {
    let rendered = format!("{command:?}");
    match command.status() {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(format!("({status}) {rendered}")),
        Err(error) => Err(format!("could not spawn {rendered}: {error}")),
    }
}

/// Run `cargo <args>` in `area_dir`, reporting failure instead of exiting.
///
/// `SQLX_OFFLINE` matches CI and the harness, so compile-time query checks never need a live
/// database. The exiting form this replaced is gone: every caller sat inside an authoritative gate,
/// and a gate that exits on its first failure cannot obey ADR-0012 rule 1.
pub(crate) fn cargo_reporting(area_dir: &Path, args: &[&str]) -> Result<(), String> {
    run_reporting(&mut cargo_command(area_dir, args))
}

pub(crate) fn cargo_command(area_dir: &Path, args: &[&str]) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(area_dir)
        .env("SQLX_OFFLINE", "true")
        .args(args);
    remove_parent_crate_metadata(&mut command);
    command
}

pub(crate) fn remove_parent_crate_metadata(command: &mut Command) {
    // `cargo run` injects xtask's crate metadata. Native build scripts can track
    // those inherited values with rerun-if-env-changed, invalidating direct Cargo
    // builds when the caller alternates. Keep user build/cache settings intact.
    let keys: Vec<_> = std::env::vars_os()
        .map(|(key, _)| key)
        .chain(command.get_envs().map(|(key, _)| key.to_owned()))
        .filter(|key| {
            let key = key.to_string_lossy();
            key.starts_with("CARGO_MANIFEST_")
                || key.starts_with("CARGO_PKG_")
                || key.starts_with("CARGO_BIN_EXE_")
                || matches!(
                    key.as_ref(),
                    "CARGO_CRATE_NAME"
                        | "CARGO_BIN_NAME"
                        | "CARGO_PRIMARY_PACKAGE"
                        | "CARGO_TARGET_TMPDIR"
                )
        })
        .collect();
    for key in keys {
        command.env_remove(key);
    }
}

/// A workflow that reaches cargo through a script is invisible to the static workflow guard
/// (scripts/guard/rust-ci-setup.sh), so xtask -- the entry point of every CI cargo lane -- checks
/// the marker the shared setup exports. Outside GitHub Actions nothing changes.
pub(crate) fn ci_rust_setup_missing(
    github_actions: Option<&str>,
    rust_setup: Option<&str>,
) -> bool {
    github_actions == Some("true") && rust_setup.is_none_or(str::is_empty)
}

pub(crate) fn fail_usage(message: &str) -> ! {
    eprintln!("xtask: {message}");
    exit(2);
}
