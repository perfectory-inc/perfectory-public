//! The cargo half of `cargo xtask verify`: clippy, builds, tests, and the pinned tools they use.
//! Moved out of `main.rs` unchanged.

use super::*;

/// clippy over the workspace, or over the affected packages; `None` when no crate is affected.
pub(crate) fn clippy_command(selection: &Selection) -> Option<Vec<String>> {
    let packages: Vec<String> = match selection {
        Selection::All => vec!["--workspace".to_owned()],
        Selection::Some { affected, .. } if affected.is_empty() => return None,
        Selection::Some { affected, .. } => affected
            .iter()
            .flat_map(|package| ["-p".to_owned(), package.clone()])
            .collect(),
    };
    // `--timings` writes the per-crate build report `summarize_build_times` reads (root ADR-0269).
    let head = ["clippy", "--locked", "--timings"].map(str::to_owned);
    let tail = ["--all-features", "--all-targets", "--", "-D", "warnings"].map(str::to_owned);
    Some(head.into_iter().chain(packages).chain(tail).collect())
}

/// The `cargo test` selections to run, each with the nextest filterset that narrows it. Every
/// affected package is built; a feature-gated one keeps its own default-features stage, as in
/// `verify_test_commands`.
pub(crate) fn test_selections(
    area: &Area,
    selection: &Selection,
) -> Vec<(Vec<String>, Option<String>)> {
    let Selection::Some {
        changed,
        readers,
        affected,
    } = selection
    else {
        return verify_test_commands(area)
            .into_iter()
            .map(|command| (command.into_iter().map(str::to_owned).collect(), None))
            .collect();
    };
    let filterset = changed
        .iter()
        .map(|package| format!("rdeps(={package})"))
        .chain(readers.iter().map(|package| format!("package(={package})")))
        .collect::<Vec<_>>()
        .join(" | ");
    let gated = feature_gated_packages(area);
    let packages = |gated_stage: bool| -> Vec<String> {
        affected
            .iter()
            .filter(|package| gated.contains(&package.as_str()) == gated_stage)
            .flat_map(|package| ["-p".to_owned(), package.clone()])
            .collect()
    };
    let mut commands = Vec::new();
    let ungated = packages(false);
    if !ungated.is_empty() {
        let head = ["test", "--locked", "--all-features"].map(str::to_owned);
        commands.push(head.into_iter().chain(ungated).collect());
    }
    // One stage per feature-gated package: each `-p <name>` pair.
    for package in packages(true).chunks(2) {
        let head = ["test", "--locked"].map(str::to_owned);
        commands.push(head.into_iter().chain(package.iter().cloned()).collect());
    }
    commands
        .into_iter()
        .map(|command| (command, Some(filterset.clone())))
        .collect()
}

/// The checks oxidecomputer/omicron runs on its workspace-hack crate (root ADR-0269), for a
/// workspace with `.config/hakari.toml`: the crate is what `cargo hakari generate` would write,
/// and every member depends on it. Both exit non-zero otherwise. They read `cargo metadata`
/// only, so they run whatever crates a pull request selects.
pub(crate) const HAKARI_CHECKS: [[&str; 3]; 2] = [
    ["hakari", "generate", "--diff"],
    ["hakari", "manage-deps", "--dry-run"],
];

pub(crate) fn verify_workspace_hack(dir: &Path, log: &mut FailureLog) {
    if !dir.join(".config/hakari.toml").is_file() {
        return;
    }
    let Some(hakari) = pinned_tool_dir("cargo-hakari", log) else {
        return;
    };
    for check in HAKARI_CHECKS {
        let mut cargo = cargo_command(dir, &check);
        cargo.env("PATH", path_with(&hakari));
        log.absorb(run_reporting(&mut cargo));
    }
}

pub(crate) fn verify_cargo(area: &Area, dir: &Path, selection: &Selection, log: &mut FailureLog) {
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    verify_cargo_builds(area, dir, selection, log);
    summarize_build_times(area.dir, started);
}

/// Prints the crates this run spent the longest building, from the `--timings` reports its clippy
/// and test builds wrote (root ADR-0269, as omicron's build analysis). A report, never a verdict:
/// the script prints why when there is nothing to show and always exits 0.
pub(crate) fn summarize_build_times(workspace: &str, started: u64) {
    let since = started.to_string();
    let summary = Command::new("python3")
        .current_dir(repo_root())
        .args(["scripts/ci/cargo_timings.py", "summarize", "--workspace"])
        .args([workspace, "--since", since.as_str()])
        .status();
    if let Err(error) = summary {
        eprintln!("xtask: no build-time summary: could not run python3: {error}");
    }
}

pub(crate) fn verify_cargo_builds(
    area: &Area,
    dir: &Path,
    selection: &Selection,
    log: &mut FailureLog,
) {
    let dir = dir.to_path_buf();
    log.absorb(cargo_reporting(&dir, &["fmt", "--all", "--", "--check"]));
    verify_workspace_hack(&dir, log);
    let Some(clippy) = clippy_command(selection) else {
        eprintln!(
            "xtask: {}: this pull request changes no crate; clippy and the Rust tests have nothing to verify",
            area.slug
        );
        return;
    };
    let args: Vec<&str> = clippy.iter().map(String::as_str).collect();
    log.absorb(cargo_reporting(&dir, &args));

    // The Rust tests run under cargo-nextest: one process per test, as many at once as the
    // machine has cores, where `cargo test` ran each test binary after the last (root ADR-0205).
    // nextest does not run doctests, so each selection also runs `cargo test --doc`.
    let Some(nextest) = pinned_tool_dir("cargo-nextest", log) else {
        return;
    };
    for (selection, filterset) in test_selections(area, selection) {
        let [mut tests, doctests] = nextest_commands(&selection);
        tests.push("--timings".to_owned());
        if let Some(filterset) = filterset {
            // A selected crate may have no tests of its own; the whole-workspace run keeps
            // nextest's default, which fails when nothing runs.
            tests.extend(["-E".to_owned(), filterset, "--no-tests=warn".to_owned()]);
        }
        let args: Vec<&str> = tests.iter().map(String::as_str).collect();
        let mut cargo = cargo_command(&dir, &args);
        cargo.env("PATH", path_with(&nextest));
        log.absorb(run_reporting(&mut cargo));
        let args: Vec<&str> = doctests.iter().map(String::as_str).collect();
        log.absorb(doctests_reporting(&mut cargo_command(&dir, &args)));
    }
}

/// Runs `cargo test --doc`; a selection without a library has no doctests, which cargo reports
/// as an error (`no library targets found`). That one answer means "nothing to run" — the tests
/// themselves already ran under nextest — and every other failure is a failure.
pub(crate) fn doctests_reporting(command: &mut Command) -> Result<(), String> {
    let rendered = format!("{command:?}");
    let output = command
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|error| format!("could not spawn {rendered}: {error}"))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprint!("{stderr}");
    print!("{}", String::from_utf8_lossy(&output.stdout));
    if output.status.success() || no_doctest_targets(&stderr) {
        return Ok(());
    }
    Err(format!("({}) {rendered}", output.status))
}

/// CI sets `CARGO_TERM_COLOR=always`, so cargo wraps `error` in colour codes even into a pipe;
/// the excuse is read from the text without them.
pub(crate) fn no_doctest_targets(stderr: &str) -> bool {
    let plain = without_ansi_colour(stderr);
    let errors: Vec<&str> = plain
        .lines()
        .filter(|line| line.starts_with("error"))
        .collect();
    !errors.is_empty()
        && errors
            .iter()
            .all(|line| line.starts_with("error: no library targets found in package"))
}

/// `text` without ANSI SGR sequences (`ESC [ … m`), the only escapes cargo's colour writes.
pub(crate) fn without_ansi_colour(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(character) = chars.next() {
        if character == '\u{1b}' {
            for inner in chars.by_ref() {
                if inner == 'm' {
                    break;
                }
            }
        } else {
            plain.push(character);
        }
    }
    plain
}

/// The two runs one `cargo test` selection becomes: nextest for the test targets, then the
/// doctests nextest cannot run. Same packages, features and exclusions in both.
pub(crate) fn nextest_commands<S: AsRef<str>>(selection: &[S]) -> [Vec<String>; 2] {
    let rest = selection
        .iter()
        .map(|arg| arg.as_ref().to_owned())
        .skip(usize::from(
            selection.first().is_some_and(|arg| arg.as_ref() == "test"),
        ));
    let nextest = ["nextest", "run", "--no-fail-fast"]
        .map(str::to_owned)
        .into_iter();
    let doc = ["test", "--doc"].map(str::to_owned).into_iter();
    [
        nextest.chain(rest.clone()).collect(),
        doc.chain(rest).collect(),
    ]
}

pub(crate) fn path_with(directory: &Path) -> std::ffi::OsString {
    let mut paths = vec![directory.to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    std::env::join_paths(paths).unwrap_or_default()
}

/// The directory holding the pinned `cargo-<tool>` (`cargo-nextest`, root ADR-0205;
/// `cargo-hakari`, root ADR-0269), fetched on first use.
///
/// The version and checksums live in `tools/<tool>.env`; the one pinned fetcher
/// (`scripts/ci/fetch_pinned.py`, root ADR-0197) downloads and verifies the archive, so CI
/// restores it from its cache and a laptop fetches it once. Any failure is a recorded failure:
/// the check did not run, which is never a pass.
pub(crate) fn pinned_tool_dir(tool: &str, log: &mut FailureLog) -> Option<PathBuf> {
    let root = repo_root();
    let pins_file = format!("tools/{tool}.env");
    let variable = format!("{}_VERSION=", tool.to_uppercase().replace('-', "_"));
    let pins = std::fs::read_to_string(root.join(&pins_file)).unwrap_or_default();
    let Some(version) = pins
        .lines()
        .find_map(|line| line.strip_prefix(variable.as_str()))
        .map(str::trim)
        .filter(|version| !version.is_empty())
    else {
        log.record(format!("{pins_file} does not pin {variable}<version>"));
        return None;
    };
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    let dir = PathBuf::from(home)
        .join(".cache/perfectory/bin")
        .join(format!("{tool}-{version}"));
    let member = if cfg!(windows) {
        format!("{tool}.exe")
    } else {
        tool.to_owned()
    };
    let binary = dir.join(&member);
    if binary.is_file() {
        return Some(dir);
    }
    let fetched = run_reporting(
        Command::new("python3")
            .current_dir(&root)
            .args([
                "scripts/ci/fetch_pinned.py",
                "tool",
                tool,
                "--member",
                &member,
            ])
            .arg("--output")
            .arg(&binary),
    );
    if let Err(message) = fetched {
        log.record(format!("could not install the pinned {tool}: {message}"));
        return None;
    }
    Some(dir)
}
