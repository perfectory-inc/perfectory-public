//! What a `--affected` run verifies: the Python/Node suites and the crates a change reaches
//! (root ADR-0269). Moved out of `main.rs` unchanged.

use super::*;

/// Which Python test modules and Node packages a `--affected` run verifies, as
/// `scripts/ci/change_scope.py suites` answers (root ADR-0269). Paths are from the repository root.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SuiteSelection {
    All,
    Some {
        tests: Vec<String>,
        nodes: Vec<String>,
    },
}

impl SuiteSelection {
    pub(crate) fn runs_test(&self, path: &str) -> bool {
        match self {
            Self::All => true,
            Self::Some { tests, .. } => tests.iter().any(|test| test == path),
        }
    }

    pub(crate) fn runs_any_under(&self, dir: &str) -> bool {
        match self {
            Self::All => true,
            Self::Some { tests, .. } => tests
                .iter()
                .any(|test| test.starts_with(&format!("{dir}/"))),
        }
    }

    pub(crate) fn runs_node(&self, dir: &str) -> bool {
        match self {
            Self::All => true,
            Self::Some { nodes, .. } => nodes.iter().any(|node| node == dir),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Some { tests, nodes } if tests.is_empty() && nodes.is_empty())
    }
}

/// `<area>/<parts…>` with `.` segments dropped, the form the selector prints.
pub(crate) fn repo_path(area: &Area, parts: &[&str]) -> String {
    std::iter::once(area.dir)
        .chain(parts.iter().copied())
        .flat_map(|part| part.split('/'))
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>()
        .join("/")
}

/// The Python and Node suites, narrowed with `--affected` to the test modules and packages the
/// change scope says a pull request reaches (root ADR-0269): a module that changed or imports,
/// or names, what changed. A selector that cannot answer is a failure.
pub(crate) fn verify_suites(area: &Area, dir: &Path, affected: bool, log: &mut FailureLog) {
    let has_suites = !area.python_tests.is_empty() || !area.node_tests.is_empty();
    let selection = if affected && has_suites {
        let Some((selection, reason)) = log.absorb(suite_selection(area)) else {
            return;
        };
        if selection.is_empty() {
            eprintln!(
                "xtask: {}: the Python and Node suites have nothing to verify: {reason}",
                area.slug
            );
            return;
        }
        eprintln!("xtask: {}: the suites run: {reason}", area.slug);
        selection
    } else {
        SuiteSelection::All
    };
    verify_python_and_node(area, dir, &selection, log);
}

/// `scripts/ci/change_scope.py suites`'s answer and its reason.
pub(crate) fn suite_selection(area: &Area) -> Result<(SuiteSelection, String), String> {
    let mut command = Command::new("python3");
    command.current_dir(repo_root()).args([
        "scripts/ci/change_scope.py",
        "suites",
        "--workspace",
        area.dir,
    ]);
    let rendered = format!("{command:?}");
    let output = command
        .stderr(std::process::Stdio::inherit())
        .output()
        .map_err(|error| format!("could not spawn {rendered}: {error}"))?;
    if !output.status.success() {
        return Err(format!("({}) {rendered}", output.status));
    }
    parse_suite_selection(&String::from_utf8_lossy(&output.stdout))
}

/// Reads `all <why>`, or `some <why>` followed by `test <path>` and `node <dir>` lines
/// (tab-separated); anything else is a failure.
pub(crate) fn parse_suite_selection(stdout: &str) -> Result<(SuiteSelection, String), String> {
    let refuse = || format!("unreadable suite selection {stdout:?}");
    let mut lines = stdout.lines();
    let (kind, reason) = lines
        .next()
        .and_then(|line| line.split_once('\t'))
        .ok_or_else(refuse)?;
    let (mut tests, mut nodes) = (Vec::new(), Vec::new());
    for line in lines {
        match line.split_once('\t') {
            Some(("test", path)) => tests.push(path.to_owned()),
            Some(("node", path)) => nodes.push(path.to_owned()),
            _ => return Err(refuse()),
        }
    }
    let selection = match kind {
        "all" if tests.is_empty() && nodes.is_empty() => SuiteSelection::All,
        "some" => SuiteSelection::Some { tests, nodes },
        _ => return Err(refuse()),
    };
    Ok((selection, reason.to_owned()))
}

/// Runs `side` on its own thread while `main` runs on this one, and puts both sides' failures in
/// `log`. A side that panics is itself a failure, never a silent pass.
pub(crate) fn run_beside(
    log: &mut FailureLog,
    side_name: &str,
    main: impl FnOnce(&mut FailureLog),
    side: impl FnOnce(&mut FailureLog) + Send,
) {
    let joined = std::thread::scope(|scope| {
        let handle = scope.spawn(|| {
            let mut side_log = FailureLog::default();
            side(&mut side_log);
            side_log
        });
        main(log);
        handle.join()
    });
    match joined {
        Ok(side_log) => log.failures.extend(side_log.failures),
        Err(_) => log.record(format!(
            "{side_name} thread panicked; its suites did not report"
        )),
    }
}

/// The crates `--affected` verifies, as `scripts/ci/change_scope.py crates` answers (root ADR-0269).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Selection {
    /// Every crate: no `--affected`, not a pull request, or a run-all input changed.
    All,
    Some {
        /// Changed members: their tests and every dependent's run (nextest `rdeps()`).
        changed: Vec<String>,
        /// Members whose tests read a changed member's sources: their own tests run (`package()`).
        readers: Vec<String>,
        /// Every member built, linted and tested; empty when the change reaches no crate.
        affected: Vec<String>,
    },
}

impl Selection {
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Some { affected, .. } if affected.is_empty())
    }

    pub(crate) fn verifies(&self, package: &str) -> bool {
        match self {
            Self::All => true,
            Self::Some { affected, .. } => affected.iter().any(|name| name == package),
        }
    }
}

/// Reads the selector's tab-separated answer: `all|some <why>`, then `changed|reader|affected <name>`.
pub(crate) fn parse_selection(stdout: &str) -> Result<Selection, String> {
    let mut lines = stdout.lines();
    let head = lines.next().unwrap_or_default();
    let (kind, reason) = head.split_once('\t').unwrap_or((head, ""));
    let (mut changed, mut readers, mut affected) = (Vec::new(), Vec::new(), Vec::new());
    for line in lines {
        match line.split_once('\t') {
            Some(("changed", name)) => changed.push(name.to_owned()),
            Some(("reader", name)) => readers.push(name.to_owned()),
            Some(("affected", name)) => affected.push(name.to_owned()),
            _ => return Err(format!("unreadable crate selection line {line:?}")),
        }
    }
    let selection = match kind {
        "all" if changed.is_empty() && readers.is_empty() && affected.is_empty() => Selection::All,
        "some" => Selection::Some {
            changed,
            readers,
            affected,
        },
        _ => return Err(format!("unreadable crate selection {head:?}")),
    };
    eprintln!("xtask: crate selection: {kind} ({reason})");
    Ok(selection)
}

/// Asks the one change scope (root ADR-0202) which of the area's crates a pull request reaches.
/// The comparison comes from `CHANGE_SCOPE_EVENT`, `CHANGE_SCOPE_BASE` and `CHANGE_SCOPE_HEAD`.
pub(crate) fn crate_selection(area: &Area) -> Result<Selection, String> {
    let mut command = Command::new("python3");
    command.current_dir(repo_root()).args([
        "scripts/ci/change_scope.py",
        "crates",
        "--workspace",
        area.dir,
    ]);
    let rendered = format!("{command:?}");
    let output = command
        .stderr(std::process::Stdio::inherit())
        .output()
        .map_err(|error| format!("could not spawn {rendered}: {error}"))?;
    if !output.status.success() {
        return Err(format!("({}) {rendered}", output.status));
    }
    parse_selection(&String::from_utf8_lossy(&output.stdout))
}
