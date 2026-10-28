//! The Python and Node half of `cargo xtask verify`. Moved out of `main.rs` unchanged.

use super::*;

pub(crate) fn verify_python_and_node(
    area: &Area,
    dir: &Path,
    selection: &SuiteSelection,
    log: &mut FailureLog,
) {
    let dir = dir.to_path_buf();
    // A covers root that points at nothing is a claim no one can honour. The
    // shell guard proves every discovered file is claimed; it cannot prove a
    // claim still resolves, so a directory rename would leave the declaration
    // pointing into space while the guard stayed green.
    for suite in area.python_tests {
        for root in suite.covers {
            let claimed = dir.join(root);
            if !claimed.is_dir() {
                log.record(format!(
                    "{} python suite '{}' claims covers root '{}', which is not a directory \
                     under {}. Fix the declaration or restore the tree.",
                    area.slug,
                    suite.dir,
                    root,
                    dir.display()
                ));
            }
        }
    }

    for (suite, plan) in area.python_tests.iter().zip(python_test_plans(area, &dir)) {
        // A selection keeps the discovered modules it names; a suite that cannot be split runs
        // whole when a selected test sits under one of its roots (root ADR-0269).
        let modules = python_discover_modules(&plan).map(|modules| {
            modules
                .into_iter()
                .filter(|args| {
                    selection.runs_test(&repo_path(area, &[suite.dir, &args[4], &args[6]]))
                })
                .collect::<Vec<_>>()
        });
        let selected = match &modules {
            Some(modules) => !modules.is_empty(),
            None => suite
                .covers
                .iter()
                .any(|root| selection.runs_any_under(&repo_path(area, &[root]))),
        };
        if !selected {
            continue;
        }
        // A suite that could not run cannot also be judged on its count; recording both would
        // report one failure twice and inflate the total the summary prints.
        let Some(output) = python_suite_output(&plan, modules, log) else {
            continue;
        };
        if executed_python_test_count(&output) == 0 {
            log.record(format!(
                "{} python suite '{}' executed 0 tests. Check the covers roots and the \
                 discovery pattern.",
                area.slug, suite.dir
            ));
        }
    }

    for (suite, plan) in area.node_tests.iter().zip(node_test_plans(area, &dir)) {
        if !selection.runs_node(&repo_path(area, &[suite.dir])) {
            continue;
        }
        if !plan.current_dir.join("package.json").is_file()
            || !plan.current_dir.join("pnpm-lock.yaml").is_file()
        {
            log.record(format!(
                "{} node suite '{}' must contain package.json and pnpm-lock.yaml",
                area.slug, suite.dir
            ));
            continue;
        }
        log.absorb(node_capturing_output(
            &plan.current_dir,
            &["install", "--frozen-lockfile"],
        ));
        for script in plan.scripts {
            let Some(output) =
                log.absorb(node_capturing_output(&plan.current_dir, &["run", script]))
            else {
                continue;
            };
            if *script == plan.test_script && executed_vitest_test_count(&output) == 0 {
                log.record(format!(
                    "{} node suite '{}' script '{}' executed 0 tests",
                    area.slug, suite.dir, script
                ));
            }
        }
    }
}

/// unittest reports `Ran N tests` on stderr; pytest reports `N passed` on stdout.
/// Both are read the same way and for the same reason as libtest's summary — see
/// [`executed_test_count`]. Parsing failure sums to zero, which fails closed.
pub(crate) fn executed_python_test_count(output: &str) -> usize {
    let mut total = 0usize;
    for line in output.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("Ran ") {
            if let Some(parsed) = rest
                .split_whitespace()
                .next()
                .and_then(|token| token.parse::<usize>().ok())
            {
                total += parsed;
                continue;
            }
        }
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if let Some(index) = tokens
            .iter()
            .position(|token| token.trim_end_matches(',') == "passed")
        {
            if let Some(parsed) = index
                .checked_sub(1)
                .and_then(|previous| tokens.get(previous))
                .and_then(|token| token.parse::<usize>().ok())
            {
                total += parsed;
            }
        }
    }
    total
}

/// Runs a Python suite and returns both streams joined.
///
/// [`cargo_capturing_stdout`] can inherit stderr because libtest writes its
/// summary to stdout. unittest does not: `Ran N tests` goes to stderr, so the
/// same choice here would discard the only signal a unittest suite emits.
///
/// The readback is not redundant with the runner's own exit code. Measured on
/// this repository: `unittest discover` matching nothing exits 5 under the host's
/// Python 3.13 but exits 0 and prints `OK` under the 3.11.2 in the pinned
/// verification image. The verdict would depend on where the command ran, which
/// is the local/CI split ADR-0004 exists to remove.
pub(crate) fn python_capturing_output(plan: &PythonCommandPlan) -> Result<String, String> {
    python_capturing_output_with(plan, plan.args)
}

/// How many test modules of one suite run at once (root ADR-0199).
pub(crate) const PYTHON_MODULE_WORKERS: usize = 4;

/// Runs one Python suite and returns everything it printed, or records why it failed.
///
/// A `unittest discover -s <root> -p <prefix>*<suffix>` suite whose root holds no package is
/// split into one process per test module, run [`PYTHON_MODULE_WORKERS`] at a time: on
/// Foundation CI `orchestration/tests` alone took 277 s as one process, a long tail of tests
/// that mostly wait on subprocesses (root ADR-0199). The modules are exactly the files discover
/// would load — it does not descend into a directory without `__init__.py` — and a root that has
/// a package keeps the single process. Every failing module is recorded, and the summed
/// `Ran N tests` still decides the zero-tests check.
pub(crate) fn python_suite_output(
    plan: &PythonCommandPlan,
    modules: Option<Vec<Vec<String>>>,
    log: &mut FailureLog,
) -> Option<String> {
    let Some(modules) = modules else {
        return log.absorb(python_capturing_output(plan));
    };
    let queue = std::sync::Mutex::new(modules.into_iter());
    let results = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..PYTHON_MODULE_WORKERS {
            scope.spawn(|| loop {
                let next = queue
                    .lock()
                    .map(|mut modules| modules.next())
                    .ok()
                    .flatten();
                let Some(args) = next else { break };
                let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
                let result = python_capturing_output_with(plan, &borrowed);
                if let Ok(mut results) = results.lock() {
                    results.push(result);
                }
            });
        }
    });
    let mut combined = String::new();
    let mut failed = false;
    for result in results.into_inner().unwrap_or_default() {
        match result {
            Ok(output) => combined.push_str(&output),
            Err(message) => {
                log.record(message);
                failed = true;
            }
        }
    }
    (!failed).then_some(combined)
}

/// The per-module argument lists for a splittable discover suite, or `None` to run it whole.
pub(crate) fn python_discover_modules(plan: &PythonCommandPlan) -> Option<Vec<Vec<String>>> {
    let [m, unittest, discover, s, root, p, pattern] = plan.args else {
        return None;
    };
    if (*m, *unittest, *discover, *s, *p) != ("-m", "unittest", "discover", "-s", "-p") {
        return None;
    }
    let (prefix, suffix) = pattern.split_once('*')?;
    if suffix.contains('*') {
        return None;
    }
    let mut modules = Vec::new();
    for entry in std::fs::read_dir(plan.current_dir.join(root)).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if path.join("__init__.py").is_file() {
                return None;
            }
            continue;
        }
        let name = path.file_name()?.to_str()?.to_owned();
        if name.starts_with(prefix) && name.ends_with(suffix) && name.len() >= pattern.len() - 1 {
            modules.push(name);
        }
    }
    modules.sort();
    Some(
        modules
            .into_iter()
            .map(|module| {
                [
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    root,
                    "-p",
                    module.as_str(),
                ]
                .map(str::to_owned)
                .to_vec()
            })
            .collect(),
    )
}

pub(crate) fn python_capturing_output_with(
    plan: &PythonCommandPlan,
    args: &[&str],
) -> Result<String, String> {
    let mut command = Command::new("python3");
    command
        .current_dir(&plan.current_dir)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(python_path) = plan.python_path {
        command.env("PYTHONPATH", python_path);
    }
    let rendered = format!("{command:?}");
    let output = match command.output() {
        Ok(output) => output,
        Err(error) => return Err(format!("could not spawn {rendered}: {error}")),
    };
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    print!("{stdout}");
    eprint!("{stderr}");
    if !output.status.success() {
        return Err(format!("({}) {rendered}", output.status));
    }
    Ok(format!("{stdout}{stderr}"))
}

pub(crate) fn python_test_plans(area: &Area, area_dir: &Path) -> Vec<PythonCommandPlan> {
    area.python_tests
        .iter()
        .map(|suite| PythonCommandPlan {
            current_dir: area_dir.join(suite.dir),
            python_path: suite.python_path,
            args: suite.args,
        })
        .collect()
}

pub(crate) fn node_test_plans(area: &Area, area_dir: &Path) -> Vec<NodeCommandPlan> {
    area.node_tests
        .iter()
        .map(|suite| NodeCommandPlan {
            current_dir: area_dir.join(suite.dir),
            scripts: suite.scripts,
            test_script: suite.test_script,
        })
        .collect()
}

/// Vitest prints `Tests  N passed (...)`; the test count, not the process exit code, proves
/// discovery selected something. A missing or changed summary fails closed as zero.
pub(crate) fn executed_vitest_test_count(output: &str) -> usize {
    output
        .lines()
        .filter_map(|line| {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.first().copied() != Some("Tests") {
                return None;
            }
            let passed = tokens.iter().position(|token| *token == "passed")?;
            tokens.get(passed.checked_sub(1)?)?.parse::<usize>().ok()
        })
        .sum()
}

pub(crate) fn node_capturing_output(dir: &Path, args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("corepack");
    command
        .current_dir(dir)
        .env("CI", "true")
        .env("NO_COLOR", "1")
        .env("WRANGLER_SEND_METRICS", "false")
        .arg("pnpm")
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let rendered = format!("{command:?}");
    let output = command
        .output()
        .map_err(|error| format!("could not spawn {rendered}: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    print!("{stdout}");
    eprint!("{stderr}");
    if !output.status.success() {
        return Err(format!("({}) {rendered}", output.status));
    }
    Ok(format!("{stdout}{stderr}"))
}
