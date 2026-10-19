//! `cargo xtask npm <command>`: the npm overrides, lockfile and advisory commands of root ADR-0158.
//!
//! - `refresh-locks [tree ...]` regenerates every tracked `pnpm-lock.yaml` (or the named trees) with the
//!   toolchain the trees pin: `corepack pnpm@<pin> install --lockfile-only` inside the pinned Node image,
//!   so a laptop and a CI runner write the same bytes and no install script runs; the overrides check
//!   runs last.
//! - `prepare-advisory-fix <osv-report.json> <out-dir>` turns a red OSV ratchet into a patch and a summary:
//!   the floors the report asks for, every manifest re-rendered, every lock regenerated, and whether the
//!   change closes the ratchet. Nothing is committed or pushed.
//! - `open-advisory-fix-pr [<run-id>]` applies the patch a failed repository guard run prepared (or
//!   prepares one here) on a branch from `origin/main` and opens the pull request with the caller's own
//!   GitHub credentials. A run's patch is accepted only from this repository's main and only when it
//!   touches the overrides contract, package manifests and lockfiles.
//!
//! Formerly `tools/npm/*.sh` (shell inventory, root ADR-0270).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const TOOL: &str = "tools/npm/security-overrides.mjs";
const CONTRACT: &str = "tools/npm/security-overrides.contract.json";
const USAGE: &str =
    "usage: cargo xtask npm <refresh-locks [tree ...] | prepare-advisory-fix <osv-report.json> \
<out-dir> | open-advisory-fix-pr [<run-id>]>";
/// `security-overrides.mjs propose` found no new npm advisory with a fixed release.
const NOTHING_TO_PROPOSE: i32 = 3;

pub(crate) fn run(root: &Path, args: &[String]) -> Result<(), String> {
    match (args.first().map(String::as_str), &args[args.len().min(1)..]) {
        (Some("refresh-locks"), trees) => refresh_locks(root, trees),
        (Some("prepare-advisory-fix"), [report, out]) => {
            prepare_advisory_fix(root, Path::new(report), Path::new(out))
        }
        (Some("open-advisory-fix-pr"), []) => open_advisory_fix_pr(root, None),
        (Some("open-advisory-fix-pr"), [run]) => open_advisory_fix_pr(root, Some(run)),
        _ => Err(USAGE.to_owned()),
    }
}

fn refresh_locks(root: &Path, trees: &[String]) -> Result<(), String> {
    let toolchain = stdout(node(root).args(["toolchain", "--root"]).arg(root))?;
    let (contract_image, pnpm) =
        toolchain_from(&toolchain).ok_or("the technology contract did not yield a toolchain")?;
    // The image the container runtime policy reads must be the image of the Node the trees pin, or a pin
    // bump left the image behind.
    let image = super::container_image(root, "NODE_VERIFY_IMAGE");
    if image != contract_image {
        return Err(format!(
            "NODE_VERIFY_IMAGE is {image} but the pinned Node needs {contract_image}"
        ));
    }
    let trees: Vec<String> = if trees.is_empty() {
        stdout(node(root).args(["trees", "--root"]).arg(root))?
            .lines()
            .map(|line| line.trim().to_owned())
            .filter(|line| !line.is_empty())
            .collect()
    } else {
        trees.to_vec()
    };
    if trees.is_empty() {
        return Err("no tracked pnpm-lock.yaml".to_owned());
    }
    let run_as = container_user()?;
    for tree in &trees {
        // One component at a time: Docker Desktop fails a bind source that mixes `\` and `/` (ENOMEM on scandir).
        let dir = tree
            .split('/')
            .fold(root.to_path_buf(), |path, part| path.join(part));
        if !dir.join("pnpm-lock.yaml").is_file() || !dir.join("package.json").is_file() {
            return Err(format!("{tree} has no package.json and pnpm-lock.yaml"));
        }
        eprintln!("xtask npm refresh-locks: {tree} (pnpm {pnpm} in {image})");
        let mut volume = dir.into_os_string();
        volume.push(":/work");
        succeed(
            Command::new("docker")
                .args(["run", "--rm", "--user", &run_as, "--volume"])
                .arg(volume)
                .args([
                    "--workdir",
                    "/work",
                    "--env",
                    "HOME=/tmp",
                    "--env",
                    "COREPACK_HOME=/tmp/corepack",
                ])
                .args([
                    "--env",
                    "COREPACK_ENABLE_DOWNLOAD_PROMPT=0",
                    "--env",
                    "CI=true",
                    &image,
                    "corepack",
                ])
                .arg(format!("pnpm@{pnpm}"))
                .args([
                    "install",
                    "--lockfile-only",
                    "--ignore-scripts",
                    "--config.confirmModulesPurge=false",
                ]),
            "docker run (pnpm install --lockfile-only)",
        )?;
    }
    succeed(
        node(root).args(["check", "--root"]).arg(root),
        "the overrides check",
    )
}

fn prepare_advisory_fix(root: &Path, report: &Path, out: &Path) -> Result<(), String> {
    fs::create_dir_all(out).map_err(|error| format!("cannot create {}: {error}", out.display()))?;
    let summary_path = out.join("summary.md");
    let patch_path = out.join("npm-advisory-fix.patch");
    let _ = fs::remove_file(&summary_path);
    let _ = fs::remove_file(&patch_path);
    let finish = |text: String| -> Result<(), String> {
        print!("{text}");
        write(&summary_path, &text)
    };
    if fs::metadata(report).map(|m| m.len() == 0).unwrap_or(true) {
        return finish(format!(
            "No OSV report at {}: the ratchet did not run, so there is nothing to prepare.\n",
            report.display()
        ));
    }
    if !tree_is_clean(root)? {
        return Err("the working tree has changes; prepare from a clean checkout".to_owned());
    }
    let proposal = out.join("proposal.txt");
    let status = node(root)
        .args(["propose", "--root"])
        .arg(root)
        .arg("--osv-report")
        .arg(report)
        .arg("--summary")
        .arg(&proposal)
        .status()
        .map_err(|error| format!("cannot run node: {error}"))?;
    let proposal_text = fs::read_to_string(&proposal).unwrap_or_default();
    match status.code() {
        Some(0) => {}
        Some(NOTHING_TO_PROPOSE) => {
            let mut text = "The OSV ratchet failed, but no new npm advisory has a fixed release an override could pin.\n\
                It is a Cargo finding, an advisory without a fix (accept it in tools/osv-vulnerability-baseline.tsv \
                with a reason), or a stale baseline row.\n"
                .to_owned();
            if !proposal_text.is_empty() {
                text.push('\n');
                text.push_str(&proposal_text);
            }
            return finish(text);
        }
        _ => return Err(format!("security-overrides.mjs propose failed: {status}")),
    }
    succeed(
        node(root).args(["render", "--root"]).arg(root),
        "security-overrides.mjs render",
    )?;
    refresh_locks(root, &[])?;
    let after_log =
        fs::File::create(out.join("osv-after.log")).map_err(|error| error.to_string())?;
    let closes = osv_gate(root, &out.join("osv-report-after.json"), Some(after_log))?;
    let ratchet = if closes {
        "closes the OSV ratchet"
    } else {
        "does NOT close the OSV ratchet on its own (see osv-after.log: a stale baseline row to delete, or a \
         finding no override reaches)"
    };
    let patch = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["diff", "--binary"])
        .output();
    let patch = patch.map_err(|error| format!("cannot run git diff: {error}"))?;
    fs::write(&patch_path, &patch.stdout).map_err(|error| error.to_string())?;
    let stat = stdout(
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["diff", "--stat"]),
    )?;
    finish(format!(
        "Raises npm advisory floors in tools/npm/security-overrides.contract.json (root ADR-0158),\n\
         re-renders every package.json and regenerates every pnpm-lock.yaml with the pinned toolchain.\n\n\
         {proposal_text}\n\nWith this change applied the repository {ratchet}.\n\n```\n{stat}```\n"
    ))
}

fn open_advisory_fix_pr(root: &Path, run: Option<&String>) -> Result<(), String> {
    let status = stdout(Command::new("git").arg("-C").arg(root).args([
        "status",
        "--porcelain=v1",
        "--untracked-files=no",
    ]))?;
    if !status.trim().is_empty() {
        return Err("the working tree has changes".to_owned());
    }
    let work = std::env::temp_dir().join(format!("xtask-npm-advisory-{}", std::process::id()));
    fs::create_dir_all(&work).map_err(|error| error.to_string())?;
    let result = open_advisory_fix_pr_in(root, run, &work);
    let _ = fs::remove_dir_all(&work);
    result
}

fn open_advisory_fix_pr_in(root: &Path, run: Option<&String>, work: &Path) -> Result<(), String> {
    let git = || {
        let mut command = Command::new("git");
        command.arg("-C").arg(root);
        command
    };
    succeed(
        git().args(["fetch", "--quiet", "origin", "main"]),
        "git fetch",
    )?;
    let login = stdout(Command::new("gh").args(["api", "user", "--jq", ".login"]))?;
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let branch = format!("{}/npm-advisory-fix-{seconds}", login.trim());
    succeed(
        git().args(["switch", "--quiet", "--create", &branch, "origin/main"]),
        "git switch",
    )?;
    let fix = work.join("npm-advisory-fix");
    let patch = fix.join("npm-advisory-fix.patch");
    if let Some(run) = run {
        let repository = stdout(Command::new("gh").args([
            "repo",
            "view",
            "--json",
            "nameWithOwner",
            "--jq",
            ".nameWithOwner",
        ]))?;
        let origin = stdout(
            Command::new("gh")
                .args([
                    "run",
                    "view",
                    run,
                    "--json",
                    "event,headBranch,headRepository",
                    "--jq",
                ])
                .arg(r#"[.event, .headBranch, (.headRepository.nameWithOwner // "")] | join(" ")"#),
        )?;
        // A pull request (a fork's included) chooses its artifact's contents: only main's own runs may supply it.
        if !run_origin_allowed(origin.trim(), repository.trim()) {
            return Err(format!(
                "run {run} is '{}'; only a schedule, push or dispatch run on main of {} may supply the patch",
                origin.trim(),
                repository.trim()
            ));
        }
        succeed(
            Command::new("gh")
                .args([
                    "run",
                    "download",
                    run,
                    "--name",
                    "repository-supply-chain",
                    "--dir",
                ])
                .arg(work),
            "gh run download",
        )?;
        if fs::metadata(&patch).map(|m| m.len() == 0).unwrap_or(true) {
            print!(
                "{}",
                fs::read_to_string(fix.join("summary.md")).unwrap_or_default()
            );
            return Err(format!("run {run} prepared no patch"));
        }
        let numstat = stdout(git().args(["apply", "--numstat"]).arg(&patch))?;
        let outside = paths_outside(&numstat);
        if !outside.is_empty() {
            return Err(format!(
                "the patch touches files outside the contract, manifests and lockfiles: {}",
                outside.join(", ")
            ));
        }
        succeed(git().args(["apply", "--index"]).arg(&patch), "git apply")?;
    } else {
        let report = work.join("osv-report.json");
        osv_gate(root, &report, None)?;
        prepare_advisory_fix(root, &report, &fix)?;
        if fs::metadata(&patch).map(|m| m.len() == 0).unwrap_or(true) {
            return Err("nothing an override can fix; see the summary above".to_owned());
        }
        succeed(git().args(["add", "--update"]), "git add")?;
    }
    succeed(node(root).arg("check"), "the overrides check")?;
    let staged = stdout(git().args(["diff", "--cached", "--unified=0", "--", CONTRACT]))?;
    let advisories = advisory_ids(&staged);
    let title = if advisories.is_empty() {
        "fix(deps): raise npm advisory floors (see contract)".to_owned()
    } else {
        format!(
            "fix(deps): raise npm advisory floors ({})",
            advisories.join(",")
        )
    };
    let summary = fix.join("summary.md");
    let body = fs::read_to_string(&summary)
        .map_err(|error| format!("cannot read {}: {error}", summary.display()))?;
    succeed(
        git().args(["commit", "--quiet", "--message", &title, "--message", &body]),
        "git commit",
    )?;
    succeed(
        git().args(["push", "--quiet", "--set-upstream", "origin", &branch]),
        "git push",
    )?;
    succeed(
        Command::new("gh")
            .current_dir(root)
            .args([
                "pr",
                "create",
                "--base",
                "main",
                "--head",
                &branch,
                "--title",
                &title,
                "--body-file",
            ])
            .arg(&summary),
        "gh pr create",
    )
}

/// Whether a run's `event headBranch headRepository` lets it supply a patch: main of this repository,
/// started by a schedule, a push or a dispatch.
fn run_origin_allowed(origin: &str, repository: &str) -> bool {
    ["schedule", "push", "workflow_dispatch"]
        .iter()
        .any(|event| origin == format!("{event} main {repository}"))
}

/// The paths of a `git apply --numstat` listing that are not the overrides contract, a package manifest or
/// a lockfile.
fn paths_outside(numstat: &str) -> Vec<String> {
    let allowed = |path: &str| {
        path == CONTRACT
            || ["package.json", "pnpm-lock.yaml"]
                .iter()
                .any(|name| path == *name || path.ends_with(&format!("/{name}")))
    };
    numstat
        .lines()
        .filter_map(|line| line.splitn(3, '\t').nth(2))
        .filter(|path| !allowed(path))
        .map(str::to_owned)
        .collect()
}

/// The GitHub advisory ids (`GHSA-xxxx-xxxx-xxxx`) on the added lines of a diff, sorted and unique.
fn advisory_ids(diff: &str) -> Vec<String> {
    const ALPHABET: &str = "23456789cfghjmpqrvwx";
    let mut found = Vec::new();
    for line in diff.lines().filter(|line| line.starts_with("+ ")) {
        let mut rest = line;
        while let Some(at) = rest.find("GHSA-") {
            let candidate = &rest[at..];
            let groups: Vec<&str> = candidate[5..].splitn(4, '-').take(3).collect();
            let valid = groups.len() == 3
                && groups.iter().enumerate().all(|(index, group)| {
                    let group = if index == 2 {
                        group.get(..4).unwrap_or("")
                    } else {
                        group
                    };
                    group.len() == 4 && group.chars().all(|c| ALPHABET.contains(c))
                });
            if valid {
                found.push(candidate[..19].to_owned());
            }
            rest = &candidate[5..];
        }
    }
    found.sort();
    found.dedup();
    found
}

/// `(NODE_IMAGE, PNPM_VERSION)` from `security-overrides.mjs toolchain`.
fn toolchain_from(stdout: &str) -> Option<(String, String)> {
    let value = |key: &str| {
        stdout
            .lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix(key)?
                    .strip_prefix('=')
                    .map(str::to_owned)
            })
            .filter(|value| !value.is_empty())
    };
    Some((value("NODE_IMAGE")?, value("PNPM_VERSION")?))
}

/// On Linux the container runs as the caller so the regenerated lock keeps its owner; Docker Desktop maps
/// bind-mount ownership itself.
fn container_user() -> Result<String, String> {
    if cfg!(windows) {
        return Ok("root".to_owned());
    }
    let id = |flag: &str| stdout(Command::new("id").arg(flag)).map(|text| text.trim().to_owned());
    Ok(format!("{}:{}", id("-u")?, id("-g")?))
}

/// Runs the repository OSV ratchet; whether it passed. Its output goes to `log` when given.
fn osv_gate(root: &Path, report: &Path, log: Option<fs::File>) -> Result<bool, String> {
    let mut command = Command::new("bash");
    command
        .arg(root.join("scripts/ci/osv-vulnerability-gate.sh"))
        .arg("--report-path")
        .arg(report)
        .arg(root);
    if let Some(log) = log {
        let err = log.try_clone().map_err(|error| error.to_string())?;
        command.stdout(log).stderr(err);
    }
    Ok(command
        .status()
        .map_err(|error| format!("cannot run the OSV gate: {error}"))?
        .success())
}

fn tree_is_clean(root: &Path) -> Result<bool, String> {
    let quiet = |cached: bool| {
        let mut command = Command::new("git");
        command.arg("-C").arg(root).args(["diff", "--quiet"]);
        if cached {
            command.arg("--cached");
        }
        command
            .status()
            .map(|status| status.success())
            .map_err(|error| error.to_string())
    };
    Ok(quiet(false)? && quiet(true)?)
}

fn node(root: &Path) -> Command {
    let mut command = Command::new("node");
    command.arg(root.join(TOOL));
    command
}

fn stdout(command: &mut Command) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("cannot run {command:?}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{command:?} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn succeed(command: &mut Command, what: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("cannot run {what}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{what} failed: {status}"))
    }
}

fn write(path: &PathBuf, text: &str) -> Result<(), String> {
    fs::write(path, text).map_err(|error| format!("cannot write {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPOSITORY: &str = "perfectory-inc/perfectory-public";

    #[test]
    fn only_a_schedule_push_or_dispatch_run_on_this_repositorys_main_supplies_a_patch() {
        for event in ["schedule", "push", "workflow_dispatch"] {
            assert!(
                run_origin_allowed(&format!("{event} main {REPOSITORY}"), REPOSITORY),
                "{event}"
            );
        }
        for origin in [
            format!("pull_request main {REPOSITORY}"),
            "pull_request feature someone/fork".to_owned(),
            format!("schedule feature {REPOSITORY}"),
            "push main someone/fork".to_owned(),
            format!("merge_group main {REPOSITORY}"),
            " ".to_owned(),
            String::new(),
        ] {
            assert!(
                !run_origin_allowed(&origin, REPOSITORY),
                "accepted '{origin}'"
            );
        }
    }

    #[test]
    fn a_patch_may_touch_only_the_contract_manifests_and_lockfiles() {
        let allowed =
            "1\t1\ttools/npm/security-overrides.contract.json\n1\t1\tproducts/app/package.json\n\
                       1\t1\tproducts/app/pnpm-lock.yaml\n1\t1\tpackage.json\n";
        assert_eq!(paths_outside(allowed), Vec::<String>::new());
        assert_eq!(
            paths_outside("1\t1\tproducts/app/package.json\n1\t1\t.github/workflows/ci.yml\n"),
            vec![".github/workflows/ci.yml"]
        );
        assert_eq!(
            paths_outside("1\t1\tproducts/app/pnpm-lock.yaml\n1\t1\tproducts/app/index.ts\n"),
            vec!["products/app/index.ts"]
        );
        // A name that only ends like a manifest is not one.
        assert_eq!(
            paths_outside("1\t1\tsrc/notpackage.json\n"),
            vec!["src/notpackage.json"]
        );
    }

    #[test]
    fn the_title_names_each_advisory_the_contract_gained_once() {
        let diff = "+    \"advisories\": [\"GHSA-2234-5678-9cfg\", \"GHSA-hjmp-qrvw-x234\"],\n\
                    -    \"advisories\": [\"GHSA-mmmm-pppp-qqqq\"],\n\
                    +    \"also\": \"GHSA-2234-5678-9cfg\"\n";
        // Added lines only (a removed advisory is not raised), each id once, sorted.
        assert_eq!(
            advisory_ids(diff),
            vec!["GHSA-2234-5678-9cfg", "GHSA-hjmp-qrvw-x234"]
        );
        assert_eq!(
            advisory_ids("+ GHSA-hjmp-qrvw-x234, GHSA-2234-5678-9cfg and GHSA-abcd-0000-1111\n"),
            vec!["GHSA-2234-5678-9cfg", "GHSA-hjmp-qrvw-x234"]
        );
    }

    #[test]
    fn the_toolchain_is_read_from_the_tools_key_value_lines() {
        assert_eq!(
            toolchain_from("NODE_IMAGE=node:24-bookworm@sha256:ab\r\nPNPM_VERSION=10.1.0\n"),
            Some(("node:24-bookworm@sha256:ab".to_owned(), "10.1.0".to_owned()))
        );
        assert_eq!(toolchain_from("NODE_IMAGE=node:24\n"), None);
        assert_eq!(toolchain_from("NODE_IMAGE=\nPNPM_VERSION=10\n"), None);
    }

    #[test]
    fn an_unknown_or_incomplete_command_prints_the_usage() {
        let run = |args: &[&str]| {
            run(
                Path::new("."),
                &args.iter().map(|a| (*a).to_owned()).collect::<Vec<_>>(),
            )
        };
        assert_eq!(run(&[]), Err(USAGE.to_owned()));
        assert_eq!(
            run(&["prepare-advisory-fix", "only-one"]),
            Err(USAGE.to_owned())
        );
        assert_eq!(
            run(&["open-advisory-fix-pr", "1", "2"]),
            Err(USAGE.to_owned())
        );
        assert_eq!(run(&["nonsense"]), Err(USAGE.to_owned()));
    }
}
