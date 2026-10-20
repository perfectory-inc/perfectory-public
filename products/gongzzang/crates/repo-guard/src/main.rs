mod guards;

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use guards::capability_layout::check_capability_layout;
use guards::file_line_limit::{check_file_line_limit, ERROR_LINES};
use guards::migration_version_prefixes::check_migration_version_prefixes;

fn main() {
    let exit_code = match run() {
        Ok(()) => 0,
        Err(error) => {
            let _ = writeln!(io::stderr(), "{error}");
            1
        }
    };
    std::process::exit(exit_code);
}

fn run() -> Result<(), String> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "repo-guard".to_string());
    let subcommand = args
        .next()
        .ok_or_else(|| format!("usage: {program} SUBCOMMAND"))?;
    if args.next().is_some() {
        return Err(format!("usage: {program} SUBCOMMAND"));
    }

    match subcommand.as_str() {
        "capability-layout" => {
            let repo_root = resolve_repo_root()?;
            let report = check_capability_layout(&repo_root)?;
            writeln!(
                io::stdout(),
                "capability-layout-ok business_crates={}",
                report.business_crates
            )
            .map_err(|error| format!("failed to write guard output: {error}"))?;
            Ok(())
        }
        "migration-version-prefixes" => {
            let repo_root = resolve_repo_root()?;
            let report = check_migration_version_prefixes(&repo_root)?;
            writeln!(
                io::stdout(),
                "migration-version-prefixes-ok files={}",
                report.files
            )
            .map_err(|error| format!("failed to write guard output: {error}"))?;
            Ok(())
        }
        "file-line-limit" => {
            // From the directory CI runs it in (products/gongzzang), as the script did.
            let root = env::current_dir()
                .map_err(|error| format!("failed to resolve current directory: {error}"))?;
            let report = check_file_line_limit(&root)?;
            let annotations = env::var("GITHUB_ACTIONS").is_ok_and(|value| value == "true");
            let mut stderr = io::stderr();
            for (file, lines) in &report.warnings {
                let line = if annotations {
                    format!("::warning file={file}::{lines} lines (> 500). Consider splitting.")
                } else {
                    format!(
                        "file-line-limit: {file} has {lines} lines (> 500). Consider splitting."
                    )
                };
                writeln!(stderr, "{line}")
                    .map_err(|error| format!("failed to write guard output: {error}"))?;
            }
            for (file, lines) in &report.errors {
                let line = if annotations {
                    format!("::error file={file}::{lines} lines (> {ERROR_LINES}). Split the file into focused parts.")
                } else {
                    format!("file-line-limit: {file} has {lines} lines (> {ERROR_LINES}). Split the file into focused parts.")
                };
                writeln!(stderr, "{line}")
                    .map_err(|error| format!("failed to write guard output: {error}"))?;
            }
            if !report.errors.is_empty() {
                return Err(format!(
                    "file-line-limit: {} file(s) over {ERROR_LINES} lines",
                    report.errors.len()
                ));
            }
            writeln!(
                io::stdout(),
                "file-line-limit-ok max={ERROR_LINES} files={}",
                report.files
            )
            .map_err(|error| format!("failed to write guard output: {error}"))?;
            Ok(())
        }
        _ => Err(format!("unknown subcommand: {subcommand}")),
    }
}

fn resolve_repo_root() -> Result<PathBuf, String> {
    let current_dir = env::current_dir()
        .map_err(|error| format!("failed to resolve current directory: {error}"))?;
    let mut candidate = Some(current_dir.as_path());
    let mut first_manifest_dir = None;

    while let Some(dir) = candidate {
        if dir.join(".git").exists() || has_workspace_manifest(dir) {
            return Ok(dir.to_path_buf());
        }
        if first_manifest_dir.is_none() && dir.join("Cargo.toml").is_file() {
            first_manifest_dir = Some(dir.to_path_buf());
        }
        candidate = dir.parent();
    }

    first_manifest_dir.ok_or_else(|| "repo root is missing: Cargo.toml or .git".to_string())
}

fn has_workspace_manifest(dir: &Path) -> bool {
    let manifest = dir.join("Cargo.toml");
    fs::read_to_string(manifest)
        .is_ok_and(|content| content.lines().any(|line| line == "[workspace]"))
}
