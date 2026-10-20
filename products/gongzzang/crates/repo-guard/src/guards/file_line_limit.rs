//! Source and document size limit (formerly `scripts/lefthook/file-line-limit.sh`).
//!
//! Every `.md`, `.rs`, `.ts`, `.tsx` and `.sql` file git tracks or would track (untracked but not
//! ignored) under the root: more than [`ERROR_LINES`] lines fails, more than [`WARN_LINES`] warns.
//! A generated file (`*.generated.ts`) is regenerated, never split by hand; its size belongs to its
//! source, so it is not counted.

use std::fs;
use std::path::Path;
use std::process::Command;

/// More lines than this fail the guard.
pub const ERROR_LINES: usize = 1500;
/// More lines than this are reported as a warning.
pub const WARN_LINES: usize = 500;

/// What the scan found: the files counted, and those over each limit with their line counts.
pub struct FileLineLimitReport {
    pub files: usize,
    pub warnings: Vec<(String, usize)>,
    pub errors: Vec<(String, usize)>,
}

/// Scans the files git lists under `root` (tracked, plus untracked files git does not ignore).
pub fn check_file_line_limit(root: &Path) -> Result<FileLineLimitReport, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
        ])
        .args([
            "*.md",
            "*.rs",
            "*.ts",
            "*.tsx",
            "*.sql",
            ":(exclude,glob)**/*.generated.ts",
        ])
        .output()
        .map_err(|error| format!("failed to run git ls-files: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git ls-files failed under {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let mut report = FileLineLimitReport {
        files: 0,
        warnings: Vec::new(),
        errors: Vec::new(),
    };
    for path in String::from_utf8_lossy(&output.stdout)
        .split('\0')
        .filter(|path| !path.is_empty())
    {
        let full = root.join(path);
        // A path git lists may be gone from the working tree (a deletion not yet staged).
        let Ok(bytes) = fs::read(&full) else {
            continue;
        };
        report.files += 1;
        let lines = line_count(&bytes);
        if lines > ERROR_LINES {
            report.errors.push((path.to_owned(), lines));
        } else if lines > WARN_LINES {
            report.warnings.push((path.to_owned(), lines));
        }
    }
    Ok(report)
}

/// Lines as `wc -l` counts them: newline characters.
pub fn line_count(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == b'\n').count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn scratch_repo(name: &str) -> Result<Scratch, String> {
        let dir = std::env::temp_dir().join(format!(
            "repo-guard-line-limit-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
        let status = Command::new("git")
            .arg("-C")
            .arg(&dir)
            .args(["init", "-q"])
            .status();
        if !status.is_ok_and(|status| status.success()) {
            return Err("git init failed".to_owned());
        }
        Ok(Scratch(dir))
    }

    fn write_lines(root: &Path, path: &str, lines: usize) -> Result<(), String> {
        let full = root.join(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        fs::write(&full, "x\n".repeat(lines)).map_err(|error| error.to_string())
    }

    fn named(found: &[(String, usize)]) -> Vec<&str> {
        found.iter().map(|(path, _)| path.as_str()).collect()
    }

    #[test]
    fn lines_are_newlines_as_wc_counts_them() {
        assert_eq!(line_count(b""), 0);
        assert_eq!(line_count(b"a\nb\n"), 2);
        assert_eq!(line_count(b"a\nb"), 1);
    }

    #[test]
    fn the_hard_limit_fails_above_and_allows_at_it() -> Result<(), String> {
        let repo = scratch_repo("limit")?;
        write_lines(&repo.0, "docs/archive/plans/oversized.md", ERROR_LINES + 1)?;
        write_lines(&repo.0, "docs/archive/plans/at-limit.md", ERROR_LINES)?;
        write_lines(&repo.0, "apps/web/lib/long.ts", WARN_LINES + 1)?;
        let report = check_file_line_limit(&repo.0)?;
        assert_eq!(
            named(&report.errors),
            vec!["docs/archive/plans/oversized.md"]
        );
        assert_eq!(report.errors[0].1, ERROR_LINES + 1);
        // Exactly at the hard limit is still over the soft one: a warning, not an error.
        assert_eq!(
            named(&report.warnings),
            vec!["apps/web/lib/long.ts", "docs/archive/plans/at-limit.md"]
        );
        assert_eq!(
            report.files, 3,
            "untracked files git does not ignore are counted"
        );
        Ok(())
    }

    #[test]
    fn ignored_generated_and_other_files_are_not_counted() -> Result<(), String> {
        let repo = scratch_repo("skips")?;
        fs::write(repo.0.join(".gitignore"), ".wrangler/\n.venv/\n")
            .map_err(|error| error.to_string())?;
        write_lines(&repo.0, ".wrangler/state/generated.ts", ERROR_LINES + 1)?;
        write_lines(
            &repo.0,
            "services/scraper-py/.venv/vendor.md",
            ERROR_LINES + 1,
        )?;
        write_lines(
            &repo.0,
            "apps/web/lib/api/contract.generated.ts",
            ERROR_LINES + 1,
        )?;
        write_lines(&repo.0, "apps/web/lib/api/contract.ts", 20)?;
        write_lines(&repo.0, "data/big.json", ERROR_LINES + 1)?;
        let report = check_file_line_limit(&repo.0)?;
        assert!(report.errors.is_empty(), "{:?}", named(&report.errors));
        assert_eq!(report.files, 1);
        Ok(())
    }

    #[test]
    fn a_root_that_is_not_a_repository_is_refused() -> Result<(), String> {
        let dir = std::env::temp_dir().join(format!(
            "repo-guard-line-limit-missing-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        assert!(check_file_line_limit(&dir).is_err());
        Ok(())
    }
}
