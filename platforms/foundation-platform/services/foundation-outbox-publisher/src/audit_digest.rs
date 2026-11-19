//! The hourly digest of the audit record and its public witness (root ADR-0569 §4, ADR-0584 §2).
//!
//! The unit runs three steps; the middle one is root's, because the witness key is root's. They
//! share the unit's `RUNTIME_DIRECTORY` (`<dir>` below), which systemd makes for the run.
//!
//! 1. `prepare-audit-digest` (this binary, as the service user): keeps the next digest
//!    (`audit.make_digest`; its trigger checks it against the rows), then writes the bytes of every
//!    digest not yet witnessed, oldest first, to `<dir>/pending/<digest_no>.txt` — normally the one
//!    just made, or also one whose upload failed an hour before. On the weekly check it first runs
//!    `audit.verify_all` and writes every witnessed digest and its bundle under `<dir>/verify/`.
//! 2. `audit-witness.sh <dir>` (root, from the control checkout): signs each pending digest with the
//!    witness key using the pinned cosign, which puts the entry in the public Rekor log and answers
//!    the bundle, `<dir>/pending/<digest_no>.bundle.json`; on the weekly check it verifies every
//!    bundle under `verify/` against Rekor.
//! 3. `record-audit-digest` (this binary): keeps each bundle as its digest's witness
//!    (`audit.witness_digest`, which refuses a bundle for another hash).
//!
//! The digest is kept before it is uploaded, so a failed upload is retried with the same digest; one
//! range never has two. Only the digest's hash leaves the host: no name, no row, no object id.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde_json::Value;
use sqlx::Row;

use crate::audit_import::importer_connection;

const DIGEST_TEXT: &str = "audit.digest_text(digest_no, from_snapshot, to_snapshot, row_count,
                                             rows_sha256, previous_digest_sha256)";

fn run_directory() -> anyhow::Result<PathBuf> {
    match std::env::var("RUNTIME_DIRECTORY") {
        // systemd may name several, separated by colons; the unit names one.
        Ok(dirs) if !dirs.trim().is_empty() => Ok(PathBuf::from(dirs.split(':').next().unwrap_or(&dirs))),
        _ => bail!("RUNTIME_DIRECTORY is not set: the digest steps run inside foundation-audit-digest.service"),
    }
}

/// The digest number a pending file names: `<digest_no>.txt` or `<digest_no>.bundle.json`.
pub(crate) fn digest_no_of(path: &Path, suffix: &str) -> Option<i64> {
    path.file_name()?
        .to_str()?
        .strip_suffix(suffix)?
        .parse()
        .ok()
        .filter(|no: &i64| *no >= 1)
}

pub async fn prepare() -> anyhow::Result<()> {
    let dir = run_directory()?;
    let weekly = std::env::var("FOUNDATION_AUDIT_VERIFY_ALL").is_ok_and(|value| value == "1");
    let mut conn = importer_connection().await?;
    if weekly {
        let problems: Vec<String> = sqlx::query_scalar("SELECT * FROM audit.verify_all()")
            .fetch_all(&mut conn)
            .await?;
        if !problems.is_empty() {
            bail!("the audit record does not verify: {}", problems.join("; "));
        }
        let verify = dir.join("verify");
        std::fs::create_dir_all(&verify)?;
        let rows = sqlx::query(&format!(
            "SELECT d.digest_no, {DIGEST_TEXT} AS text, w.rekor_inclusion_proof AS bundle
             FROM audit.digest d JOIN audit.digest_witness w USING (digest_no) ORDER BY d.digest_no"
        ))
        .fetch_all(&mut conn)
        .await?;
        for row in &rows {
            let no: i64 = row.try_get("digest_no")?;
            let text: String = row.try_get("text")?;
            let bundle: Value = row.try_get("bundle")?;
            std::fs::write(verify.join(format!("{no}.txt")), text)?;
            std::fs::write(verify.join(format!("{no}.bundle.json")), bundle.to_string())?;
        }
        println!(
            "audit-digest verified {} digests against their rows",
            rows.len()
        );
    }
    let made: Option<(i64, i64)> =
        sqlx::query_as("SELECT digest_no, row_count FROM audit.make_digest()")
            .fetch_optional(&mut conn)
            .await
            .context("making the next audit digest")?;
    match made {
        Some((no, rows)) => println!("audit-digest made digest {no} over {rows} rows"),
        None => println!("audit-digest no new row since the last digest"),
    }
    let pending = sqlx::query(&format!(
        "SELECT digest_no, {DIGEST_TEXT} AS text FROM audit.digest
         WHERE NOT EXISTS (SELECT FROM audit.digest_witness w WHERE w.digest_no = digest.digest_no)
         ORDER BY digest_no"
    ))
    .fetch_all(&mut conn)
    .await?;
    if !pending.is_empty() {
        let dir = dir.join("pending");
        std::fs::create_dir_all(&dir)?;
        for row in &pending {
            let no: i64 = row.try_get("digest_no")?;
            let text: String = row.try_get("text")?;
            std::fs::write(dir.join(format!("{no}.txt")), text)?;
        }
    }
    println!("audit-digest {} digests to witness", pending.len());
    Ok(())
}

pub async fn record() -> anyhow::Result<()> {
    let dir = run_directory()?.join("pending");
    let mut bundles = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if let Some(no) = digest_no_of(&path, ".bundle.json") {
                bundles.push((no, path));
            }
        }
    }
    if bundles.is_empty() {
        println!("audit-digest unchanged nothing to witness");
        return Ok(());
    }
    bundles.sort_unstable();
    let mut conn = importer_connection().await?;
    let mut witnessed = Vec::new();
    for (no, path) in &bundles {
        let bundle = read_bundle(path)?;
        let index: i64 = sqlx::query_scalar("SELECT audit.witness_digest($1, $2)")
            .bind(no)
            .bind(&bundle)
            .fetch_one(&mut conn)
            .await
            .with_context(|| format!("keeping the witness of digest {no}"))?;
        witnessed.push(format!("{no}@{index}"));
    }
    println!("audit-digest changed witnessed {}", witnessed.join(" "));
    Ok(())
}

fn read_bundle(path: &Path) -> anyhow::Result<Value> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("the witness wrote no bundle at {}", path.display()))?;
    let bundle: Value = serde_json::from_str(&text).context("the witness bundle is not JSON")?;
    if !bundle.is_object() {
        bail!("the witness bundle is not an object");
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::digest_no_of;
    use std::path::Path;

    #[test]
    fn a_pending_file_names_its_digest() {
        assert_eq!(
            digest_no_of(Path::new("/run/x/pending/12.txt"), ".txt"),
            Some(12)
        );
        assert_eq!(
            digest_no_of(Path::new("/run/x/pending/12.bundle.json"), ".bundle.json"),
            Some(12)
        );
        assert_eq!(
            digest_no_of(Path::new("/run/x/pending/12.bundle.json"), ".txt"),
            None
        );
        assert_eq!(
            digest_no_of(Path::new("/run/x/pending/0.txt"), ".txt"),
            None
        );
        assert_eq!(
            digest_no_of(Path::new("/run/x/pending/x.txt"), ".txt"),
            None
        );
    }
}
