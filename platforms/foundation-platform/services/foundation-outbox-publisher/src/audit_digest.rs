//! The hourly digest of the audit chain and its public witness (root ADR-0569 §4).
//!
//! The unit runs three steps; the middle one is root's, because the witness key is root's:
//!
//! The steps share the unit's `RUNTIME_DIRECTORY` (`<dir>` below), which systemd makes for the run.
//!
//! 1. `prepare-audit-digest` (this binary, as the service user): asks the database for the
//!    next digest (`audit.next_digest`, which refuses a range that does not chain) and writes its
//!    bytes to `<dir>/digest.txt` and the range to `<dir>/digest.json`. With no new row it writes
//!    neither. On the weekly check it also writes every kept digest and its Rekor bundle under
//!    `<dir>/verify/`, after `audit.verify_all` found the chain and the digests whole.
//! 2. `audit-witness.sh <dir>` (root, from the control checkout): signs `digest.txt` with the
//!    witness key using the pinned cosign, which puts the entry in the public Rekor log and answers
//!    the bundle, `<dir>/bundle.json`; on the weekly check it verifies every bundle under `verify/`
//!    against Rekor.
//! 3. `record-audit-digest` (this binary): keeps the digest with its Rekor entry
//!    (`audit.record_digest`, which computes the digest again).
//!
//! Only the digest's hash leaves the host: no name, no row, no object id.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use crate::audit_import::importer_connection;

/// The range a prepared digest covers and its hash, for the record step.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub from_seq: i64,
    pub to_seq: i64,
    pub digest_sha256: String,
}

fn run_directory() -> anyhow::Result<PathBuf> {
    match std::env::var("RUNTIME_DIRECTORY") {
        // systemd may name several, separated by colons; the unit names one.
        Ok(dirs) if !dirs.trim().is_empty() => Ok(PathBuf::from(dirs.split(':').next().unwrap_or(&dirs))),
        _ => bail!("RUNTIME_DIRECTORY is not set: the digest steps run inside foundation-audit-digest.service"),
    }
}

/// The Rekor log index a cosign bundle names: `verificationMaterial.tlogEntries[0].logIndex`, a
/// string in the protobuf JSON of a v0.3 bundle and a number in older ones.
pub(crate) fn bundle_log_index(bundle: &Value) -> Option<i64> {
    let entry = bundle
        .pointer("/verificationMaterial/tlogEntries/0")
        .or_else(|| bundle.pointer("/rekorBundle/Payload"))?;
    let index = entry.get("logIndex")?;
    index
        .as_i64()
        .or_else(|| index.as_str().and_then(|text| text.parse().ok()))
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
            bail!("the audit chain does not verify: {}", problems.join("; "));
        }
        let verify = dir.join("verify");
        std::fs::create_dir_all(&verify)?;
        let rows = sqlx::query(
            "SELECT to_seq, audit.digest_text(from_seq, to_seq, count, head_hash, previous_digest_sha256) AS text,
                    rekor_inclusion_proof AS bundle
             FROM audit.digest ORDER BY to_seq",
        )
        .fetch_all(&mut conn)
        .await?;
        for row in &rows {
            let to_seq: i64 = row.try_get("to_seq")?;
            let text: String = row.try_get("text")?;
            let bundle: Option<Value> = row.try_get("bundle")?;
            let Some(bundle) = bundle else {
                bail!("digest ..{to_seq} has no Rekor bundle");
            };
            std::fs::write(verify.join(format!("{to_seq}.txt")), text)?;
            std::fs::write(
                verify.join(format!("{to_seq}.bundle.json")),
                bundle.to_string(),
            )?;
        }
        println!("audit-digest verified rows and {} digests", rows.len());
    }
    let next = sqlx::query(
        "SELECT from_seq, to_seq, digest_text, encode(digest_sha256, 'hex') AS digest_sha256
         FROM audit.next_digest()",
    )
    .fetch_optional(&mut conn)
    .await
    .context("the next audit digest")?;
    let Some(next) = next else {
        println!("audit-digest unchanged no new row");
        return Ok(());
    };
    let prepared = Prepared {
        from_seq: next.try_get("from_seq")?,
        to_seq: next.try_get("to_seq")?,
        digest_sha256: next.try_get("digest_sha256")?,
    };
    let text: String = next.try_get("digest_text")?;
    std::fs::write(dir.join("digest.txt"), text)?;
    std::fs::write(dir.join("digest.json"), serde_json::to_string(&prepared)?)?;
    println!(
        "audit-digest prepared seq {}..{} sha256 {}",
        prepared.from_seq, prepared.to_seq, prepared.digest_sha256
    );
    Ok(())
}

pub async fn record() -> anyhow::Result<()> {
    let dir = run_directory()?;
    let meta = dir.join("digest.json");
    if !meta.exists() {
        println!("audit-digest unchanged no new row");
        return Ok(());
    }
    let prepared: Prepared = serde_json::from_str(&std::fs::read_to_string(&meta)?)?;
    let bundle = read_bundle(&dir.join("bundle.json"))?;
    let Some(log_index) = bundle_log_index(&bundle) else {
        bail!("the witness answered a bundle without a Rekor log index");
    };
    let mut conn = importer_connection().await?;
    sqlx::query("SELECT audit.record_digest($1, decode($2, 'hex'), $3, $4)")
        .bind(prepared.to_seq)
        .bind(&prepared.digest_sha256)
        .bind(log_index)
        .bind(&bundle)
        .execute(&mut conn)
        .await
        .context("keeping the witnessed digest")?;
    println!(
        "audit-digest changed seq {}..{} rekor {}",
        prepared.from_seq, prepared.to_seq, log_index
    );
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
    use super::bundle_log_index;
    use serde_json::json;

    #[test]
    fn the_log_index_is_read_from_either_bundle_format() {
        let v3 = json!({"mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
                        "verificationMaterial": {"tlogEntries": [{"logIndex": "123456789"}]}});
        assert_eq!(bundle_log_index(&v3), Some(123_456_789));
        let legacy = json!({"rekorBundle": {"Payload": {"logIndex": 42}}});
        assert_eq!(bundle_log_index(&legacy), Some(42));
        assert_eq!(bundle_log_index(&json!({"verificationMaterial": {}})), None);
    }
}
