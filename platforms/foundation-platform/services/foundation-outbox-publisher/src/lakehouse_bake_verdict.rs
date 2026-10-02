//! The matching verdict a lakehouse bake of a Silver snapshot must carry (root ADR-0133 §5).
//!
//! A v2 served summary names the `silver.parcel_boundaries` snapshot its Gold was built from. The
//! bake binds its new revision to that snapshot only with the ADR-0113 §7 verdict
//! `parcel_matching_gate.py` wrote for the same snapshot, and only when it passed and checked every
//! parcel of it. This reads that verdict before the build is started; the build-row guard in the
//! database refuses the same things, so a caller that skips this still cannot start the build.

use anyhow::{bail, ensure, Context as _};
use catalog_application::ports::LakehouseBakeSilverSource;
use sha2::{Digest as _, Sha256};

/// Path of the verdict JSON for the snapshot a v2 summary names.
pub(crate) const MATCHING_VERDICT_ENV: &str =
    "FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE_MATCHING_VERDICT";
/// The verdict schema `parcel_matching_gate.py` writes and the build-row guard reads.
pub(crate) const MATCHING_VERDICT_SCHEMA: &str = "foundation-platform.parcel_matching_verdict.v1";

/// The Silver source to bind for a summary naming `source_snapshot_id`, or `None` for a v1 summary.
pub(crate) fn silver_source(
    source_snapshot_id: Option<&str>,
) -> anyhow::Result<Option<LakehouseBakeSilverSource>> {
    let Some(snapshot) = source_snapshot_id else {
        return Ok(None);
    };
    let path = std::env::var_os(MATCHING_VERDICT_ENV).with_context(|| {
        format!("the served summary names Silver snapshot {snapshot}; {MATCHING_VERDICT_ENV} must name its matching verdict")
    })?;
    let bytes = std::fs::read(&path)
        .with_context(|| format!("matching verdict {}", path.to_string_lossy()))?;
    Ok(Some(verdict_source(snapshot, &bytes)?))
}

/// Checks `bytes` is a passing verdict for exactly `snapshot` that covered every parcel of it.
pub(crate) fn verdict_source(
    snapshot: &str,
    bytes: &[u8],
) -> anyhow::Result<LakehouseBakeSilverSource> {
    let verdict: serde_json::Value =
        serde_json::from_slice(bytes).context("the matching verdict is not JSON")?;
    ensure!(
        verdict["schema_version"] == MATCHING_VERDICT_SCHEMA,
        "the matching verdict is not {MATCHING_VERDICT_SCHEMA}"
    );
    ensure!(
        verdict["snapshot_id"] == snapshot,
        "the matching verdict is for snapshot {}, the served Gold for {snapshot}",
        verdict["snapshot_id"]
    );
    if verdict["passed"] != true {
        bail!("the matching gate refused Silver snapshot {snapshot}; the bake does not start");
    }
    let total = verdict["snapshot_parcel_count"].as_u64().unwrap_or(0);
    ensure!(
        total > 0 && verdict["parcels"]["checked"].as_u64() == Some(total),
        "the matching verdict for {snapshot} did not check every parcel of the snapshot"
    );
    Ok(LakehouseBakeSilverSource {
        source_snapshot_id: snapshot.to_owned(),
        matching_verdict: verdict,
        matching_verdict_sha256: format!("{:x}", Sha256::digest(bytes)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verdict(snapshot: &str, passed: bool, checked: u64, total: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": MATCHING_VERDICT_SCHEMA,
            "snapshot_id": snapshot,
            "passed": passed,
            "snapshot_parcel_count": total,
            "parcels": {"passed": passed, "checked": checked},
        }))
        .unwrap_or_default()
    }

    #[test]
    fn a_passing_verdict_for_the_snapshot_is_bound_with_its_digest() -> anyhow::Result<()> {
        let bytes = verdict("synthetic-2", true, 3, 3);
        let source = verdict_source("synthetic-2", &bytes)?;
        assert_eq!(source.source_snapshot_id, "synthetic-2");
        assert_eq!(
            source.matching_verdict_sha256,
            format!("{:x}", Sha256::digest(&bytes))
        );
        Ok(())
    }

    #[test]
    fn a_refusal_another_snapshot_or_partial_coverage_does_not_start_the_bake() {
        for (bytes, reason) in [
            (verdict("synthetic-2", false, 3, 3), "refused"),
            (verdict("synthetic-1", true, 3, 3), "another snapshot"),
            (verdict("synthetic-2", true, 2, 3), "one sido of three"),
            (b"{}".to_vec(), "no schema"),
        ] {
            assert!(verdict_source("synthetic-2", &bytes).is_err(), "{reason}");
        }
    }

    #[test]
    fn no_bound_snapshot_needs_no_verdict() -> anyhow::Result<()> {
        // Which summaries bind a snapshot is decided by ServedSummary::bound_silver_snapshot and
        // tested with a real admin-shaped v1 summary in lakehouse_tile_bake_tests.rs.
        assert!(silver_source(None)?.is_none());
        Ok(())
    }
}
