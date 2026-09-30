//! Loads the parcel-lineage review queue the lakehouse handed off into the steward database
//! (root ADR-0115 §9).
//!
//! The rules live in the stewardship crates: `parse_handoff` refuses a partial or inconsistent
//! file, and `replace_review_items` swaps the unit's items in one transaction and refuses to go
//! back in time. This command only reads the file and reports what changed.

use anyhow::{bail, Context};
use stewardship_domain::handoff::parse_handoff;
use stewardship_infrastructure::PgLineageStewardshipStore;

use crate::public_data_control_support::{optional_bool_env, required_env_value};

const INPUT_ENV: &str = "FOUNDATION_PLATFORM_LINEAGE_REVIEW_HANDOFF_INPUT";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_LINEAGE_REVIEW_LOAD_CONFIRM";

pub(crate) async fn run() -> anyhow::Result<()> {
    if !optional_bool_env(CONFIRM_ENV)?.unwrap_or(false) {
        bail!("{CONFIRM_ENV}=true is required: this command replaces catalog.lineage_review_item");
    }
    let path = required_env_value(INPUT_ENV)?;
    let text = std::fs::read_to_string(&path).with_context(|| format!("cannot read {path}"))?;
    let handoff =
        parse_handoff(&text).with_context(|| format!("{path} is not a whole review queue"))?;
    let pool = sqlx::PgPool::connect(&required_env_value("DATABASE_URL")?)
        .await
        .context("cannot connect to the Foundation database")?;
    let verdict = PgLineageStewardshipStore::new(pool)
        .replace_review_items(&handoff)
        .await
        .context("loading the review queue failed")?;
    println!(
        "lineage-review-load-json {}",
        serde_json::json!({
            "unit": handoff.unit,
            "published_at_utc": handoff.published_at_utc,
            "previous": verdict.previous,
            "loaded": verdict.loaded,
            "added": verdict.added,
            "removed": verdict.removed,
        })
    );
    Ok(())
}
