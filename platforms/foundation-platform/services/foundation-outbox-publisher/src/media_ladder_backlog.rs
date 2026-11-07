//! `media-ladder-backlog`: the display ladders a photo's own collector run did not make (root
//! ADR-0427 §7, ADR-0429).
//!
//! A collector makes each photo's ladder in the run that stores it, within that run's budget. What it
//! defers or fails is not lost: this job reads the Silver photo indexes the shared media store's
//! contract names (`data-go-kr-onbid.contract.json` `media.ladder_backlog.index_tables`) and makes
//! the ladder of every source it has not settled yet, within its own budget, through the same
//! `media_renditions::ensure_ladders`.
//!
//! Partial (root ADR-0314): the state file keeps, per index table, the Iceberg snapshot it read and
//! the newest `source_snapshot_id` it took, and the sources still pending. A table whose snapshot did
//! not move is not read; otherwise only its rows from the newest run taken on are added (that run's
//! rows again, for a load that committed late; a source settled before costs one HEAD). A new ladder
//! version starts every table over (root ADR-0427 §3: a new version is made for every source once).
//! The state is written after each run, so a run that hits its time limit loses only its own work.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::bronze_object_storage::live_write_bronze_object_storage_from_env;
use crate::media_renditions::{self, RenditionBudget};
use crate::public_data_control_support::{required_env_value, write_json_file};

const PREFIX: &str = "FOUNDATION_PLATFORM_MEDIA_LADDER_BACKLOG";
const STATE_FILE: &str = "backlog.json";

/// `media` of the store's writer contract, as far as this job reads it.
#[derive(Debug, Deserialize)]
struct Media {
    object_prefix: String,
    ladder_backlog: Backlog,
}

#[derive(Debug, Deserialize)]
struct Backlog {
    index_tables: Vec<String>,
    #[serde(flatten)]
    budget: RenditionBudget,
}

/// What the job remembers between runs.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct State {
    /// The ladder version the marks below are for.
    pub(crate) version: String,
    /// Per index table: the Iceberg snapshot read, and the newest `source_snapshot_id` taken.
    pub(crate) tables: BTreeMap<String, TableMark>,
    /// Sources not settled yet, oldest first: `(sha256, media_format)`.
    pub(crate) pending: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TableMark {
    pub(crate) snapshot_id: i64,
    pub(crate) newest_run: Option<String>,
}

impl State {
    /// The state for `version`: kept when it is the same version, started over otherwise.
    pub(crate) fn for_version(self, version: &str) -> Self {
        if self.version == version {
            self
        } else {
            Self {
                version: version.to_owned(),
                ..Self::default()
            }
        }
    }

    /// Adds one table's rows (`sha256`, `media_format`, `source_snapshot_id`) from the newest run
    /// taken on, each source once, and moves the table's marks.
    pub(crate) fn take<'a>(
        &mut self,
        table: &str,
        snapshot_id: i64,
        rows: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
    ) -> usize {
        let since = self
            .tables
            .get(table)
            .and_then(|mark| mark.newest_run.clone());
        let mut queued: HashSet<String> = self.pending.iter().map(|(sha, _)| sha.clone()).collect();
        let mut newest = since.clone();
        let mut added = 0;
        for row in rows {
            let (Some(sha), Some(format), Some(run)) = (
                row.get("sha256").and_then(JsonValue::as_str),
                row.get("media_format").and_then(JsonValue::as_str),
                row.get("source_snapshot_id").and_then(JsonValue::as_str),
            ) else {
                continue;
            };
            if since.as_deref().is_some_and(|since| run < since) {
                continue;
            }
            if newest.as_deref().is_none_or(|newest| run > newest) {
                newest = Some(run.to_owned());
            }
            if queued.insert(sha.to_owned()) {
                self.pending.push((sha.to_owned(), format.to_owned()));
                added += 1;
            }
        }
        self.tables.insert(
            table.to_owned(),
            TableMark {
                snapshot_id,
                newest_run: newest,
            },
        );
        added
    }
}

fn media() -> anyhow::Result<Media> {
    let contract: JsonValue = serde_json::from_str(crate::data_go_kr_onbid_collect::CONTRACT_JSON)
        .context("data-go-kr-onbid.contract.json is not valid")?;
    serde_json::from_value(contract["media"].clone())
        .context("data-go-kr-onbid.contract.json media.ladder_backlog does not parse")
}

fn load_state(path: &Path) -> anyhow::Result<State> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is not a backlog state", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Runs one backlog pass.
///
/// # Errors
/// The contract does not parse, an index cannot be read, or the state cannot be written. A ladder
/// the store refuses stays pending; it never fails the run.
pub async fn run() -> anyhow::Result<()> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_COURT_AUCTION_ITEM_PHOTO;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    let media = media()?;
    let spec = media_renditions::spec()?;
    let state_dir = PathBuf::from(required_env_value(&format!("{PREFIX}_STATE_DIR"))?);
    std::fs::create_dir_all(&state_dir)?;
    let state_path = state_dir.join(STATE_FILE);
    let mut state = load_state(&state_path)?.for_version(&spec.version);

    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let lakehouse = LakehouseObjectReader::from_env()?;
    let mut read = serde_json::Map::new();
    for table in &media.ladder_backlog.index_tables {
        // One reader per photo index: a table the job cannot decode is refused, never guessed.
        let contract = match table.as_str() {
            name if name == SILVER_COURT_AUCTION_ITEM_PHOTO.table_name => {
                &SILVER_COURT_AUCTION_ITEM_PHOTO
            }
            other => anyhow::bail!("media.ladder_backlog names {other}, which has no reader"),
        };
        let Some(snapshot) = catalog
            .load_current_snapshot_manifest_list(table)
            .await
            .with_context(|| format!("failed to resolve {table}"))?
        else {
            read.insert(table.clone(), json!({"skipped": "no snapshot yet"}));
            continue;
        };
        if state
            .tables
            .get(table)
            .is_some_and(|mark| mark.snapshot_id == snapshot.snapshot_id)
        {
            read.insert(
                table.clone(),
                json!({"unchanged_snapshot": snapshot.snapshot_id}),
            );
            continue;
        }
        let scanned = scan_snapshot_rows(contract, &lakehouse, &snapshot).await?;
        let added = state.take(table, snapshot.snapshot_id, scanned.rows.iter());
        read.insert(
            table.clone(),
            json!({"snapshot": snapshot.snapshot_id, "rows": scanned.rows.len(), "added": added}),
        );
    }

    let pending = state.pending.len();
    let (ladders, left) = if state.pending.is_empty() {
        (media_renditions::LadderReport::default(), Vec::new())
    } else {
        let storage = live_write_bronze_object_storage_from_env().await?;
        media_renditions::ensure_ladders(
            media.object_prefix.trim_end_matches('/'),
            &state.pending,
            &media.ladder_backlog.budget,
            storage.as_ref(),
        )
        .await?
    };
    state.pending = left;
    write_json_file(&state_path, &serde_json::to_value(&state)?)?;
    let summary = json!({
        "version": state.version,
        "indexes": read,
        "pending_before": pending,
        "pending_after": state.pending.len(),
        "ladders": ladders,
    });
    println!("media-ladder-backlog-summary-json {summary}");
    println!(
        "foundation-job-outcome {}",
        if ladders.written > 0 {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn row(sha: char, run: &str) -> serde_json::Map<String, JsonValue> {
        json!({"sha256": sha.to_string().repeat(64), "media_format": "jpg", "source_snapshot_id": run})
            .as_object()
            .cloned()
            .unwrap()
    }

    #[test]
    fn the_contract_names_the_court_photo_index_and_a_budget_inside_the_unit() {
        let media = media().unwrap();
        assert_eq!(media.object_prefix, "onbid/media");
        assert_eq!(
            media.ladder_backlog.index_tables,
            vec!["silver.court_auction_item_photo".to_owned()]
        );
        // The job's time limit is 40 minutes; the ladder budget leaves room for the index read.
        assert!(media.ladder_backlog.budget.run_time_budget_seconds <= 30 * 60);
    }

    #[test]
    fn a_table_is_taken_from_its_newest_run_on_and_each_source_once() {
        let mut state = State::default().for_version("r1");
        let first = [
            row('a', "20991101T160500Z"),
            row('b', "20991102T160500Z"),
            row('a', "20991102T160500Z"),
        ];
        assert_eq!(state.take("t", 1, first.iter()), 2);
        assert_eq!(
            state.tables["t"].newest_run.as_deref(),
            Some("20991102T160500Z")
        );
        // The ladders were made; the next snapshot holds one old run, the newest again and a new one.
        state.pending.clear();
        let second = [
            row('a', "20991101T160500Z"),
            row('b', "20991102T160500Z"),
            row('c', "20991103T160500Z"),
        ];
        assert_eq!(
            state.take("t", 2, second.iter()),
            2,
            "b again (late commits), c new, a not"
        );
        let shas: Vec<char> = state
            .pending
            .iter()
            .map(|(sha, _)| sha.chars().next().unwrap())
            .collect();
        assert_eq!(shas, vec!['b', 'c']);
    }

    #[test]
    fn a_new_ladder_version_starts_every_table_over() {
        let mut state = State::default().for_version("r1");
        state.take("t", 1, [row('a', "20991101T160500Z")].iter());
        state.pending.clear();
        let next = state.for_version("r2");
        assert!(next.tables.is_empty() && next.pending.is_empty());
        assert_eq!(next.version, "r2");
    }
}
