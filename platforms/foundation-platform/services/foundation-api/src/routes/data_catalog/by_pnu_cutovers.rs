//! The by-PNU cut-overs operators approved and what the host did with each (root ADR-0433 §6).
//!
//! An approval is the request file foundation-api wrote (`<switches>/cutover.d/<id>`, ADR-0433 §2);
//! what happened to it is the host's progress (`<releases>/<id>.jsonl`, one JSON line per event,
//! `scripts/ops/release-progress.py`), mounted read-only. Foundation only reads both, as it reads the
//! edition readiness report: a directory it cannot read is said in `error`, not a 500.

use std::path::Path;
use std::sync::Arc;

use axum::{extract::State, Json};
use foundation_contracts::data_catalog::{ByPnuCutoverApproval, ByPnuCutoverEvent, ByPnuCutovers};

use crate::state::AppState;

/// Where the approvals are: the switch directory foundation-api writes (root ADR-0282 §3).
pub(super) const SWITCHES_ENV: &str = "FOUNDATION_PLATFORM_JOB_SWITCHES_DIR";
/// Where the host's progress is, mounted read-only.
pub(super) const PROGRESS_ENV: &str = "FOUNDATION_PLATFORM_RELEASE_PROGRESS_DIR";
/// The newest approvals shown.
const SHOWN: usize = 20;

/// The newest approved by-PNU cut-overs, their state and every event the host recorded.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/by-pnu-cutovers",
    operation_id = "readByPnuCutovers",
    responses(
        (status = 200, body = ByPnuCutovers),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
    )
)]
pub(super) async fn by_pnu_cutovers(State(_state): State<Arc<AppState>>) -> Json<ByPnuCutovers> {
    let Some(switches) = std::env::var_os(SWITCHES_ENV) else {
        return Json(unread(format!("{SWITCHES_ENV} is not set on this host")));
    };
    let progress = std::env::var_os(PROGRESS_ENV);
    Json(read(
        &Path::new(&switches).join("cutover.d"),
        progress.as_deref().map(Path::new),
    ))
}

const fn unread(error: String) -> ByPnuCutovers {
    ByPnuCutovers {
        approvals: Vec::new(),
        error: Some(error),
    }
}

/// `key=value` lines, the switch board's format.
fn fields(text: &str) -> std::collections::HashMap<&str, &str> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .collect()
}

fn is_approval_id(name: &str) -> bool {
    uuid::Uuid::parse_str(name).is_ok_and(|id| id.hyphenated().to_string() == name)
}

/// A few files of a few hundred bytes each, so they are read in place.
fn read(approvals: &Path, progress: Option<&Path>) -> ByPnuCutovers {
    let entries = match std::fs::read_dir(approvals) {
        Ok(entries) => entries,
        // No approval yet: the directory is made by the first one.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ByPnuCutovers {
                approvals: Vec::new(),
                error: None,
            }
        }
        Err(error) => return unread(format!("cannot read {}: {error}", approvals.display())),
    };
    let mut listed: Vec<ByPnuCutoverApproval> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            // A partial write (`<id>.partial`) or anything else is not an approval.
            if !is_approval_id(&name) {
                return None;
            }
            let text = std::fs::read_to_string(entry.path()).ok()?;
            let fields = fields(&text);
            let events =
                progress.map_or_else(Vec::new, |dir| events(&dir.join(format!("{name}.jsonl"))));
            Some(ByPnuCutoverApproval {
                state: state(&events),
                id: name,
                actor: fields.get("actor")?.to_string(),
                reason: fields.get("reason")?.to_string(),
                lanes: fields.get("lanes")?.to_string(),
                generation: fields.get("generation")?.parse().ok()?,
                requested_at: fields.get("requested_at")?.to_string(),
                expires_at: fields.get("expires_at")?.to_string(),
                events,
            })
        })
        .collect();
    // RFC 3339 times of one writer sort as text.
    listed.sort_by(|a, b| b.requested_at.cmp(&a.requested_at));
    listed.truncate(SHOWN);
    ByPnuCutovers {
        approvals: listed,
        error: progress.is_none().then(|| {
            format!("{PROGRESS_ENV} is not set on this host: the host's progress is not shown")
        }),
    }
}

/// Every event of one approval, oldest first; a line that is not one is skipped.
fn events(path: &Path) -> Vec<ByPnuCutoverEvent> {
    std::fs::read_to_string(path)
        .map(|text| {
            text.lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// `pending` before the host answered; then the last event's state, a `finished` one as its
/// outcome. A `step` is a run still going.
fn state(events: &[ByPnuCutoverEvent]) -> String {
    match events.last() {
        None => "pending".to_owned(),
        Some(event) if event.state == "finished" => {
            event.outcome.clone().unwrap_or_else(|| "failed".to_owned())
        }
        Some(event) if event.state == "step" => "started".to_owned(),
        Some(event) => event.state.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const APPROVAL: &str = "44444444-4444-4444-8444-444444444444";
    const OLDER: &str = "55555555-5555-4555-8555-555555555555";

    fn scratch(name: &str) -> std::io::Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "foundation-by-pnu-cutovers-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("cutover.d"))?;
        std::fs::create_dir_all(dir.join("releases"))?;
        Ok(dir)
    }

    fn approve(dir: &Path, id: &str, at: &str) -> std::io::Result<()> {
        std::fs::write(
            dir.join("cutover.d").join(id),
            format!(
                "request={id}\nactor=staff-owner\nrequested_at={at}\nreason=g1 first publish\njob=by_pnu_lane_cutover\nexpires_at=2026-11-08T03:46:38+00:00\nlanes=parcel-ownership\ngeneration=1\n"
            ),
        )
    }

    #[test]
    fn the_eleven_o_six_late_approval_shows_it_waits_then_each_step(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 2026-11-06 03:46:38Z: approved too late for the night; it waits for 21:50 and then runs.
        let dir = scratch("steps")?;
        approve(&dir, APPROVAL, "2026-11-06T03:46:38+00:00")?;
        approve(&dir, OLDER, "2026-11-06T00:37:35+00:00")?;
        std::fs::write(
            dir.join("cutover.d").join(format!("{APPROVAL}.partial")),
            "lanes=x\n",
        )?;
        let progress = dir.join("releases").join(format!("{APPROVAL}.jsonl"));
        std::fs::write(
            &progress,
            concat!(
                r#"{"approval":"44444444-4444-4444-8444-444444444444","at":"2026-11-06T03:46:39Z","budget_minutes":202,"start_at":"2026-11-06T21:50:00Z","state":"scheduled"}"#,
                "\n",
                r#"{"approval":"44444444-4444-4444-8444-444444444444","at":"2026-11-06T21:50:01Z","state":"started","unit":"foundation-by-pnu-lane-cutover-g1"}"#,
                "\n",
            ),
        )?;
        let answer = read(&dir.join("cutover.d"), Some(&dir.join("releases")));
        assert_eq!(answer.error, None);
        let ids: Vec<&str> = answer.approvals.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(
            ids,
            [APPROVAL, OLDER],
            "newest first, the partial write skipped"
        );
        let [late, older] = answer.approvals.as_slice() else {
            return Err("two approvals".into());
        };
        assert_eq!((late.state.as_str(), late.events.len()), ("started", 2));
        assert_eq!(
            late.events[0].start_at.as_deref(),
            Some("2026-11-06T21:50:00Z")
        );
        assert_eq!((older.state.as_str(), older.generation), ("pending", 1));
        std::fs::write(
            &progress,
            std::fs::read_to_string(&progress)?
                + r#"{"at":"2026-11-06T22:17:00Z","state":"step","lane":"parcel-ownership","step":"gold","outcome":"passed","seconds":1602,"reason":"gold-kind gold.parcel_ownership mode=full"}"#
                + "\nnot json\n"
                + r#"{"at":"2026-11-06T23:40:00Z","state":"finished","outcome":"passed","reason":"every lane cut over at generation 1"}"#
                + "\n",
        )?;
        let answer = read(&dir.join("cutover.d"), Some(&dir.join("releases")));
        let late = &answer.approvals[0];
        assert_eq!((late.state.as_str(), late.events.len()), ("passed", 4));
        assert_eq!(late.events[2].seconds, Some(1602));
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn no_approval_yet_is_an_empty_list_and_no_progress_is_said(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = scratch("empty")?;
        let none = read(&dir.join("missing"), Some(&dir.join("releases")));
        assert_eq!((none.approvals.len(), none.error), (0, None));
        approve(&dir, APPROVAL, "2026-11-06T00:37:35+00:00")?;
        let unread = read(&dir.join("cutover.d"), None);
        assert_eq!(unread.approvals.len(), 1);
        assert!(unread
            .error
            .is_some_and(|error| error.contains(PROGRESS_ENV)));
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
