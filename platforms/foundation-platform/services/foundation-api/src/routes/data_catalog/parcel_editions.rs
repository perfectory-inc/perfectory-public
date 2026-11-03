//! Every cadastral edition's readiness and the promotions ledger (root ADR-0291 §2, ADR-0379), as
//! `scripts/ops/parcel-edition-readiness.sh` last wrote them on the host. Foundation only reads the
//! file, as it reads the service checks (root ADR-0209); a missing or unreadable one is said in
//! `error`, not a 500, so the console shows why the editions are not listed.

use std::path::Path;
use std::sync::Arc;

use axum::{extract::State, Json};
use foundation_contracts::data_catalog::ParcelEditions;

use crate::state::AppState;

pub(super) const PATH_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_EDITIONS_PATH";

/// Every cadastral edition, whether it is all there, and who served which edition when.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/parcel-editions",
    operation_id = "readParcelEditions",
    responses(
        (status = 200, body = ParcelEditions),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
    )
)]
pub(super) async fn parcel_editions(State(_state): State<Arc<AppState>>) -> Json<ParcelEditions> {
    let Some(path) = std::env::var_os(PATH_ENV) else {
        return Json(unread(format!("{PATH_ENV} is not set on this host")));
    };
    Json(read(Path::new(&path)))
}

/// A few kilobytes, so it is read in place rather than on a blocking thread.
fn read(path: &Path) -> ParcelEditions {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return unread(format!(
                "the edition readiness check has not written {} yet ({error})",
                path.display()
            ))
        }
    };
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        unread(format!(
            "{} is not an edition readiness report ({error})",
            path.display()
        ))
    })
}

const fn unread(error: String) -> ParcelEditions {
    ParcelEditions {
        checked_at_utc: None,
        served_edition: None,
        editions: Vec::new(),
        promotions: Vec::new(),
        error: Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A report `parcel_edition_readiness.py readiness` writes, on synthetic 2099 editions
    /// (`infra/lakehouse/spark/tests/test_parcel_edition_readiness.py` checks the same shape).
    fn example() -> Result<String, Box<dyn std::error::Error>> {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        Ok(std::fs::read_to_string(Path::new(&crate_dir).join(
            "src/routes/data_catalog/parcel-editions.example.json",
        ))?)
    }

    fn scratch(name: &str) -> std::io::Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "foundation-parcel-editions-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[test]
    fn the_report_the_host_writes_is_served_as_it_is() -> Result<(), Box<dyn std::error::Error>> {
        let dir = scratch("served")?;
        let path = dir.join("readiness.json");
        std::fs::write(&path, example()?)?;
        let report = read(&path);
        assert_eq!(report.error, None);
        assert_eq!(report.served_edition.as_deref(), Some("209910"));
        let older = &report.editions[0];
        assert_eq!(
            (
                older.edition.as_str(),
                older.ready,
                older.first_failing_stage.as_deref()
            ),
            ("209906", false, Some("rehearsal"))
        );
        assert!(older.stages.source.passed && !older.stages.rehearsal.passed);
        assert_eq!(report.promotions.len(), 2);
        assert!(report.promotions[0].readiness.is_none());
        assert_eq!(report.promotions[1].from_edition.as_deref(), Some("209906"));
        assert!(report.promotions[1].readiness.is_some());
        Ok(())
    }

    #[test]
    fn a_missing_or_foreign_file_is_an_error_field_not_a_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = scratch("unread")?;
        let missing = read(&dir.join("absent.json"));
        assert!(missing
            .error
            .is_some_and(|error| error.contains("not written")));
        assert!(missing.editions.is_empty() && missing.promotions.is_empty());
        let foreign = dir.join("foreign.json");
        std::fs::write(&foreign, r#"{"editions": 3}"#)?;
        assert!(read(&foreign)
            .error
            .is_some_and(|error| error.contains("not an edition readiness report")));
        Ok(())
    }
}
