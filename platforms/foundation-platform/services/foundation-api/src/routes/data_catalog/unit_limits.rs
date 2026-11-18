//! Every host unit and container measured against its limits (root ADR-0574), as
//! `infra/systemd/foundation-unit-limits.service` last wrote them. Foundation only reads the file;
//! a missing or unreadable one is said in `error`, not a 500, so Dawneer shows why the table is
//! empty. The limits themselves change only in code (the host budget contract and its guard).

use std::path::Path;
use std::sync::Arc;

use axum::{extract::State, Json};
use foundation_contracts::data_catalog::UnitLimits;

use crate::state::AppState;

pub(super) const PATH_ENV: &str = "FOUNDATION_PLATFORM_UNIT_LIMITS_PATH";

/// Each unit's and container's declared limits, measured peak and run time, failures and state.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/unit-limits",
    operation_id = "readUnitLimits",
    responses(
        (status = 200, body = UnitLimits),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
    )
)]
pub(super) async fn unit_limits(State(_state): State<Arc<AppState>>) -> Json<UnitLimits> {
    let Some(path) = std::env::var_os(PATH_ENV) else {
        return Json(unread(format!("{PATH_ENV} is not set on this host")));
    };
    Json(read(Path::new(&path)))
}

/// About 200 units at a few hundred bytes each, so it is read in place rather than on a blocking
/// thread.
fn read(path: &Path) -> UnitLimits {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return unread(format!(
                "the unit limits have not been written to {} yet ({error})",
                path.display()
            ))
        }
    };
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        unread(format!(
            "{} is not a unit limits file ({error})",
            path.display()
        ))
    })
}

const fn unread(error: String) -> UnitLimits {
    UnitLimits {
        measured_at: None,
        window_days: None,
        near_share: None,
        near_runs: None,
        host_memory_bytes: None,
        container_caps_bytes: None,
        units: Vec::new(),
        containers: Vec::new(),
        containers_error: None,
        error: Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file both sides test with (`orchestration/tests/test_unit_limits.py` holds the script's
    /// view to the same shape), found at run time from the directory cargo runs the test in.
    fn example() -> Result<String, Box<dyn std::error::Error>> {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        Ok(std::fs::read_to_string(
            Path::new(&crate_dir).join("src/routes/data_catalog/unit-limits.example.json"),
        )?)
    }

    fn scratch(name: &str) -> std::io::Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "foundation-unit-limits-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[test]
    fn the_file_the_host_writes_is_served_as_it_is() -> Result<(), Box<dyn std::error::Error>> {
        let dir = scratch("served")?;
        let path = dir.join("unit-limits.json");
        std::fs::write(&path, example()?)?;
        let limits = read(&path);
        assert_eq!(limits.error, None);
        assert_eq!(limits.window_days, Some(14));
        let failing = limits
            .units
            .iter()
            .find(|unit| unit.state == "failing")
            .ok_or("no failing unit")?;
        assert_eq!((failing.oom, failing.memory_max), (1, Some(536_870_912)));
        assert!(failing.last_oom_at.is_some());
        assert_eq!(limits.containers[0].oom, 2);
        assert_eq!(limits.container_caps_bytes, Some(2_684_354_560));
        Ok(())
    }

    #[test]
    fn a_missing_or_foreign_file_is_an_error_field_not_a_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = scratch("unread")?;
        let missing = read(&dir.join("absent.json"));
        assert!(missing
            .error
            .is_some_and(|error| error.contains("not been written")));
        assert!(missing.units.is_empty());
        let foreign = dir.join("foreign.json");
        std::fs::write(&foreign, r#"{"units": 3}"#)?;
        assert!(read(&foreign)
            .error
            .is_some_and(|error| error.contains("not a unit limits file")));
        Ok(())
    }
}
