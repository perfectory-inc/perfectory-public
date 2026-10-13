//! The host's service checks (root ADR-0207): how the live hostnames are reached from Korea and the
//! weekly pull request CI time, as `infra/systemd/foundation-service-checks.service` last wrote
//! them. Foundation only reads the file; a missing or unreadable one is said in `error`, not a 500,
//! so the operations map shows why the panel is empty.

use std::path::Path;
use std::sync::Arc;

use axum::{extract::State, Json};
use foundation_contracts::data_catalog::ServiceChecks;

use crate::state::AppState;

const PATH_ENV: &str = "FOUNDATION_PLATFORM_SERVICE_CHECKS_PATH";

/// The edge path from Korea to each live hostname and the CI time trend.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/service-checks",
    operation_id = "readServiceChecks",
    responses(
        (status = 200, body = ServiceChecks),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
    )
)]
pub(super) async fn service_checks(State(_state): State<Arc<AppState>>) -> Json<ServiceChecks> {
    let Some(path) = std::env::var_os(PATH_ENV) else {
        return Json(unread(format!("{PATH_ENV} is not set on this host")));
    };
    Json(read(Path::new(&path)))
}

/// A few kilobytes, so it is read in place rather than on a blocking thread.
fn read(path: &Path) -> ServiceChecks {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return unread(format!(
                "the service checks have not written {} yet ({error})",
                path.display()
            ))
        }
    };
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        unread(format!(
            "{} is not a service checks file ({error})",
            path.display()
        ))
    })
}

const fn unread(error: String) -> ServiceChecks {
    ServiceChecks {
        checked_at: None,
        edge: None,
        ci: None,
        error: Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file both sides test with (`orchestration/tests/test_service_checks.py` writes the same
    /// shape), found at run time from the directory cargo runs the test in, not compiled in.
    fn example() -> Result<String, Box<dyn std::error::Error>> {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        Ok(std::fs::read_to_string(Path::new(&crate_dir).join(
            "src/routes/data_catalog/service-checks.example.json",
        ))?)
    }

    fn scratch(name: &str) -> std::io::Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "foundation-service-checks-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[test]
    fn the_file_the_host_writes_is_served_as_it_is() -> Result<(), Box<dyn std::error::Error>> {
        let dir = scratch("served")?;
        let path = dir.join("service-checks.json");
        std::fs::write(&path, example()?)?;
        let checks = read(&path);
        assert_eq!(checks.error, None);
        let edge = checks.edge.ok_or("no edge part")?;
        assert_eq!(edge.reference_colo.as_deref(), Some("ICN"));
        assert_eq!(edge.hosts[0].colo.as_deref(), Some("SEA"));
        assert_eq!(
            (edge.hosts[0].p50_ms, edge.hosts[0].p95_ms),
            (Some(957), Some(3566))
        );
        assert_eq!(edge.hosts[1].error.as_deref(), Some("URLError: timed out"));
        let ci = checks.ci.ok_or("no ci part")?;
        assert!(ci
            .budget_minutes
            .is_some_and(|minutes| (minutes - 10.0).abs() < f64::EPSILON));
        assert_eq!(ci.pending_days, Some(3));
        assert!(ci.weeks[0]
            .p50
            .is_some_and(|minutes| (minutes - 23.4).abs() < f64::EPSILON));
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
        assert!(missing.edge.is_none() && missing.ci.is_none());
        let foreign = dir.join("foreign.json");
        std::fs::write(&foreign, r#"{"edge": 3}"#)?;
        assert!(read(&foreign)
            .error
            .is_some_and(|error| error.contains("not a service checks file")));
        Ok(())
    }
}
