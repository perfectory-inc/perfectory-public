//! Each collected source's expected cadence as Prometheus series, so the ingestion rules compare a
//! source's last Bronze run with its own bound instead of one 24-hour line for all (root ADR-0443).
//!
//! The bounds are the endpoint catalog's `expected_cadence`
//! (`docs/catalog/public-source-endpoint-catalog.v1.json`), derived from each provider's release
//! schedule. A source whose provider publishes on no fixed
//! schedule declares no age bound: its Bronze age is not a health signal, its job's schedule is.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::OnceLock;

use serde::Deserialize;

use super::pipeline_graph::artifact_path_from;
use super::prometheus_label_value as escape;

const PATH_ENV: &str = "FOUNDATION_PLATFORM_SOURCE_ENDPOINT_CATALOG_PATH";
const DEFAULT_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
/// The run-time bound of a source that declares none: the line the rule used for every source.
const DEFAULT_MAX_RUN_MINUTES: u64 = 60;

const HELP: &str = concat!(
    "# HELP foundation_platform_ingestion_source_expected_max_age_seconds Longest a source's latest finished Bronze run may be old, from its provider's release schedule (root ADR-0443).\n",
    "# TYPE foundation_platform_ingestion_source_expected_max_age_seconds gauge\n",
    "# HELP foundation_platform_ingestion_source_max_run_seconds Longest a source's Bronze run may take (root ADR-0443).\n",
    "# TYPE foundation_platform_ingestion_source_max_run_seconds gauge\n",
);

#[derive(Deserialize)]
struct Catalog {
    endpoints: Vec<Endpoint>,
}

#[derive(Deserialize)]
struct Endpoint {
    bronze: Bronze,
    expected_cadence: Option<Cadence>,
}

#[derive(Deserialize)]
struct Bronze {
    source_slug: String,
}

#[derive(Deserialize)]
struct Cadence {
    max_age_hours: Option<u64>,
    max_run_minutes: Option<u64>,
}

/// The series of the catalog the image carries; only the HELP lines when it cannot be read, which
/// the rules read as unreadable (`absent`). The file is fixed for the life of the process.
pub(super) fn source_cadence_metrics() -> String {
    static BODY: OnceLock<String> = OnceLock::new();
    BODY.get_or_init(|| {
        let path = artifact_path_from(std::env::var(PATH_ENV).ok().as_deref(), DEFAULT_PATH);
        match std::fs::read(&path).map_err(|error| error.to_string()).and_then(|bytes| {
            serde_json::from_slice::<Catalog>(&bytes).map_err(|error| error.to_string())
        }) {
            Ok(catalog) => render(&catalog),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "source endpoint catalog unreadable");
                HELP.to_owned()
            }
        }
    })
    .clone()
}

fn render(catalog: &Catalog) -> String {
    // A source two endpoints feed keeps the tighter age and the longer run.
    let mut bounds: BTreeMap<&str, (Option<u64>, u64)> = BTreeMap::new();
    for endpoint in &catalog.endpoints {
        let Some(cadence) = &endpoint.expected_cadence else {
            continue;
        };
        let run = cadence.max_run_minutes.unwrap_or(DEFAULT_MAX_RUN_MINUTES);
        let entry = bounds
            .entry(endpoint.bronze.source_slug.as_str())
            .or_insert((cadence.max_age_hours, run));
        entry.0 = match (entry.0, cadence.max_age_hours) {
            (Some(held), Some(this)) => Some(held.min(this)),
            (held, this) => held.or(this),
        };
        entry.1 = entry.1.max(run);
    }
    let mut body = String::from(HELP);
    for (source, (age_hours, run_minutes)) in bounds {
        let source = escape(source);
        if let Some(hours) = age_hours {
            let _ = writeln!(
                body,
                "foundation_platform_ingestion_source_expected_max_age_seconds{{source=\"{source}\"}} {}",
                hours * 3600
            );
        }
        let _ = writeln!(
            body,
            "foundation_platform_ingestion_source_max_run_seconds{{source=\"{source}\"}} {}",
            run_minutes * 60
        );
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(text: &str) -> Result<Catalog, serde_json::Error> {
        serde_json::from_str(text)
    }

    #[test]
    fn each_declared_source_gets_its_bounds_and_an_irregular_one_only_a_run_bound(
    ) -> Result<(), serde_json::Error> {
        let body = render(&catalog(
            r#"{"endpoints":[
                {"bronze":{"source_slug":"synthetic__monthly"},"expected_cadence":{"max_age_hours":1080}},
                {"bronze":{"source_slug":"synthetic__irregular"},"expected_cadence":{"max_age_hours":null,"max_run_minutes":300}},
                {"bronze":{"source_slug":"synthetic__two"},"expected_cadence":{"max_age_hours":72}},
                {"bronze":{"source_slug":"synthetic__two"},"expected_cadence":{"max_age_hours":48,"max_run_minutes":90}},
                {"bronze":{"source_slug":"synthetic__not_collected"}}
            ]}"#,
        )?);
        let series: Vec<&str> = body.lines().filter(|line| !line.starts_with('#')).collect();
        assert_eq!(
            series,
            vec![
                "foundation_platform_ingestion_source_max_run_seconds{source=\"synthetic__irregular\"} 18000",
                "foundation_platform_ingestion_source_expected_max_age_seconds{source=\"synthetic__monthly\"} 3888000",
                "foundation_platform_ingestion_source_max_run_seconds{source=\"synthetic__monthly\"} 3600",
                "foundation_platform_ingestion_source_expected_max_age_seconds{source=\"synthetic__two\"} 172800",
                "foundation_platform_ingestion_source_max_run_seconds{source=\"synthetic__two\"} 5400",
            ]
        );
        Ok(())
    }

    #[test]
    fn the_repository_catalog_renders_a_bound_for_every_source_it_declares() {
        let body = source_cadence_metrics();
        assert!(
            body.lines()
                .filter(|line| line
                    .starts_with("foundation_platform_ingestion_source_max_run_seconds{"))
                .count()
                > 100,
            "the catalog declares the cadence of every collected source"
        );
    }
}
