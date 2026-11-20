//! The Foundation routes the console may reach, and nothing else (root ADR-0116 §2).
//!
//! The browser asks `/api/foundation/<path>`; only a method and path on this list is relayed to
//! `<foundation>/catalog/v1/<path>`. Everything is decided by Foundation and the Identity
//! Platform — this list only narrows what a signed-in browser can even ask for.

use axum::http::Method;

/// One segment of an allowed route.
enum Segment {
    Literal(&'static str),
    Uuid,
    OneOf(&'static [&'static str]),
    /// A scheduled job's id: `[a-z][a-z0-9_]*`, as orchestration/jobs.v1.json names it.
    JobId,
}

use Segment::{JobId, Literal, OneOf, Uuid};

/// The relayed Foundation routes: the steward API (root ADR-0115) and read-only status views.
const ROUTES: &[(Method, &[Segment])] = &[
    // What the map serves now (root ADR-0111).
    (Method::GET, &[Literal("vector-tiles"), Literal("manifest")]),
    (
        Method::GET,
        &[Literal("vector-tiles"), Literal("runtime-manifest")],
    ),
    // What the pipeline last collected, loaded and delivered, as Foundation checks it live (the
    // graph's runtime overlay; root ADR-0165). Read-only.
    (Method::GET, &[Literal("pipeline-graph")]),
    // The canonical industrial complexes, read-only; edits stay with their owners.
    (Method::GET, &[Literal("complexes")]),
    (Method::GET, &[Literal("complexes"), Uuid]),
    (Method::GET, &[Literal("lineage-review"), Literal("items")]),
    (
        Method::GET,
        &[Literal("lineage-review"), Literal("items"), Uuid],
    ),
    (
        Method::POST,
        &[
            Literal("lineage-review"),
            Literal("items"),
            Uuid,
            OneOf(&["claim", "release", "decisions"]),
        ],
    ),
    (
        Method::POST,
        &[
            Literal("lineage-review"),
            Literal("decisions"),
            Uuid,
            Literal("approval"),
        ],
    ),
];

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => *b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

/// The data catalog routes (root ADR-0119): the browser asks `/api/data-catalog/<path>` and only
/// these are relayed to `<foundation>/data-catalog/v1/<path>`: reads, and the one write an operator
/// makes, switching a scheduled job (root ADR-0282), which Foundation authorizes per job.
const DATA_CATALOG_ROUTES: &[(Method, &[Segment])] = &[
    (Method::GET, &[Literal("search")]),
    (Method::GET, &[Literal("entity")]),
    // How the scheduled jobs ran and what the contracts' checks found (root ADR-0165).
    (Method::GET, &[Literal("scheduled-jobs")]),
    (Method::GET, &[Literal("quality-checks")]),
    // A dataset's Iceberg snapshots and what read them (root ADR-0195).
    (Method::GET, &[Literal("dataset-snapshots")]),
    // How Korea reaches the live hostnames and how long pull request CI takes (root ADR-0209).
    (Method::GET, &[Literal("service-checks")]),
    // Every cadastral edition's readiness and who served which (root ADR-0379).
    (Method::GET, &[Literal("parcel-editions")]),
    // The by-PNU cut-overs operators approved and what the host did with each (root ADR-0433).
    // Approving goes through `operator-actions` above, never through this read.
    (Method::GET, &[Literal("by-pnu-cutovers")]),
    // Pause, resume, run once now or stop one scheduled job (root ADR-0282).
    (
        Method::POST,
        &[
            Literal("scheduled-jobs"),
            JobId,
            Literal("operator-actions"),
        ],
    ),
    // The audit chain: read by account, object and time, exported, and its importers (root
    // ADR-0569 §7). Read only: the chain is written by the actions it records.
    (Method::GET, &[Literal("audit"), Literal("events")]),
    (
        Method::GET,
        &[Literal("audit"), Literal("events"), Literal("export")],
    ),
    (Method::GET, &[Literal("audit"), Literal("import-status")]),
];

/// Whether `method path` (path relative to `/catalog/v1/`) may be relayed.
#[must_use]
pub fn allowed(method: &Method, path: &str) -> bool {
    matches(ROUTES, method, path)
}

/// Whether `method path` (path relative to `/data-catalog/v1/`) may be relayed.
#[must_use]
pub fn data_catalog_allowed(method: &Method, path: &str) -> bool {
    matches(DATA_CATALOG_ROUTES, method, path)
}

fn is_job_id(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn matches(routes: &[(Method, &[Segment])], method: &Method, path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    routes.iter().any(|(m, segments)| {
        m == method
            && segments.len() == parts.len()
            && segments
                .iter()
                .zip(&parts)
                .all(|(segment, part)| match segment {
                    Literal(literal) => literal == part,
                    Uuid => is_uuid(part),
                    JobId => is_job_id(part),
                    OneOf(options) => options.contains(part),
                })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "00000000-0000-5000-8000-000000000001";

    #[test]
    fn only_the_listed_routes_are_relayed() {
        assert!(allowed(&Method::GET, "lineage-review/items"));
        assert!(allowed(&Method::GET, &format!("lineage-review/items/{ID}")));
        assert!(allowed(
            &Method::POST,
            &format!("lineage-review/items/{ID}/decisions")
        ));
        assert!(allowed(
            &Method::POST,
            &format!("lineage-review/decisions/{ID}/approval")
        ));

        assert!(
            !allowed(&Method::DELETE, "lineage-review/items"),
            "another method"
        );
        assert!(
            !allowed(&Method::POST, "lineage-review/items"),
            "another method"
        );
        assert!(
            allowed(&Method::GET, "pipeline-graph"),
            "the operations status reads its live runtime overlay (root ADR-0165)"
        );
        assert!(
            !allowed(&Method::POST, "pipeline-graph"),
            "status views are read-only"
        );
        assert!(allowed(&Method::GET, "vector-tiles/runtime-manifest"));
        assert!(
            !allowed(&Method::POST, "vector-tiles/manifest"),
            "status views are read-only"
        );
        assert!(allowed(&Method::GET, "complexes"));
        assert!(allowed(&Method::GET, &format!("complexes/{ID}")));
        assert!(
            !allowed(&Method::POST, "complexes"),
            "registering a complex is not the console's"
        );
        assert!(
            !allowed(&Method::GET, &format!("complexes/{ID}/attachments")),
            "another route"
        );
        assert!(!allowed(
            &Method::POST,
            &format!("lineage-review/items/{ID}/delete")
        ));
        assert!(
            !allowed(&Method::GET, "lineage-review/items/../../internal"),
            "not an id"
        );
        assert!(
            !allowed(&Method::GET, &format!("lineage-review/items/{ID}/")),
            "trailing segment"
        );
    }

    #[test]
    fn an_operator_switches_one_listed_kind_of_job_id_and_nothing_else() {
        // Root ADR-0282: the one data catalog write; Foundation decides who may, per job.
        assert!(data_catalog_allowed(
            &Method::POST,
            "scheduled-jobs/shape_ledger_check/operator-actions"
        ));
        for path in [
            "scheduled-jobs/Shape/operator-actions",
            "scheduled-jobs/../operator-actions",
            "scheduled-jobs/shape-ledger/operator-actions",
            "scheduled-jobs//operator-actions",
            "scheduled-jobs/1job/operator-actions",
            "scheduled-jobs/shape_ledger_check/operator-actions/x",
            "scheduled-jobs/shape_ledger_check",
        ] {
            assert!(!data_catalog_allowed(&Method::POST, path), "{path}");
        }
        assert!(
            !data_catalog_allowed(
                &Method::GET,
                "scheduled-jobs/shape_ledger_check/operator-actions"
            ),
            "only the write is listed"
        );
    }

    #[test]
    fn the_data_catalog_reads_are_read_only() {
        assert!(data_catalog_allowed(&Method::GET, "search"));
        assert!(data_catalog_allowed(&Method::GET, "entity"));
        assert!(data_catalog_allowed(&Method::GET, "scheduled-jobs"));
        assert!(data_catalog_allowed(&Method::GET, "quality-checks"));
        assert!(data_catalog_allowed(&Method::GET, "dataset-snapshots"));
        assert!(data_catalog_allowed(&Method::GET, "service-checks"));
        assert!(data_catalog_allowed(&Method::GET, "parcel-editions"));
        assert!(data_catalog_allowed(&Method::GET, "by-pnu-cutovers"));
        assert!(
            !data_catalog_allowed(&Method::POST, "by-pnu-cutovers"),
            "a cut-over is approved through operator-actions"
        );
        assert!(
            !data_catalog_allowed(&Method::POST, "parcel-editions"),
            "the served edition moves through a reviewed promotion, never from the console"
        );
        assert!(
            !data_catalog_allowed(&Method::POST, "service-checks"),
            "the checks run on the host, never from a screen"
        );
        assert!(
            !data_catalog_allowed(&Method::GET, "dataset-snapshots/x"),
            "one segment only"
        );
        assert!(!data_catalog_allowed(&Method::POST, "search"), "read-only");
        assert!(
            !data_catalog_allowed(&Method::POST, "scheduled-jobs"),
            "reading runs never starts one"
        );
        assert!(
            !data_catalog_allowed(&Method::GET, "graphql"),
            "no pass-through"
        );
        assert!(
            !allowed(&Method::GET, "search"),
            "the catalog list does not open data catalog paths"
        );
    }

    /// DW-2: the cut-over tab called a route this list did not hold, and the relay answered 404.
    /// `web/src/api/relayed-routes.json` is every route the web client calls (its own test keeps
    /// it equal to the client); each must be relayed.
    #[test]
    fn every_route_the_web_client_calls_is_relayed() -> Result<(), Box<dyn std::error::Error>> {
        // `cargo test` runs a crate's tests from that crate's directory.
        let text = std::fs::read_to_string("../web/src/api/relayed-routes.json")?;
        let listed: serde_json::Value = serde_json::from_str(&text)?;
        let routes = listed["routes"].as_array().ok_or("no routes array")?;
        assert!(!routes.is_empty());
        for route in routes {
            let method: Method = route["method"].as_str().ok_or("no method")?.parse()?;
            let path = route["path"].as_str().ok_or("no path")?;
            let relayed = match route["api"].as_str() {
                Some("foundation") => allowed(&method, path),
                Some("data-catalog") => data_catalog_allowed(&method, path),
                other => return Err(format!("unknown relay {other:?}").into()),
            };
            assert!(
                relayed,
                "the web calls {method} {path}, which the relay refuses"
            );
        }
        Ok(())
    }

    #[test]
    fn the_audit_chain_is_read_and_exported_never_written() {
        assert!(data_catalog_allowed(&Method::GET, "audit/events"));
        assert!(data_catalog_allowed(&Method::GET, "audit/events/export"));
        assert!(data_catalog_allowed(&Method::GET, "audit/import-status"));
        for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            assert!(
                !data_catalog_allowed(&method, "audit/events"),
                "{method} audit/events: the chain is written by the actions it records"
            );
        }
        assert!(!data_catalog_allowed(&Method::GET, "audit"));
        assert!(!data_catalog_allowed(&Method::GET, "audit/events/1"));
    }
}
