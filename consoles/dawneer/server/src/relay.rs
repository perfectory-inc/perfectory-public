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
}

use Segment::{Literal, OneOf, Uuid};

/// The relayed Foundation routes: the steward API (root ADR-0115) and read-only status views.
const ROUTES: &[(Method, &[Segment])] = &[
    // What the map serves now (root ADR-0111).
    (Method::GET, &[Literal("vector-tiles"), Literal("manifest")]),
    (
        Method::GET,
        &[Literal("vector-tiles"), Literal("runtime-manifest")],
    ),
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
/// these are relayed to `<foundation>/data-catalog/v1/<path>`, read-only.
const DATA_CATALOG_ROUTES: &[(Method, &[Segment])] = &[
    (Method::GET, &[Literal("search")]),
    (Method::GET, &[Literal("entity")]),
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
            !allowed(&Method::GET, "pipeline-graph"),
            "the data catalog replaced the declared-graph view (root ADR-0119)"
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
    fn the_data_catalog_is_read_only_and_has_two_routes() {
        assert!(data_catalog_allowed(&Method::GET, "search"));
        assert!(data_catalog_allowed(&Method::GET, "entity"));
        assert!(!data_catalog_allowed(&Method::POST, "search"), "read-only");
        assert!(
            !data_catalog_allowed(&Method::GET, "graphql"),
            "no pass-through"
        );
        assert!(
            !allowed(&Method::GET, "search"),
            "the catalog list does not open data catalog paths"
        );
    }
}
