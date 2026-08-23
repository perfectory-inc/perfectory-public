//! `GET /catalog/v1/complexes` — one filtered, ordered page of the canonical collection.
//!
//! Split from `routes/catalog.rs` because that file is the whole Catalog surface and was already
//! near this workspace's 1,500-line ceiling. What lives here is the parameter translation and the
//! handler; the bounds themselves live in `catalog_application::complex_search`, and the response
//! mapping stays with the other Catalog DTO mappers in the parent module.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::Json;
use catalog_application::complex_search::{
    parse_status_filter, ComplexSearchPaging, ComplexSearchQuery, ComplexSearchQueryError,
    ComplexSearchSort, ComplexSearchText, SidoCodeFilter,
};
use catalog_application::ports::CatalogRepository;
use foundation_contracts::catalog::IndustrialComplexListResponse;
use serde::Deserialize;

use super::super::ApiError;
use super::industrial_complex_list_response;
use crate::state::AppState;

/// Query parameters for `GET /catalog/v1/complexes`.
///
/// Names follow the Gongzzang listing search (`routes/listings/search.rs`): `page` is zero-indexed,
/// `size` defaults to 20 and is capped at 100, and a multi-valued filter arrives comma-separated.
/// A second spelling of the same idea on a sibling collection is a thing callers get wrong.
#[derive(Debug, Deserialize)]
pub struct ListComplexesQuery {
    /// Substring the complex name or the official complex code must contain.
    pub q: Option<String>,
    /// Two-digit province code the complex's resolved address falls in.
    pub sido_code: Option<String>,
    /// Development lifecycle filter, comma-separated (e.g. `"operating,developing"`).
    pub status: Option<String>,
    /// Zero-indexed page number (default `0`).
    pub page: Option<u32>,
    /// Page size (default `20`, maximum `100`).
    pub size: Option<u32>,
    /// Order: `name_asc` (default) | `area_desc` | `official_complex_code_asc`.
    pub sort: Option<String>,
}

impl ListComplexesQuery {
    /// Translates the wire parameters into a bounded repository query.
    ///
    /// Every bound lives in `catalog_application::complex_search`, so this function cannot admit a
    /// value the repository query would then have to defend against.
    fn into_search_query(self) -> Result<ComplexSearchQuery, ComplexSearchQueryError> {
        Ok(ComplexSearchQuery {
            text: self
                .q
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(ComplexSearchText::try_new)
                .transpose()?,
            sido_code: self
                .sido_code
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(SidoCodeFilter::try_new)
                .transpose()?,
            statuses: self
                .status
                .as_deref()
                .map(parse_status_filter)
                .transpose()?
                .unwrap_or_default(),
            paging: ComplexSearchPaging::try_new(self.page, self.size)?,
            sort: self
                .sort
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(ComplexSearchSort::from_wire)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes",
    operation_id = "listComplexes",
    // utoipa defaults an operation's tag to its module path, so moving this handler into a child
    // module would have moved it into a tag of its own in the published document. The tag is stated
    // instead: a file split is not a contract change, and every other Catalog operation is here.
    tag = "super::catalog",
    params(
        ("q" = Option<String>, Query, description = "Substring of the complex name or official complex code"),
        ("sido_code" = Option<String>, Query, description = "Two-digit province code", min_length = 2, max_length = 2, pattern = "^[0-9]{2}$"),
        ("status" = Option<String>, Query, description = "Comma-separated development lifecycle filter: planned, developing, operating, changed, abolished, unknown"),
        ("page" = Option<u32>, Query, description = "Zero-indexed page number (default 0)"),
        ("size" = Option<u32>, Query, description = "Page size (default 20, maximum 100)", minimum = 1, maximum = 100),
        ("sort" = Option<String>, Query, description = "name_asc (default) | area_desc | official_complex_code_asc")
    ),
    responses(
        (status = 200, body = IndustrialComplexListResponse),
        (status = 400, description = "A search parameter was outside the served bounds")
    )
)]
pub async fn list_complexes(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListComplexesQuery>,
) -> Result<Json<IndustrialComplexListResponse>, ApiError> {
    let query = query
        .into_search_query()
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let page = state.catalog_repo.search_complexes(&query).await?;
    let complex_ids = page
        .rows
        .iter()
        .map(|complex| complex.id)
        .collect::<Vec<_>>();
    let gold_pointers = state
        .industrial_complex_gold_pointer_reader
        .list_industrial_complex_gold_pointers(&complex_ids)
        .await?
        .into_iter()
        .map(|pointer| (pointer.complex_id, pointer))
        .collect::<HashMap<_, _>>();

    Ok(Json(IndustrialComplexListResponse {
        complexes: industrial_complex_list_response(page.rows, gold_pointers),
        total: page.total,
        page: query.paging.page(),
        size: query.paging.size(),
        has_next: query.paging.has_next(page.total),
    }))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Query parameters as a browser would send them, before validation.
    fn list_query(
        q: Option<&str>,
        sido_code: Option<&str>,
        status: Option<&str>,
        page: Option<u32>,
        size: Option<u32>,
        sort: Option<&str>,
    ) -> ListComplexesQuery {
        ListComplexesQuery {
            q: q.map(ToOwned::to_owned),
            sido_code: sido_code.map(ToOwned::to_owned),
            status: status.map(ToOwned::to_owned),
            page,
            size,
            sort: sort.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn refuses_a_page_size_past_the_bound() {
        // What this prevents: one request pulling the whole canonical table (1,448 rows) plus a
        // Gold-pointer lookup per row. Disabling experiment: remove the `size > MAX_PAGE_SIZE` arm
        // in `ComplexSearchPaging::try_new` and this assertion goes red — the query builds instead.
        let error = list_query(None, None, None, None, Some(100_000), None)
            .into_search_query()
            .expect_err("100000 rows is not a page");
        assert_eq!(error, ComplexSearchQueryError::PageSizeOutOfRange(100_000));

        let accepted = list_query(None, None, None, None, Some(100), None)
            .into_search_query()
            .expect("100 is the documented maximum");
        assert_eq!(accepted.paging.size(), 100);
    }

    #[test]
    fn defaults_follow_the_listing_search() {
        let query = list_query(None, None, None, None, None, None)
            .into_search_query()
            .expect("an unparameterized request is valid");

        assert_eq!(query.paging.page(), 0);
        assert_eq!(query.paging.size(), 20);
        assert_eq!(query.sort.wire_name(), "name_asc");
        assert!(query.text.is_none());
        assert!(query.sido_code.is_none());
        assert!(query.statuses.is_empty());
    }

    #[test]
    fn carries_a_korean_word_into_an_escaped_pattern() {
        let query = list_query(Some(" 반월 "), None, None, None, None, None)
            .into_search_query()
            .expect("a korean word is a word");
        let text = query.text.expect("q was stated");
        assert_eq!(text.as_str(), "반월");
        assert_eq!(text.contains_pattern(), "%반월%");
    }

    #[test]
    fn refuses_parameters_it_cannot_answer() {
        // Each of these would otherwise become "no results", which reads as "there are none"
        // rather than "you asked for something this route does not serve".
        assert_eq!(
            list_query(None, Some("411"), None, None, None, None)
                .into_search_query()
                .expect_err("three digits is not a province code"),
            ComplexSearchQueryError::InvalidSidoCode("411".to_owned())
        );
        assert_eq!(
            list_query(None, None, Some("operating,sold_out"), None, None, None)
                .into_search_query()
                .expect_err("sold_out is not a lifecycle value"),
            ComplexSearchQueryError::UnknownStatus("sold_out".to_owned())
        );
        assert_eq!(
            list_query(None, None, None, None, None, Some("price_asc"))
                .into_search_query()
                .expect_err("this collection has no price"),
            ComplexSearchQueryError::UnknownSort("price_asc".to_owned())
        );
        assert_eq!(
            list_query(Some("   "), None, None, None, None, None)
                .into_search_query()
                .expect_err("whitespace is not a search word"),
            ComplexSearchQueryError::BlankText
        );
    }

    #[test]
    fn an_empty_q_is_the_same_request_as_no_q() {
        // A search box that has been cleared sends `q=`. That is "no filter", not "match nothing".
        let query = list_query(Some(""), Some(""), None, None, None, Some(""))
            .into_search_query()
            .expect("empty parameters are absent parameters");
        assert!(query.text.is_none());
        assert!(query.sido_code.is_none());
        assert_eq!(query.sort.wire_name(), "name_asc");
    }
}
