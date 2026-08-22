//! `GET /api/complexes` — 산업단지 목록·검색.
//!
//! 1,442곳을 이름으로 찾을 방법이 지도 클릭밖에 없던 것을 메우는 라우트예요. Gongzzang 은 B2C
//! 라우트 계약만 소유하고, 정본 표와 그 검색은 Foundation Platform Catalog 가 소유해요 —
//! 여기서는 published contract (`listComplexes`) 로만 읽어요.
//!
//! 파라미터 이름(`page`/`size`/`sort`, comma-separated 목록)은 이 저장소의 매물 검색
//! (`routes/listings/search.rs`) 관례를 그대로 따라요. 같은 화면에서 두 컬렉션을 넘기는 호출자가
//! 두 벌의 이름을 기억해야 하는 것이 이름이 갈리는 진짜 비용이에요.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use foundation_platform_client::CatalogComplexListQuery;
use product_identity_infrastructure::middleware::AuthenticatedUser;
use serde::{Deserialize, Serialize};

use crate::http::problem::{problem, ProblemResponse};

use super::{ComplexCatalogListRecord, ComplexCatalogPage, ComplexSearchRejected, ComplexesState};

/// 기본 쪽 크기. 매물 검색과 같은 값이에요.
const DEFAULT_PAGE_SIZE: u32 = 20;

/// 최대 쪽 크기.
///
/// 매물 검색(`routes/listings/search.rs`)과 같은 이 저장소의 관례예요. 이 방어가 막는 사고:
/// 로그인한 세션 하나가 `size=100000` 으로 정본 1,448행 전체를 한 요청에 끌어가고, 그 뒤에서
/// Foundation 이 행마다 Gold 포인터를 한 번씩 더 읽는 것. Foundation 도 같은 상한을 자기 문에서
/// 따로 거부하지만(`catalog_application::complex_search::MAX_PAGE_SIZE`), 브라우저가 두드리는
/// 문은 여기고, 사용자에게 해요체로 답해야 하는 것도 여기예요.
const MAX_PAGE_SIZE: u32 = 100;

/// 이 라우트가 중계하는 정렬 wire 값.
///
/// Foundation `listComplexes` 가 서빙하는 목록 그대로예요. 여기서 걸러 내는 이유는, 모르는 값을
/// 그냥 넘기면 사용자가 502(게이트웨이 오류)를 받기 때문이에요 — 실제로는 400(잘못된 요청)인데.
const SUPPORTED_SORTS: [&str; 3] = ["name_asc", "area_desc", "official_complex_code_asc"];

/// `GET /api/complexes` 쿼리 파라미터.
#[derive(Debug, Deserialize)]
pub struct ComplexesQuery {
    /// 이름 또는 산업단지코드 부분일치.
    pub q: Option<String>,
    /// 시도 코드 2자리.
    pub sido_code: Option<String>,
    /// 조성 단계 필터, comma-separated (예: `"operating,developing"`).
    pub status: Option<String>,
    /// 쪽 번호 (0부터, 기본 0).
    pub page: Option<u32>,
    /// 쪽 크기 (기본 20, 최대 100).
    pub size: Option<u32>,
    /// 정렬: `name_asc`(기본) | `area_desc` | `official_complex_code_asc`.
    pub sort: Option<String>,
}

/// 검증을 통과한 검색 조건.
///
/// 라우트가 만들고 reader 가 받아요. `size` 가 상한 안이라는 것을 타입이 아니라 이 구조체를
/// 만든 [`ComplexesQuery::validate`] 하나가 보장하고, reader 는 그 뒤로 검사를 다시 하지 않아요.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexSearchRequest {
    /// 이름·코드 부분일치 검색어. 공백만 있는 값은 여기 오지 않아요.
    pub q: Option<String>,
    /// 시도 코드 2자리.
    pub sido_code: Option<String>,
    /// 조성 단계 필터 원문 (comma-separated).
    pub status: Option<String>,
    /// 0부터 세는 쪽 번호.
    pub page: u32,
    /// 쪽 크기. `1..=100`.
    pub size: u32,
    /// 정렬 wire 값.
    pub sort: Option<String>,
}

impl ComplexSearchRequest {
    /// Foundation Catalog 컬렉션 쿼리로 옮겨요.
    #[must_use]
    pub fn to_catalog_query(&self) -> CatalogComplexListQuery {
        CatalogComplexListQuery {
            q: self.q.clone(),
            sido_code: self.sido_code.clone(),
            status: self.status.clone(),
            page: Some(self.page),
            size: Some(self.size),
            sort: self.sort.clone(),
        }
    }
}

impl ComplexesQuery {
    /// 쿼리 파라미터를 검증된 검색 조건으로 옮겨요.
    ///
    /// # Errors
    ///
    /// 상한 밖 `size`, 2자리가 아닌 `sido_code`, 서빙하지 않는 `sort` 는 `400` 이에요.
    fn validate(self) -> Result<ComplexSearchRequest, ProblemResponse> {
        let size = self.size.unwrap_or(DEFAULT_PAGE_SIZE);
        if size == 0 || size > MAX_PAGE_SIZE {
            return Err(invalid_filter(
                "size 파라미터는 1~100 사이여야 해요",
                format!("got size={size}, allowed 1..={MAX_PAGE_SIZE}"),
            ));
        }

        let sido_code = stated(self.sido_code);
        if let Some(code) = sido_code.as_deref() {
            if code.len() != 2 || !code.chars().all(|c| c.is_ascii_digit()) {
                return Err(invalid_filter(
                    "sido_code 는 2자리 숫자여야 해요",
                    format!("got '{code}'"),
                ));
            }
        }

        let sort = stated(self.sort);
        if let Some(value) = sort.as_deref() {
            if !SUPPORTED_SORTS.contains(&value) {
                return Err(invalid_filter(
                    "sort 값이 올바르지 않아요",
                    format!("unknown sort: {value}"),
                ));
            }
        }

        Ok(ComplexSearchRequest {
            q: stated(self.q),
            sido_code,
            status: stated(self.status),
            page: self.page.unwrap_or(0),
            size,
            sort,
        })
    }
}

/// 값이 실제로 적힌 파라미터만 남겨요. 빈 문자열은 "지우고 비운 검색창"이지 필터가 아니에요.
fn stated(value: Option<String>) -> Option<String> {
    value
        .map(|raw| raw.trim().to_owned())
        .filter(|trimmed| !trimmed.is_empty())
}

fn invalid_filter(title: &str, detail: String) -> ProblemResponse {
    problem(
        "complexes/invalid-filter",
        title,
        StatusCode::BAD_REQUEST,
        Some(detail),
    )
}

/// 목록 한 줄.
///
/// 값이 없는 칸은 **키 자체가 빠져요** (`skip_serializing_if`). 주소가 없는 단지가 실제로 있고,
/// 그런 줄은 화면이 주소 조각을 아예 빼야지 빈 자리를 남기면 안 돼요.
#[derive(Debug, Serialize)]
pub struct ComplexListItemResponse {
    /// 패널을 여는 열쇠. 없는 단지는 이 키가 빠지고, 화면은 그 줄을 누를 수 없게 그려요.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lakehouse_complex_id: Option<String>,
    /// 원천 산업단지 고유번호.
    pub official_complex_code: String,
    /// 산업단지명.
    pub name: String,
    /// 단지 종류 wire 값.
    pub kind: String,
    /// 조성 단계 wire 값.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// 원천이 적은 주소 문구.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_text: Option<String>,
}

impl From<ComplexCatalogListRecord> for ComplexListItemResponse {
    fn from(record: ComplexCatalogListRecord) -> Self {
        Self {
            lakehouse_complex_id: record.lakehouse_complex_id,
            official_complex_code: record.official_complex_code,
            name: record.name,
            kind: record.kind,
            status: record.status,
            address_text: record.address_text,
        }
    }
}

/// 쪽나눔 포함 목록 응답.
#[derive(Debug, Serialize)]
pub struct ComplexesResponse {
    /// 이 쪽의 줄들.
    pub complexes: Vec<ComplexListItemResponse>,
    /// 조건에 맞는 전체 건수. 화면의 "1,442곳 중 20곳" 이 이 값으로 쓰여요.
    pub total: u64,
    /// 0부터 세는 쪽 번호.
    pub page: u32,
    /// 쪽 크기.
    pub size: u32,
    /// 다음 쪽 존재 여부.
    pub has_next: bool,
}

impl From<ComplexCatalogPage> for ComplexesResponse {
    fn from(page: ComplexCatalogPage) -> Self {
        Self {
            complexes: page
                .complexes
                .into_iter()
                .map(ComplexListItemResponse::from)
                .collect(),
            total: page.total,
            page: page.page,
            size: page.size,
            has_next: page.has_next,
        }
    }
}

/// `GET /api/complexes` — 인증 필수.
///
/// # Errors
///
/// - 파라미터 검증 실패 → `400 complexes/invalid-filter`
/// - reader 백엔드 실패 → `502 complexes/lookup-failed`
#[tracing::instrument(
    skip(state, _auth),
    fields(page = q.page, size = q.size, sort = ?q.sort),
)]
pub async fn list_complexes(
    State(state): State<ComplexesState>,
    _auth: AuthenticatedUser,
    Query(q): Query<ComplexesQuery>,
) -> Result<Json<ComplexesResponse>, ProblemResponse> {
    let request = q.validate()?;

    let page = state.reader.search(&request).await.map_err(|e| {
        // 뒷단이 조건 자체를 거절한 것과 답을 못 준 것은 서로 다른 답이에요. 앞은 사용자가 고칠
        // 수 있고, 뒤는 기다리는 것 말고 할 수 있는 게 없어요.
        if let Some(rejected) = e.downcast_ref::<ComplexSearchRejected>() {
            tracing::info!(detail = %rejected.detail, "complex_catalog search parameters refused");
            return invalid_filter("검색 조건이 올바르지 않아요", rejected.detail.clone());
        }
        tracing::warn!(error = %e, "complex_catalog search failed");
        problem(
            "complexes/lookup-failed",
            "산업단지 목록을 불러오지 못했어요. 잠시 후 다시 시도해 주세요",
            StatusCode::BAD_GATEWAY,
            None,
        )
    })?;

    Ok(Json(ComplexesResponse::from(page)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use super::*;

    fn query(size: Option<u32>) -> ComplexesQuery {
        ComplexesQuery {
            q: None,
            sido_code: None,
            status: None,
            page: None,
            size,
            sort: None,
        }
    }

    #[test]
    fn a_size_past_the_bound_is_refused_at_this_door() {
        // 무력화 실험: `size > MAX_PAGE_SIZE` 조건을 지우면 이 단정이 빨개져요 — 그리고 그때
        // `size=100000` 은 그대로 Foundation 까지 흘러가요.
        let response = query(Some(100_000))
            .validate()
            .expect_err("100000 rows is not a page");

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert!(response.body.type_.ends_with("complexes/invalid-filter"));

        let zero = query(Some(0)).validate().expect_err("0 is not a page size");
        assert_eq!(zero.status, StatusCode::BAD_REQUEST);

        let accepted = query(Some(MAX_PAGE_SIZE))
            .validate()
            .expect("100 is served");
        assert_eq!(accepted.size, MAX_PAGE_SIZE);
    }

    #[test]
    fn defaults_match_the_listing_search() {
        let request = query(None).validate().expect("no parameters is a request");
        assert_eq!(request.page, 0);
        assert_eq!(request.size, DEFAULT_PAGE_SIZE);
        assert!(request.sort.is_none());
    }

    #[test]
    fn a_cleared_search_box_is_not_a_filter() {
        let request = ComplexesQuery {
            q: Some("   ".to_owned()),
            sido_code: Some(String::new()),
            status: None,
            page: None,
            size: None,
            sort: Some(String::new()),
        }
        .validate()
        .expect("blank parameters are absent parameters");

        assert!(request.q.is_none());
        assert!(request.sido_code.is_none());
        assert!(request.sort.is_none());
    }

    #[test]
    fn a_korean_word_survives_into_the_catalog_query() {
        let request = ComplexesQuery {
            q: Some(" 반월 ".to_owned()),
            sido_code: Some("41".to_owned()),
            status: Some("operating".to_owned()),
            page: Some(2),
            size: Some(50),
            sort: Some("area_desc".to_owned()),
        }
        .validate()
        .expect("a korean word is a word");

        let catalog_query = request.to_catalog_query();
        assert_eq!(catalog_query.q.as_deref(), Some("반월"));
        assert_eq!(catalog_query.sido_code.as_deref(), Some("41"));
        assert_eq!(catalog_query.status.as_deref(), Some("operating"));
        assert_eq!(catalog_query.page, Some(2));
        assert_eq!(catalog_query.size, Some(50));
        assert_eq!(catalog_query.sort.as_deref(), Some("area_desc"));
    }

    #[test]
    fn parameters_this_route_cannot_answer_are_400_not_502() {
        for bad in [
            ComplexesQuery {
                sido_code: Some("411".to_owned()),
                ..query(None)
            },
            ComplexesQuery {
                sort: Some("price_asc".to_owned()),
                ..query(None)
            },
        ] {
            let response = bad.validate().expect_err("unserved parameter");
            assert_eq!(response.status, StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn a_refused_condition_is_400_not_502() {
        // 이 라우트는 `status` 어휘를 갖고 있지 않아요 — Catalog 가 소유해요. 어휘를 베끼는 대신
        // 뒷단의 4xx 를 그대로 "고칠 수 있는 입력"으로 옮겨요. 이게 없으면 `status=bogus` 가
        // "잠시 후 다시 시도해 주세요"(502)로 끝나고, 다시 시도해도 영원히 같은 답이에요.
        let rejected: crate::routes::complexes::ComplexCatalogError =
            Box::new(ComplexSearchRejected {
                detail: "catalog answered 400 Bad Request".to_owned(),
            });

        assert!(rejected.downcast_ref::<ComplexSearchRejected>().is_some());

        let transport: crate::routes::complexes::ComplexCatalogError =
            Box::<dyn std::error::Error + Send + Sync>::from("connection refused");
        assert!(transport.downcast_ref::<ComplexSearchRejected>().is_none());
    }

    #[test]
    fn a_row_without_a_lakehouse_id_omits_the_key_rather_than_nulling_it() {
        // 정본 1,448행 중 6행이 이 모양이에요(쓰기 API 등록). 화면은 키의 부재로 "누를 수 없는
        // 줄"을 판정하므로, `null` 로 실어 보내면 프런트가 그 판정을 문자열 비교로 하게 돼요.
        let page = ComplexCatalogPage {
            complexes: vec![ComplexCatalogListRecord {
                lakehouse_complex_id: None,
                official_complex_code: "111010".to_owned(),
                name: "테스트산업단지".to_owned(),
                kind: "national".to_owned(),
                status: None,
                address_text: None,
            }],
            total: 1,
            page: 0,
            size: 20,
            has_next: false,
        };

        let json = serde_json::to_value(ComplexesResponse::from(page)).expect("serialize");
        let row = json["complexes"][0].as_object().expect("row object");
        assert!(!row.contains_key("lakehouse_complex_id"));
        assert!(!row.contains_key("address_text"));
        assert!(!row.contains_key("status"));
        assert_eq!(row["name"], "테스트산업단지");
        assert_eq!(json["total"], 1);
    }
}
