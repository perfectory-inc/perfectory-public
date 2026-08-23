//! `GET /api/complexes/:lakehouse_complex_id` — 산업단지 요약 조회.
//!
//! Gongzzang 은 B2C 라우트 계약과 사용자 대면 응답 모양만 소유해요. 산업단지 정본은 Foundation
//! Platform Catalog 가 소유하고, 여기서는 published contract 로만 읽어요.
//!
//! 경로 키가 lakehouse id 인 이유: 지도의 `complex` 레이어가 feature id 로 발행하는 값이 그것이고,
//! Catalog 의 `id` 와는 서로 계산되지 않는 **다른 식별자**예요. 둘을 섞으면 조회가 조용히 비므로
//! [`shared_kernel::lakehouse_complex_id::LakehouseComplexId`] 가 입구에서 형식을 거부해요.
//!
//! 목록(`GET /api/complexes`)은 [`search`] 에 있어요. 두 라우트가 같은 reader 포트를 쓰는 이유는
//! 같은 정본 표를 읽기 때문이고, 목록 행이 `lakehouse_complex_id` 를 실어 보내는 이유는 그 값이
//! 단건 조회의 열쇠이기 때문이에요 — 목록에서 한 줄을 눌러 패널을 열 수 있는 근거가 그거예요.

/// `GET /api/complexes` — 산업단지 목록·검색.
pub mod search;

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use product_identity_infrastructure::middleware::AuthenticatedUser;
use serde::Serialize;
use shared_kernel::lakehouse_complex_id::LakehouseComplexId;

use crate::http::problem::{problem, ProblemResponse};

/// Reader 통신·파싱 실패. 라우트에서 `502` 로 매핑돼요.
pub type ComplexCatalogError = Box<dyn std::error::Error + Send + Sync>;

/// 산업단지 reader 포트.
///
/// 구현:
/// - production: `services/gongzzang-api/src/complex_reader.rs::FoundationPlatformComplexCatalogReader`
/// - dev fallback: `startup.rs::NoOpComplexCatalogReader`
pub trait ComplexCatalogReader: Send + Sync {
    /// lakehouse id 로 산업단지 하나를 조회해요. 없으면 `Ok(None)`.
    ///
    /// # Errors
    ///
    /// 뒷단 Foundation Platform 호출이나 응답 변환이 실패하면 reader 에러를 반환해요.
    fn find_by_lakehouse_id<'a>(
        &'a self,
        lakehouse_complex_id: &'a LakehouseComplexId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<ComplexCatalogRecord>, ComplexCatalogError>,
                > + Send
                + 'a,
        >,
    >;

    /// 검증된 검색 조건으로 산업단지 한 쪽(page)을 읽어요.
    ///
    /// # Errors
    ///
    /// 뒷단 Foundation Platform 호출이나 응답 변환이 실패하면 reader 에러를 반환해요.
    fn search<'a>(
        &'a self,
        query: &'a search::ComplexSearchRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ComplexCatalogPage, ComplexCatalogError>>
                + Send
                + 'a,
        >,
    >;
}

/// 뒷단이 **요청 자체를 거절**했어요 — 답을 못 준 것이 아니라.
///
/// 이 라우트는 자기가 아는 조건(`size`·`sido_code`·`sort`)만 문 앞에서 검사해요. Catalog 가
/// 소유한 어휘(`status` 의 조성 단계 값 같은 것)를 여기에 베껴 두면 그 사본이 낡고, 낡은 사본은
/// 원천이 값을 하나 늘린 날 멀쩡한 요청을 거절해요. 그래서 검사하지 않고 넘기되, 뒷단이 4xx 로
/// 답하면 그것은 "게이트웨이 고장"이 아니라 "그런 조건은 서빙하지 않는다"이므로 `400` 으로
/// 옮겨요. 이 구분이 없으면 사용자는 고칠 수 있는 입력 오류를 "잠시 후 다시 시도해 주세요"로
/// 돌려받아요.
#[derive(Debug, thiserror::Error)]
#[error("catalog refused the search parameters: {detail}")]
pub struct ComplexSearchRejected {
    /// 뒷단이 돌려준 상태 코드 등, 로그에만 남는 설명.
    pub detail: String,
}

/// 목록 한 줄이 그리는 칸만 실은 레코드.
///
/// 요약 카드(`ComplexCatalogRecord`)보다 좁아요. 목록은 이름으로 하나를 찾는 화면이고, 한 줄이
/// 답해야 하는 질문은 "이게 내가 찾던 그 단지인가"뿐이에요. 나머지는 눌러서 열리는 패널의 몫이고,
/// 목록 응답이 그것까지 실으면 1,448행의 정본 표를 20행씩 그대로 내보내는 셈이에요.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexCatalogListRecord {
    /// lakehouse 식별자. 패널을 여는 열쇠예요.
    ///
    /// `None` 인 단지가 실제로 있어요(쓰기 API 로 등록된 행 — 2026-08-23 정본 1,448행 중 6행).
    /// 그런 줄은 열 패널이 없으므로 화면이 누를 수 없는 줄로 그려요.
    pub lakehouse_complex_id: Option<String>,
    /// 원천 산업단지 고유번호.
    pub official_complex_code: String,
    /// 산업단지명.
    pub name: String,
    /// 단지 종류 wire 값.
    pub kind: String,
    /// 조성 단계 wire 값.
    pub status: Option<String>,
    /// 원천이 적은 주소 문구.
    pub address_text: Option<String>,
}

/// 산업단지 목록 한 쪽과 그 쪽이 잘라 온 전체 건수.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexCatalogPage {
    /// 이 쪽의 줄들.
    pub complexes: Vec<ComplexCatalogListRecord>,
    /// 조건에 맞는 전체 건수.
    pub total: u64,
    /// 0부터 세는 쪽 번호.
    pub page: u32,
    /// 쪽 크기.
    pub size: u32,
    /// 다음 쪽 존재 여부.
    pub has_next: bool,
}

/// `/api/complexes` 핸들러 공유 상태.
#[derive(Clone)]
pub struct ComplexesState {
    /// 산업단지 reader 포트.
    pub reader: Arc<dyn ComplexCatalogReader>,
}

/// 라우트 대면 산업단지 레코드.
///
/// Catalog 응답 전부가 아니라 요약 카드가 그리는 칸만 실어요. `version`·`updated_at`·`archived_at`
/// 과 Gold 포인터는 Catalog 의 장부와 R2 주소지정이고, 그것까지 옮기면 쓰지도 않는 용도를
/// 주장하는 셈이에요.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexCatalogRecord {
    /// 원천 산업단지 고유번호.
    pub official_complex_code: String,
    /// 산업단지명.
    pub name: String,
    /// 단지 종류 wire 값 (`national` / `general` / `agricultural` / `urban_high_tech`).
    pub kind: String,
    /// 조성 단계 wire 값. `unknown` 은 "원천이 이 계약이 모르는 값을 적었다", 부재는 "원천이
    /// 아무것도 적지 않았다" — 서로 다른 답이에요.
    pub status: Option<String>,
    /// 원천이 적은 주소 문구.
    pub address_text: Option<String>,
    /// 지정 면적 (㎡).
    pub area_m2: u64,
    /// 관리기관명.
    pub management_agency_name: Option<String>,
    /// 시행자명.
    pub developer_name: Option<String>,
    /// 지정일 (`YYYY-MM-DD`).
    pub designated_date: Option<String>,
    /// 착공일 (`YYYY-MM-DD`).
    pub construction_start_date: Option<String>,
    /// 준공일 (`YYYY-MM-DD`).
    pub completion_date: Option<String>,
    /// 조성진행률. 정확한 십진 문자열이고 `"0.00"` 은 실제 답이에요 (착공 전).
    pub development_progress_percent: Option<String>,
    /// 분양 상태 wire 값 (`planned` / `in_progress` / `completed`).
    pub lot_sales_status: Option<String>,
    /// 사업기간 원문.
    pub business_period_raw: Option<String>,
    /// 지정 근거 법률 원문.
    pub designation_basis_law_raw: Option<String>,
    /// 개발 방식 원문.
    pub development_method_raw: Option<String>,
    /// 조성 목적 원문.
    pub development_purpose_raw: Option<String>,
    /// 유치 업종 원문.
    pub invited_industries_raw: Option<String>,
}

/// 산업단지 요약 응답.
///
/// 값이 없는 칸은 **키 자체가 빠져요** (`skip_serializing_if`). 프런트가 빈 문자열과 "없음" 을
/// 구분하려고 애쓰는 대신, 없는 줄은 애초에 그리지 않도록 하기 위해서예요.
#[derive(Debug, Serialize)]
pub struct ComplexInfoResponse {
    /// 요청에 쓰인 lakehouse 식별자를 그대로 돌려줘요.
    pub lakehouse_complex_id: String,
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
    /// 지정 면적 (㎡).
    pub area_m2: u64,
    /// 관리기관명.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub management_agency_name: Option<String>,
    /// 시행자명.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_name: Option<String>,
    /// 지정일.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub designated_date: Option<String>,
    /// 착공일.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub construction_start_date: Option<String>,
    /// 준공일.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_date: Option<String>,
    /// 조성진행률 (정확한 십진 문자열).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub development_progress_percent: Option<String>,
    /// 분양 상태 wire 값.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lot_sales_status: Option<String>,
    /// 사업기간 원문.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub business_period_raw: Option<String>,
    /// 지정 근거 법률 원문.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub designation_basis_law_raw: Option<String>,
    /// 개발 방식 원문.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub development_method_raw: Option<String>,
    /// 조성 목적 원문.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub development_purpose_raw: Option<String>,
    /// 유치 업종 원문.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invited_industries_raw: Option<String>,
}

impl ComplexInfoResponse {
    fn from_record(
        lakehouse_complex_id: &LakehouseComplexId,
        record: ComplexCatalogRecord,
    ) -> Self {
        Self {
            lakehouse_complex_id: lakehouse_complex_id.as_str().to_owned(),
            official_complex_code: record.official_complex_code,
            name: record.name,
            kind: record.kind,
            status: record.status,
            address_text: record.address_text,
            area_m2: record.area_m2,
            management_agency_name: record.management_agency_name,
            developer_name: record.developer_name,
            designated_date: record.designated_date,
            construction_start_date: record.construction_start_date,
            completion_date: record.completion_date,
            development_progress_percent: record.development_progress_percent,
            lot_sales_status: record.lot_sales_status,
            business_period_raw: record.business_period_raw,
            designation_basis_law_raw: record.designation_basis_law_raw,
            development_method_raw: record.development_method_raw,
            development_purpose_raw: record.development_purpose_raw,
            invited_industries_raw: record.invited_industries_raw,
        }
    }
}

/// `GET /api/complexes/:lakehouse_complex_id` — 인증 필수.
///
/// # Errors
///
/// - lakehouse 식별자 형식 오류 → `400 invalid-lakehouse-complex-id`
/// - reader 백엔드 실패 → `502 complex-lookup-failed`
/// - 미발견 → `404 complex-not-found`
pub async fn get_complex(
    State(state): State<ComplexesState>,
    _auth: AuthenticatedUser,
    Path(raw_id): Path<String>,
) -> Result<Json<ComplexInfoResponse>, ProblemResponse> {
    let lakehouse_complex_id = LakehouseComplexId::try_new(&raw_id).map_err(|e| {
        problem(
            "invalid-lakehouse-complex-id",
            "잘못된 산업단지 식별자예요",
            StatusCode::BAD_REQUEST,
            Some(format!("{e}")),
        )
    })?;

    let record = state
        .reader
        .find_by_lakehouse_id(&lakehouse_complex_id)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, lakehouse_complex_id = %lakehouse_complex_id, "complex_catalog read failed");
            problem(
                "complex-lookup-failed",
                "산업단지 정보를 불러오지 못했어요. 잠시 후 다시 시도해 주세요",
                StatusCode::BAD_GATEWAY,
                None,
            )
        })?
        .ok_or_else(|| {
            problem(
                "complex-not-found",
                "해당 산업단지를 찾지 못했어요",
                StatusCode::NOT_FOUND,
                Some(format!("lakehouse_complex_id={lakehouse_complex_id}")),
            )
        })?;

    Ok(Json(ComplexInfoResponse::from_record(
        &lakehouse_complex_id,
        record,
    )))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use axum::extract::{Path, State};
    use chrono::Utc;
    use product_identity_infrastructure::claims::{Audience, Claims};
    use shared_kernel::email::Email;
    use shared_kernel::id::Id;
    use user_domain::entity::{User, UserKind};

    use super::*;

    const LAKEHOUSE_ID: &str = "7df3859c-1e0a-51fa-8b7d-9a1c2e3f4a5b";
    /// `Uuid::now_v7()` 모양 — `catalog.industrial_complex.id` 가 사는 식별자 공간.
    const CATALOG_ID: &str = "01a0136d-2b3c-7e61-8f90-a1b2c3d4e5f6";

    struct StubReader(Result<Option<ComplexCatalogRecord>, &'static str>);

    impl ComplexCatalogReader for StubReader {
        fn find_by_lakehouse_id<'a>(
            &'a self,
            _lakehouse_complex_id: &'a LakehouseComplexId,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Option<ComplexCatalogRecord>, ComplexCatalogError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                match &self.0 {
                    Ok(record) => Ok(record.clone()),
                    Err(message) => Err(Box::<dyn std::error::Error + Send + Sync>::from(*message)),
                }
            })
        }

        fn search<'a>(
            &'a self,
            query: &'a search::ComplexSearchRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ComplexCatalogPage, ComplexCatalogError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(ComplexCatalogPage {
                    complexes: Vec::new(),
                    total: 0,
                    page: query.page,
                    size: query.size,
                    has_next: false,
                })
            })
        }
    }

    fn state(reader: StubReader) -> State<ComplexesState> {
        State(ComplexesState {
            reader: Arc::new(reader),
        })
    }

    fn authenticated_user() -> AuthenticatedUser {
        let user = User::try_new(
            Id::new(),
            "complex-test-sub",
            Email::try_new("complex@test.local").expect("email"),
            "Complex Tester",
            UserKind::Individual,
            Utc::now(),
        )
        .expect("test user");
        AuthenticatedUser {
            user,
            claims: Claims {
                sub: "complex-test-sub".to_owned(),
                email: Some("complex@test.local".to_owned()),
                name: Some("Complex Tester".to_owned()),
                preferred_username: None,
                jti: "test-jti".to_owned(),
                exp: i64::MAX,
                nbf: None,
                iss: "test".to_owned(),
                aud: Audience::Single("test".to_owned()),
            },
        }
    }

    fn record() -> ComplexCatalogRecord {
        ComplexCatalogRecord {
            official_complex_code: "111010".to_owned(),
            name: "테스트산업단지".to_owned(),
            kind: "national".to_owned(),
            status: None,
            address_text: None,
            area_m2: 1_234_567,
            management_agency_name: None,
            developer_name: None,
            designated_date: None,
            construction_start_date: None,
            completion_date: None,
            development_progress_percent: None,
            lot_sales_status: None,
            business_period_raw: None,
            designation_basis_law_raw: None,
            development_method_raw: None,
            development_purpose_raw: None,
            invited_industries_raw: None,
        }
    }

    #[tokio::test]
    async fn rejects_a_catalog_id_at_the_door() {
        // 이 라우트가 존재하는 이유가 두 식별자 공간이 다르다는 것이므로, 정본 id 를 보낸
        // 요청은 "그런 단지 없음"(404)이 아니라 "식별자가 틀렸음"(400)으로 끝나야 해요.
        let response = get_complex(
            state(StubReader(Ok(Some(record())))),
            authenticated_user(),
            Path(CATALOG_ID.to_owned()),
        )
        .await
        .expect_err("a catalog id must not be accepted here");

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert!(response
            .body
            .type_
            .ends_with("invalid-lakehouse-complex-id"));
    }

    #[tokio::test]
    async fn missing_complex_is_404() {
        let response = get_complex(
            state(StubReader(Ok(None))),
            authenticated_user(),
            Path(LAKEHOUSE_ID.to_owned()),
        )
        .await
        .expect_err("no record means not found");

        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(response.body.type_.ends_with("complex-not-found"));
    }

    #[tokio::test]
    async fn reader_failure_is_502_without_leaking_the_backend_message() {
        let response = get_complex(
            state(StubReader(Err(
                "foundation says: connection refused to 192.0.2.10",
            ))),
            authenticated_user(),
            Path(LAKEHOUSE_ID.to_owned()),
        )
        .await
        .expect_err("a reader failure is a gateway failure");

        assert_eq!(response.status, StatusCode::BAD_GATEWAY);
        assert!(response.body.type_.ends_with("complex-lookup-failed"));
        assert!(
            response.body.detail.is_none(),
            "backend detail must not reach the browser"
        );
    }

    #[tokio::test]
    async fn absent_columns_are_absent_keys_not_empty_ones() {
        // 카드가 "없는 줄은 그리지 않는다"를 할 수 있는 근거가 이 응답 모양이에요. 값이 없는
        // 칸은 `null` 이 아니라 키 자체가 없어야 해요.
        let response = get_complex(
            state(StubReader(Ok(Some(record())))),
            authenticated_user(),
            Path(LAKEHOUSE_ID.to_owned()),
        )
        .await
        .expect("record is served");

        let json = serde_json::to_value(&response.0).expect("serialize");
        let object = json.as_object().expect("object");
        assert_eq!(object["lakehouse_complex_id"], LAKEHOUSE_ID);
        assert_eq!(object["official_complex_code"], "111010");
        for absent in [
            "status",
            "address_text",
            "completion_date",
            "development_progress_percent",
            "invited_industries_raw",
        ] {
            assert!(!object.contains_key(absent), "{absent} must not be a key");
        }
    }

    #[tokio::test]
    async fn zero_progress_is_serialized() {
        let response = get_complex(
            state(StubReader(Ok(Some(ComplexCatalogRecord {
                development_progress_percent: Some("0.00".to_owned()),
                ..record()
            })))),
            authenticated_user(),
            Path(LAKEHOUSE_ID.to_owned()),
        )
        .await
        .expect("record is served");

        let json = serde_json::to_value(&response.0).expect("serialize");
        assert_eq!(json["development_progress_percent"], "0.00");
    }
}
