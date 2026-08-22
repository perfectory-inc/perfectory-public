//! `GET /api/complexes/:lakehouse_complex_id` — 산업단지 요약 조회.
//!
//! Gongzzang 은 B2C 라우트 계약과 사용자 대면 응답 모양만 소유해요. 산업단지 정본은 Foundation
//! Platform Catalog 가 소유하고, 여기서는 published contract 로만 읽어요.
//!
//! 경로 키가 lakehouse id 인 이유: 지도의 `complex` 레이어가 feature id 로 발행하는 값이 그것이고,
//! Catalog 의 `id` 와는 서로 계산되지 않는 **다른 식별자**예요. 둘을 섞으면 조회가 조용히 비므로
//! [`shared_kernel::lakehouse_complex_id::LakehouseComplexId`] 가 입구에서 형식을 거부해요.

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
