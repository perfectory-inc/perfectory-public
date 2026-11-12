//! The 부동산중개업 lanes of the land-attribute export (root ADR-0511 §4): the national AL_D171
//! register of brokerage offices and the AL_D172 roster of their people. Same reader, same
//! handoff; only the layout below is theirs.

use lakehouse_domain::{SILVER_REAL_ESTATE_BROKER_MEMBER, SILVER_REAL_ESTATE_BROKER_OFFICE};

use super::{run_lane, Lane};

// The national AL_D171 CSV of the 부동산중개업 source; its AL_D172 (members) and AL_D170 (SHP)
// siblings are other datasets of the same prefix (root ADR-0511 §4). Quoted fields carry commas
// in names and addresses (47 rows of the 2026-11-09 file split wrong on a bare comma); the RFC 4180
// reader keeps them whole.
pub(super) const REAL_ESTATE_BROKER_OFFICE_LANE: Lane = Lane {
    env_prefix: "FOUNDATION_PLATFORM_REAL_ESTATE_BROKER_OFFICE",
    contract: &SILVER_REAL_ESTATE_BROKER_OFFICE,
    expected_header: &[
        "법정동코드",
        "법정동명",
        "등록번호",
        "사업자상호",
        "중개업자명",
        "상태구분코드",
        "상태구분명",
        "등록일자",
        "전화번호",
        "보증설정시작일",
        "보증설정종료일",
        "데이터기준일자",
        "지번주소",
        "도로명주소",
        "도로명주소코드",
    ],
    csv_columns: &[
        Some("sigungu_code"),
        Some("sigungu_name"),
        Some("registration_no"),
        Some("office_name"),
        Some("representative_name"),
        Some("status_code"),
        Some("status_name"),
        Some("registered_on"),
        Some("phone_number"),
        Some("guarantee_start_on"),
        Some("guarantee_end_on"),
        Some("data_reference_date"),
        Some("jibun_address"),
        Some("road_address"),
        Some("road_address_code"),
    ],
    member_matches: |name| name.starts_with("AL_D171_") && name.ends_with(".csv"),
    pnu_position: None,
    positive_area: false,
    delimiter: None,
};

// The national AL_D172 roster: every 공인중개사, 중개보조원, 중개인 and 법인 registered to an
// office, with their 자격증번호 where they hold one (root ADR-0511 §4).
pub(super) const REAL_ESTATE_BROKER_MEMBER_LANE: Lane = Lane {
    env_prefix: "FOUNDATION_PLATFORM_REAL_ESTATE_BROKER_MEMBER",
    contract: &SILVER_REAL_ESTATE_BROKER_MEMBER,
    expected_header: &[
        "법정동코드",
        "법정동명",
        "등록번호",
        "사업자상호",
        "중개업자명",
        "중개업자종별코드",
        "중개업자종별명",
        "자격증번호",
        "자격증취득일",
        "직위구분코드",
        "직위구분명",
        "데이터기준일자",
    ],
    csv_columns: &[
        Some("sigungu_code"),
        Some("sigungu_name"),
        Some("registration_no"),
        Some("office_name"),
        Some("member_name"),
        Some("member_kind_code"),
        Some("member_kind_name"),
        Some("license_no"),
        Some("license_acquired_on"),
        Some("position_code"),
        Some("position_name"),
        Some("data_reference_date"),
    ],
    member_matches: |name| name.starts_with("AL_D172_") && name.ends_with(".csv"),
    pnu_position: None,
    positive_area: false,
    delimiter: None,
};

/// Exports every office's roster through the shared handoff transport (root ADR-0511 §4).
///
/// # Errors
/// Refuses header drift, ambiguous members, and failed IO.
pub async fn run_real_estate_broker_member() -> anyhow::Result<()> {
    run_lane(&REAL_ESTATE_BROKER_MEMBER_LANE).await
}

/// Exports every registered brokerage office through the shared handoff transport (root
/// ADR-0511 §4).
///
/// # Errors
/// Refuses header drift, ambiguous members, and failed IO.
pub async fn run_real_estate_broker_office() -> anyhow::Result<()> {
    run_lane(&REAL_ESTATE_BROKER_OFFICE_LANE).await
}
