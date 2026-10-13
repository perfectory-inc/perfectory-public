//! 건축물 원문 재시도 시각의 공통 계약. 원문 파일명의 날짜·공급 월·판 규칙은
//! [`crate::hub_provider_release`] 한 곳에 있다.

/// Full typed exports keep row timestamps fixed across attempts; manual exports default to now.
/// # Errors
/// 지정한 환경변수가 유효한 UTF-8 또는 RFC 3339 시각이 아니면 실패한다.
pub fn ingested_at_utc() -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    match std::env::var("FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC") {
        Ok(raw) => Ok(chrono::DateTime::parse_from_rfc3339(&raw)?.with_timezone(&chrono::Utc)),
        Err(std::env::VarError::NotPresent) => Ok(chrono::Utc::now()),
        Err(error) => Err(error.into()),
    }
}
