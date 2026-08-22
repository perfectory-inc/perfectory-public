//! `LakehouseComplexId` — Foundation Platform 이 발행한 산업단지 식별자 (`UUIDv5`).
//!
//! 산업단지에는 식별자가 **둘** 있고 서로 계산되지 않아요. Catalog 가 행을 만들 때 `Uuid::now_v7()`
//! 로 찍는 `id` 가 하나, Bronze→Silver 작업이 `UUIDv5` 로 유도하는 lakehouse `complex_id` 가
//! 다른 하나예요. 바깥으로 발행되는 쪽 — `complex` 벡터 타일의 feature id, Gold 산출물 객체 키 —
//! 은 후자이고, 공짱이 지도 클릭에서 손에 쥐는 값도 후자예요.
//!
//! 이 타입이 존재하는 이유는 그 둘을 섞어 보내는 실수를 **입구에서** 막기 위해서예요. 버전 자리를
//! `5` 로 고정하면 `now_v7()` 로 찍힌 Catalog `id` 는 형식 검사에서 거부돼요 — 조회 결과가 조용히
//! 비는 대신에요. 같은 불변식이 Foundation 쪽 DB 제약
//! (`industrial_complex_lakehouse_complex_id_is_uuid_v5`) 과
//! `serving_postgis.industrial_complex_boundary_publication` 의
//! `complex_boundary_publication_complex_id_is_uuid_v5` 에도 걸려 있어요.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 하이픈 포함 `UUID` 문자열 길이.
const UUID_TEXT_LEN: usize = 36;
/// 하이픈이 오는 자리 (0-based).
const HYPHEN_POSITIONS: [usize; 4] = [8, 13, 18, 23];
/// `UUID` 버전 니블 자리 (0-based).
const VERSION_POSITION: usize = 14;
/// `UUID` variant 니블 자리 (0-based).
const VARIANT_POSITION: usize = 19;

/// Foundation Platform lakehouse 산업단지 식별자.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LakehouseComplexId(String);

/// [`LakehouseComplexId`] 검증 에러.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LakehouseComplexIdError {
    /// 하이픈 포함 36자가 아님.
    #[error("lakehouse complex id must be 36 characters, got {actual}")]
    InvalidLength {
        /// 실제 길이 (바이트 단위, `str::len`).
        actual: usize,
    },
    /// 소문자 16진수 또는 하이픈 배치가 표준 `UUID` 형식이 아님.
    #[error("lakehouse complex id must be a lowercase hyphenated UUID")]
    NotLowercaseUuid,
    /// 버전 니블이 `5` 가 아님 — Catalog 의 `now_v7()` id 를 보냈을 때 여기서 걸려요.
    #[error("lakehouse complex id must be UUIDv5, got version nibble '{actual}'")]
    NotVersion5 {
        /// 실제 버전 니블.
        actual: char,
    },
    /// RFC 4122 variant 니블이 `8`/`9`/`a`/`b` 가 아님.
    #[error("lakehouse complex id must use the RFC 4122 variant, got '{actual}'")]
    InvalidVariant {
        /// 실제 variant 니블.
        actual: char,
    },
}

impl LakehouseComplexId {
    /// 검증 후 [`LakehouseComplexId`] 생성.
    ///
    /// # Errors
    ///
    /// - 36자가 아니면 [`LakehouseComplexIdError::InvalidLength`]
    /// - 소문자 16진수·하이픈 배치가 아니면 [`LakehouseComplexIdError::NotLowercaseUuid`]
    /// - 버전 니블이 `5` 가 아니면 [`LakehouseComplexIdError::NotVersion5`]
    /// - variant 니블이 `8`/`9`/`a`/`b` 가 아니면 [`LakehouseComplexIdError::InvalidVariant`]
    pub fn try_new(s: &str) -> Result<Self, LakehouseComplexIdError> {
        if s.len() != UUID_TEXT_LEN {
            return Err(LakehouseComplexIdError::InvalidLength { actual: s.len() });
        }
        let bytes = s.as_bytes();
        for (index, byte) in bytes.iter().enumerate() {
            let character = char::from(*byte);
            let expected_hyphen = HYPHEN_POSITIONS.contains(&index);
            if expected_hyphen {
                if character != '-' {
                    return Err(LakehouseComplexIdError::NotLowercaseUuid);
                }
            } else if !character.is_ascii_digit() && !('a'..='f').contains(&character) {
                return Err(LakehouseComplexIdError::NotLowercaseUuid);
            }
        }
        let version = char::from(bytes[VERSION_POSITION]);
        if version != '5' {
            return Err(LakehouseComplexIdError::NotVersion5 { actual: version });
        }
        let variant = char::from(bytes[VARIANT_POSITION]);
        if !matches!(variant, '8' | '9' | 'a' | 'b') {
            return Err(LakehouseComplexIdError::InvalidVariant { actual: variant });
        }
        Ok(Self(s.to_owned()))
    }

    /// 내부 `UUID` 문자열.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LakehouseComplexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    /// 산업단지 코드 `111010` 의 실제 lakehouse id 형태 (migration
    /// `20260818082317` 주석이 `7df3859c-…-51fa-…` 라고 적어 둔 그 값의 형식).
    const VALID: &str = "7df3859c-1e0a-51fa-8b7d-9a1c2e3f4a5b";

    #[test]
    fn accepts_a_lowercase_uuid_v5() {
        assert_eq!(LakehouseComplexId::try_new(VALID).unwrap().as_str(), VALID);
    }

    #[test]
    fn rejects_the_catalog_id_space() {
        // `Uuid::now_v7()` 모양. 이 값을 lakehouse 조회에 보내면 조용히 404 가 나는 대신
        // 여기서 거부돼야 해요 — 두 식별자를 섞은 것이 원인이라는 사실이 응답에 남도록.
        let now_v7 = "01a0136d-2b3c-7e61-8f90-a1b2c3d4e5f6";
        assert_eq!(
            LakehouseComplexId::try_new(now_v7),
            Err(LakehouseComplexIdError::NotVersion5 { actual: '7' })
        );
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(matches!(
            LakehouseComplexId::try_new("7df3859c-1e0a-51fa-8b7d-9a1c2e3f4a5"),
            Err(LakehouseComplexIdError::InvalidLength { actual: 35 })
        ));
    }

    #[test]
    fn rejects_uppercase_hex() {
        assert_eq!(
            LakehouseComplexId::try_new("7DF3859C-1E0A-51FA-8B7D-9A1C2E3F4A5B"),
            Err(LakehouseComplexIdError::NotLowercaseUuid)
        );
    }

    #[test]
    fn rejects_misplaced_hyphens() {
        assert_eq!(
            LakehouseComplexId::try_new("7df3859c1-e0a-51fa-8b7d-9a1c2e3f4a5b"),
            Err(LakehouseComplexIdError::NotLowercaseUuid)
        );
    }

    #[test]
    fn rejects_non_rfc4122_variant() {
        assert_eq!(
            LakehouseComplexId::try_new("7df3859c-1e0a-51fa-cb7d-9a1c2e3f4a5b"),
            Err(LakehouseComplexIdError::InvalidVariant { actual: 'c' })
        );
    }
}
