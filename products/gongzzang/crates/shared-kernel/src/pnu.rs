//! `PNU` — 19자리 한국 필지 식별자.
//!
//! 형식: `[시도2][시군구3][읍면동3][리/통2][산여부1][본번4][부번4]` = 19자리.
//!
//! 예: `"9999900101100010000"` = 공개 테스트용 예약 PNU, 본번 1, 부번 0, 일반.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 한국 필지 식별자 (`PNU`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Pnu(String);

/// `Pnu` 검증 에러.
#[derive(Debug, Error)]
pub enum PnuError {
    /// 19자리가 아님.
    #[error("PNU must be 19 digits, got {actual}")]
    InvalidLength {
        /// 실제 길이 (바이트 단위, `str::len`).
        actual: usize,
    },
    /// `ASCII` 숫자가 아닌 문자 포함.
    #[error("PNU must contain only ASCII digits")]
    NonDigit,
    /// 11번째 대장구분 자리가 표준 지적 코드가 아님.
    #[error("PNU cadastral register digit must be one of 1/2/8/9, got {actual}")]
    InvalidDaejangDigit {
        /// 실제 대장구분 자리.
        actual: char,
    },
}

impl Pnu {
    /// 검증 후 `Pnu` 생성.
    ///
    /// # Errors
    ///
    /// - 19자리가 아니면 [`PnuError::InvalidLength`]
    /// - `ASCII` 숫자 외 문자 포함 시 [`PnuError::NonDigit`]
    pub fn try_new(s: &str) -> Result<Self, PnuError> {
        if s.len() != 19 {
            return Err(PnuError::InvalidLength { actual: s.len() });
        }
        if !s.chars().all(|c| c.is_ascii_digit()) {
            return Err(PnuError::NonDigit);
        }
        let daejang_digit = s.as_bytes()[10] as char;
        if !matches!(daejang_digit, '1' | '2' | '8' | '9') {
            return Err(PnuError::InvalidDaejangDigit {
                actual: daejang_digit,
            });
        }
        Ok(Self(s.to_owned()))
    }

    /// 내부 19자리 문자열.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 시도 코드 (2자리).
    #[must_use]
    pub fn sido_code(&self) -> &str {
        &self.0[0..2]
    }

    /// 시군구 코드 (5자리, 시도 포함).
    #[must_use]
    pub fn sigungu_code(&self) -> &str {
        &self.0[0..5]
    }

    /// 읍면동 코드 (8자리, 시도+시군구 포함).
    #[must_use]
    pub fn eupmyeondong_code(&self) -> &str {
        &self.0[0..8]
    }

    /// 산 여부 (`2`=임야대장, `9`=폐쇄 임야대장).
    #[must_use]
    pub fn is_san(&self) -> bool {
        matches!(&self.0[10..11], "2" | "9")
    }

    /// 본번 (chars 11-14, 4자리 정수).
    ///
    /// # Panics
    ///
    /// `try_new` 검증을 통과한 `Pnu`만 생성되므로, chars 11-14는 항상 4자리
    /// `ASCII` 숫자예요. `parse::<u32>()`는 절대 실패하지 않아요.
    #[must_use]
    #[allow(clippy::expect_used)] // see # Panics
    pub fn jibun_main(&self) -> u32 {
        self.0[11..15].parse().expect("digits validated by try_new")
    }

    /// 부번 (chars 15-18, 4자리 정수).
    ///
    /// # Panics
    ///
    /// `jibun_main`과 동일 — `try_new` 검증으로 항상 4자리 `ASCII` 숫자예요.
    #[must_use]
    #[allow(clippy::expect_used)] // see # Panics
    pub fn jibun_sub(&self) -> u32 {
        self.0[15..19].parse().expect("digits validated by try_new")
    }
}

impl std::fmt::Display for Pnu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for Pnu {
    type Err = PnuError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    const SYNTHETIC_PNU: &str = "9999900101100010000";
    const STANDARD_PNU_PATTERN: &str = "^[0-9]{10}[1289][0-9]{8}$";

    #[test]
    fn consumer_contract_pin_uses_the_standard_pnu_pattern() {
        let contract: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json"
        ))
        .expect("valid Foundation Platform contract pin");

        assert_eq!(contract["schemas"]["pnu"]["pattern"], STANDARD_PNU_PATTERN);
    }

    #[test]
    fn parse_valid_pnu() {
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid PNU");
        assert_eq!(pnu.as_str(), SYNTHETIC_PNU);
    }

    #[test]
    fn extracts_sido_code() {
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid");
        assert_eq!(pnu.sido_code(), "99");
    }

    #[test]
    fn extracts_sigungu_code() {
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid");
        assert_eq!(pnu.sigungu_code(), "99999");
    }

    #[test]
    fn extracts_eupmyeondong_code() {
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid");
        assert_eq!(pnu.eupmyeondong_code(), "99999001");
    }

    #[test]
    fn jibun_main_extracts_4_digits() {
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid");
        assert_eq!(pnu.jibun_main(), 1);
    }

    #[test]
    fn jibun_sub_extracts_4_digits() {
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid");
        assert_eq!(pnu.jibun_sub(), 0);
    }

    #[test]
    fn is_san_false_for_normal_parcel() {
        // char 10 of SYNTHETIC_PNU is '1' (일반)
        let pnu = Pnu::try_new(SYNTHETIC_PNU).expect("valid");
        assert!(!pnu.is_san());
    }

    #[test]
    fn is_san_true_when_char_10_is_2() {
        // Mountain parcel: same prefix, but char 10 = '2'
        let mountain = "9999900101200010000";
        assert_eq!(mountain.len(), 19);
        let pnu = Pnu::try_new(mountain).expect("valid");
        assert!(pnu.is_san());
    }

    #[test]
    fn is_san_true_for_closed_forest_register() {
        let closed_forest = "9999900101900010000";
        let pnu = Pnu::try_new(closed_forest).expect("valid closed forest PNU");
        assert!(pnu.is_san());
    }

    #[test]
    fn rejects_hub_register_dialect_digit() {
        assert!(Pnu::try_new("9999900101000010000").is_err());
    }

    #[test]
    fn rejects_too_short() {
        let err = Pnu::try_new("123").unwrap_err();
        assert!(matches!(err, PnuError::InvalidLength { actual: 3 }));
    }

    #[test]
    fn rejects_too_long() {
        let err = Pnu::try_new("12345678901234567890").unwrap_err();
        assert!(matches!(err, PnuError::InvalidLength { actual: 20 }));
    }

    #[test]
    fn rejects_non_digit_letter() {
        let err = Pnu::try_new("99999001011000100AB").unwrap_err();
        assert!(matches!(err, PnuError::NonDigit));
    }

    #[test]
    fn rejects_non_digit_unicode() {
        // 한글 char는 UTF-8에서 3바이트 → str::len()이 19와 달라 InvalidLength로 떨어져요.
        // 어느 variant든 에러이기만 하면 OK.
        let err = Pnu::try_new("99999001011000100가나").unwrap_err();
        assert!(matches!(
            err,
            PnuError::InvalidLength { .. } | PnuError::NonDigit
        ));
    }

    #[test]
    fn display_renders_inner() {
        use std::str::FromStr;
        let pnu = Pnu::from_str(SYNTHETIC_PNU).expect("valid");
        assert_eq!(format!("{pnu}"), SYNTHETIC_PNU);
    }

    #[test]
    fn from_str_roundtrips() {
        use std::str::FromStr;
        let pnu = Pnu::from_str(SYNTHETIC_PNU).expect("valid");
        assert_eq!(pnu.as_str(), SYNTHETIC_PNU);
    }
}
