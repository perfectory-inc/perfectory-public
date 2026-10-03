//! Parcel Number Unit value object.
//!
//! Foundation Platform treats PNU as a canonical 19-digit parcel identity string. Keeping this as a
//! validated value object prevents downstream services from mixing arbitrary location strings
//! with parcel identifiers.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Validated 19-digit Parcel Number Unit.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Pnu(String);

/// Validation errors returned while parsing a PNU.
#[derive(Debug, Error)]
pub enum PnuError {
    /// Input length was not exactly 19 characters.
    #[error("PNU must be exactly 19 digits, got {0}")]
    InvalidLength(usize),
    /// Input contained a non-digit character.
    #[error("PNU allows only ASCII digits, got {0:?}")]
    NonDigit(char),
    /// The 대장구분 digit (position 11) is outside the standard cadastral code
    /// table (1=토지대장, 2=임야대장, 8/9=폐쇄대장). Hub-register dialect values
    /// (0=대지 등) must be converted before they reach a `Pnu` (ADR 0023).
    #[error("PNU 대장구분 digit must be one of 1/2/8/9, got {0:?}")]
    InvalidDaejangDigit(char),
}

impl Pnu {
    /// Parses and validates a PNU.
    ///
    /// # Errors
    /// Returns `PnuError::InvalidLength` when input is not 19 characters, or
    /// `PnuError::NonDigit` when it contains a non-digit character.
    pub fn parse(input: impl Into<String>) -> Result<Self, PnuError> {
        let raw: String = input.into();
        if raw.len() != 19 {
            return Err(PnuError::InvalidLength(raw.len()));
        }
        if let Some(c) = raw.chars().find(|c| !c.is_ascii_digit()) {
            return Err(PnuError::NonDigit(c));
        }
        let daejang = raw.as_bytes()[10] as char;
        if !matches!(daejang, '1' | '2' | '8' | '9') {
            return Err(PnuError::InvalidDaejangDigit(daejang));
        }
        Ok(Self(raw))
    }

    /// Returns the canonical 19-digit PNU string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the two-digit province or metropolitan-city code.
    #[must_use]
    pub fn sido_code(&self) -> &str {
        &self.0[0..2]
    }

    /// Returns the five-digit city, county, or district code.
    #[must_use]
    pub fn sigungu_code(&self) -> &str {
        &self.0[0..5]
    }

    /// Returns the ten-digit legal-dong code.
    #[must_use]
    pub fn bjdong_code(&self) -> &str {
        &self.0[0..10]
    }

    /// Returns the zero-padded four-digit main lot number.
    #[must_use]
    pub fn bonbun(&self) -> &str {
        &self.0[11..15]
    }

    /// Returns the zero-padded four-digit sub lot number.
    #[must_use]
    pub fn bubun(&self) -> &str {
        &self.0[15..19]
    }
}

impl TryFrom<String> for Pnu {
    type Error = PnuError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<Pnu> for String {
    fn from(pnu: Pnu) -> Self {
        pnu.0
    }
}

/// Composes a **standard** PNU from hub building-register address code columns.
///
/// The hub register 대지구분코드 (`0`=대지, `1`=산, `2`=블록) is a different code
/// table from the standard cadastral PNU digit (`1`=일반, `2`=산) — ADR 0023.
/// Block parcels (`2`) and unknown codes have no standard PNU and yield `None`;
/// fabricating a lot number for them is forbidden.
#[must_use]
pub fn standard_pnu_from_hub_register_codes(
    sigungu: &str,
    bjdong: &str,
    daeji_kind: &str,
    bon: &str,
    bu: &str,
) -> Option<String> {
    let standard_digit = match daeji_kind.trim() {
        "0" => '1',
        "1" => '2',
        _ => return None,
    };
    Some(format!(
        "{:0>5}{:0>5}{standard_digit}{:0>4}{:0>4}",
        sigungu.trim(),
        bjdong.trim(),
        bon.trim(),
        bu.trim(),
    ))
}

/// Hub 시군구 crosswalk (current → superseded) and the merged 시도 it governs.
///
/// Geography identity Wave 1 (ADR-0103): the hub register feed carries the
/// authority-current merged 시군구 code (12xxx, 전남광주통합특별시) while the
/// cadastral map still keys parcels by the superseded codes (29xxx 광주 /
/// 46xxx 전남). The kernel stays pure — the mapping is passed in — and any
/// code outside a governed 시도 passes through unchanged (identity).
///
/// A code *inside* a governed 시도 with no mapping is refused, never passed
/// through or dropped: the merged code has no cadastral parcel, so either
/// outcome would silently orphan every building of that 시군구. The 2026-09-27
/// title snapshot lost the PNU of all 916,461 ordinary-land rows of 시도 12
/// because a changed resolver withheld the mapping without saying so.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SigunguCrosswalk {
    superseded_by_current: HashMap<String, String>,
    governed_sido: BTreeSet<String>,
}

/// Errors raised while building or applying a [`SigunguCrosswalk`].
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SigunguCrosswalkError {
    /// A governed 시도 code is not two ASCII digits.
    #[error("governed 시도 code must be 2 digits, got {0:?}")]
    InvalidSidoCode(String),
    /// A mapping's current code lies outside every governed 시도, so nothing declares why it exists.
    #[error("crosswalk maps {0} but no governed 시도 covers it")]
    UngovernedMapping(String),
    /// A hub 시군구 code inside a governed 시도 has no mapping.
    #[error("hub 시군구 {0} belongs to a merged 시도 but the crosswalk has no mapping for it")]
    UnmappedGovernedSigungu(String),
}

impl SigunguCrosswalk {
    /// A crosswalk that governs nothing: every code composes as-is.
    #[must_use]
    pub fn identity() -> Self {
        Self::default()
    }

    /// Builds a crosswalk from `current_code → superseded_code` pairs and the
    /// 2-digit current 시도 codes whose every 시군구 must be mapped.
    ///
    /// # Errors
    /// Fails when a 시도 code is not 2 digits or a mapping falls outside every governed 시도.
    pub fn new(
        superseded_by_current: HashMap<String, String>,
        governed_sido: impl IntoIterator<Item = String>,
    ) -> Result<Self, SigunguCrosswalkError> {
        let governed_sido = governed_sido.into_iter().collect::<BTreeSet<_>>();
        if let Some(code) = governed_sido
            .iter()
            .find(|code| code.len() != 2 || !code.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(SigunguCrosswalkError::InvalidSidoCode(code.clone()));
        }
        if let Some(code) = superseded_by_current
            .keys()
            .find(|code| !governed_sido.contains(code.get(..2).unwrap_or_default()))
        {
            return Err(SigunguCrosswalkError::UngovernedMapping(code.clone()));
        }
        Ok(Self {
            superseded_by_current,
            governed_sido,
        })
    }

    /// The superseded code a current code maps to, if any.
    #[must_use]
    pub fn superseded_code(&self, current_code: &str) -> Option<&str> {
        self.superseded_by_current
            .get(current_code.trim())
            .map(String::as_str)
    }

    /// Number of mapped current codes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.superseded_by_current.len()
    }

    /// Whether the crosswalk maps no code.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.superseded_by_current.is_empty()
    }

    /// Resolves a hub 시군구 code to the code the cadastral map keys parcels by.
    ///
    /// # Errors
    /// Fails when the code lies inside a governed 시도 and has no mapping.
    pub fn resolve<'a>(&'a self, sigungu: &'a str) -> Result<&'a str, SigunguCrosswalkError> {
        let trimmed = sigungu.trim();
        if let Some(superseded) = self.superseded_by_current.get(trimmed) {
            return Ok(superseded);
        }
        let governed = trimmed.len() == 5
            && trimmed.bytes().all(|b| b.is_ascii_digit())
            && self.governed_sido.contains(&trimmed[..2]);
        if governed {
            return Err(SigunguCrosswalkError::UnmappedGovernedSigungu(
                trimmed.to_owned(),
            ));
        }
        Ok(trimmed)
    }
}

/// Composes a standard PNU from hub register codes, normalizing the 시군구
/// code through `crosswalk` first (see [`SigunguCrosswalk::resolve`]).
///
/// Behaves exactly like [`standard_pnu_from_hub_register_codes`] for every
/// 시군구 code outside a governed 시도. Never fabricates a PNU: block (`2`)
/// and unknown 대지구분 codes still yield `None` (ADR 0023).
///
/// # Errors
/// Fails when the 시군구 code lies inside a governed 시도 and has no mapping.
pub fn standard_pnu_from_hub_register_codes_via(
    crosswalk: &SigunguCrosswalk,
    sigungu: &str,
    bjdong: &str,
    daeji_kind: &str,
    bon: &str,
    bu: &str,
) -> Result<Option<String>, SigunguCrosswalkError> {
    Ok(standard_pnu_from_hub_register_codes(
        crosswalk.resolve(sigungu)?,
        bjdong,
        daeji_kind,
        bon,
        bu,
    ))
}

/// Composes the hub-native register parcel key from the same columns.
///
/// This is the raw hub composition (대지구분 code kept as-is). It is **not** a
/// PNU: it exists so register-internal joins (전유부↔표제부, scope keys) keep a
/// total key even for block parcels whose standard PNU is `None` (ADR 0023).
#[must_use]
pub fn hub_register_parcel_key(
    sigungu: &str,
    bjdong: &str,
    daeji_kind: &str,
    bon: &str,
    bu: &str,
) -> String {
    format!(
        "{:0>5}{:0>5}{:0>1}{:0>4}{:0>4}",
        sigungu.trim(),
        bjdong.trim(),
        daeji_kind.trim(),
        bon.trim(),
        bu.trim(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        hub_register_parcel_key, standard_pnu_from_hub_register_codes,
        standard_pnu_from_hub_register_codes_via, Pnu, PnuError, SigunguCrosswalk,
        SigunguCrosswalkError,
    };
    use std::collections::HashMap;

    fn seed_crosswalk() -> SigunguCrosswalk {
        SigunguCrosswalk::new(
            HashMap::from([
                ("12240".to_owned(), "29140".to_owned()),
                ("12190".to_owned(), "46230".to_owned()),
            ]),
            ["12".to_owned()],
        )
        .unwrap_or_default()
    }

    #[test]
    fn parses_19_digit_pnu() -> Result<(), PnuError> {
        let pnu = Pnu::parse("9999900101100010001")?;
        assert_eq!(pnu.as_str(), "9999900101100010001");
        Ok(())
    }

    #[test]
    fn exposes_canonical_location_and_lot_components() -> Result<(), PnuError> {
        let pnu = Pnu::parse("9999938029105800001")?;

        assert_eq!(pnu.sido_code(), "99");
        assert_eq!(pnu.sigungu_code(), "99999");
        assert_eq!(pnu.bjdong_code(), "9999938029");
        assert_eq!(pnu.bonbun(), "0580");
        assert_eq!(pnu.bubun(), "0001");
        Ok(())
    }

    #[test]
    fn composes_standard_pnu_from_hub_register_codes() {
        // 허브 대지구분 0(대지) → 표준 1(일반)
        assert_eq!(
            standard_pnu_from_hub_register_codes("99999", "01101", "0", "0734", "0000").as_deref(),
            Some("9999901101107340000")
        );
        // 허브 1(산) → 표준 2(산)
        assert_eq!(
            standard_pnu_from_hub_register_codes("99999", "01201", "1", "0508", "0123").as_deref(),
            Some("9999901201205080123")
        );
        // zero-padding은 기존 조립과 동일
        assert_eq!(
            standard_pnu_from_hub_register_codes("99999", "00101", "0", "8", "16").as_deref(),
            Some("9999900101100080016")
        );
    }

    #[test]
    fn block_and_unknown_daeji_kinds_have_no_standard_pnu() {
        // 허브 2(블록): 지적 지번이 아니므로 표준 PNU 없음 — 날조 금지 (ADR 0023)
        assert_eq!(
            standard_pnu_from_hub_register_codes("99999", "00901", "2", "0529", "0000"),
            None
        );
        for daeji in ["", "3", "9", "-"] {
            assert_eq!(
                standard_pnu_from_hub_register_codes("99999", "00101", daeji, "0001", "0000"),
                None,
                "daeji={daeji}"
            );
        }
    }

    #[test]
    fn crosswalk_maps_merged_sigungu_to_superseded_cadastral_code() {
        let crosswalk = seed_crosswalk();
        assert_eq!(crosswalk.len(), 2);
        // 통합 현행 12240 → 지적도 29140: 크로스워크 경유가 29140 직접 조립과 동일
        assert_eq!(
            standard_pnu_from_hub_register_codes_via(
                &crosswalk, "12240", "01101", "0", "0734", "0000"
            ),
            Ok(standard_pnu_from_hub_register_codes(
                "29140", "01101", "0", "0734", "0000"
            )),
        );
        // 비산술 사례: 12190 → 46230
        assert_eq!(
            standard_pnu_from_hub_register_codes_via(
                &crosswalk, "12190", "01201", "1", "0508", "0123"
            ),
            Ok(standard_pnu_from_hub_register_codes(
                "46230", "01201", "1", "0508", "0123"
            )),
        );
    }

    #[test]
    fn crosswalk_leaves_ungoverned_sigungu_byte_identical() {
        let crosswalk = seed_crosswalk();
        // 통합되지 않은 시도의 코드는 그대로 통과 (identity)
        assert_eq!(crosswalk.resolve("26110"), Ok("26110"));
        assert_eq!(
            standard_pnu_from_hub_register_codes_via(&crosswalk, "26110", "00101", "0", "8", "16"),
            Ok(standard_pnu_from_hub_register_codes(
                "26110", "00101", "0", "8", "16"
            )),
        );
    }

    #[test]
    fn an_unmapped_code_inside_a_merged_sido_is_refused_not_passed_through(
    ) -> Result<(), SigunguCrosswalkError> {
        // 심은 위반: 통합 시도 99 를 다스린다고 선언했지만 99990 의 짝이 없다.
        let crosswalk = SigunguCrosswalk::new(
            HashMap::from([("99110".to_owned(), "99999".to_owned())]),
            ["99".to_owned()],
        )?;
        assert_eq!(
            standard_pnu_from_hub_register_codes_via(
                &crosswalk, "99110", "00101", "0", "0001", "0000"
            ),
            Ok(Some("9999900101100010000".to_owned()))
        );
        // 대지(0)든 산(1)이든 블록(2)이든 같은 거부: 시도 하나가 통째로 NULL 이 되는 길이 없다.
        for daeji in ["0", "1", "2"] {
            assert_eq!(
                standard_pnu_from_hub_register_codes_via(
                    &crosswalk, "99990", "00101", daeji, "0001", "0000"
                ),
                Err(SigunguCrosswalkError::UnmappedGovernedSigungu(
                    "99990".to_owned()
                )),
                "daeji={daeji}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_mapping_outside_every_governed_sido_is_refused_at_build() {
        assert_eq!(
            SigunguCrosswalk::new(
                HashMap::from([("98110".to_owned(), "97110".to_owned())]),
                ["99".to_owned()],
            ),
            Err(SigunguCrosswalkError::UngovernedMapping("98110".to_owned()))
        );
        assert_eq!(
            SigunguCrosswalk::new(HashMap::new(), ["9".to_owned()]),
            Err(SigunguCrosswalkError::InvalidSidoCode("9".to_owned()))
        );
    }

    #[test]
    fn crosswalk_never_fabricates_pnu_for_block_or_unknown_daeji() {
        let crosswalk = seed_crosswalk();
        // 매핑 대상 시군구여도 블록(2)·미지 대지구분은 여전히 None (ADR 0023)
        for daeji in ["2", "", "3", "9", "-"] {
            assert_eq!(
                standard_pnu_from_hub_register_codes_via(
                    &crosswalk, "12240", "00901", daeji, "0529", "0000"
                ),
                Ok(None),
                "daeji={daeji}"
            );
        }
    }

    #[test]
    fn identity_crosswalk_matches_identity_composition() {
        let identity = SigunguCrosswalk::identity();
        assert!(identity.is_empty());
        for (sigungu, bjdong, daeji, bon, bu) in [
            ("99999", "01101", "0", "0734", "0000"),
            ("12240", "01101", "0", "0734", "0000"),
            ("26110", "00101", "0", "8", "16"),
            ("99999", "00901", "2", "0529", "0000"),
        ] {
            assert_eq!(
                standard_pnu_from_hub_register_codes_via(
                    &identity, sigungu, bjdong, daeji, bon, bu
                ),
                Ok(standard_pnu_from_hub_register_codes(
                    sigungu, bjdong, daeji, bon, bu
                )),
                "sigungu={sigungu} daeji={daeji}"
            );
        }
    }

    #[test]
    fn hub_register_parcel_key_keeps_raw_hub_composition() {
        // 내부 조인 전용 키: 허브 코드 그대로 (PNU 아님)
        assert_eq!(
            hub_register_parcel_key("99999", "01101", "0", "0734", "0000"),
            "9999901101007340000"
        );
        assert_eq!(
            hub_register_parcel_key("99999", "00901", "2", "0529", "0000"),
            "9999900901205290000"
        );
    }

    #[test]
    fn rejects_short_input() {
        assert!(matches!(
            Pnu::parse("12345"),
            Err(PnuError::InvalidLength(5))
        ));
    }

    #[test]
    fn rejects_non_digit() {
        assert!(matches!(
            Pnu::parse("999990010110001000A"),
            Err(PnuError::NonDigit('A'))
        ));
    }

    #[test]
    fn rejects_hub_dialect_daejang_digit() {
        // 표준 대장구분은 1(토지)/2(임야)/8·9(폐쇄대장)뿐 — 허브 사투리 0은
        // 파서 레벨에서 차단해 재유입을 막는다 (ADR 0023).
        assert!(matches!(
            Pnu::parse("9999900101000010001"),
            Err(PnuError::InvalidDaejangDigit('0'))
        ));
        for pnu in [
            "9999900101100010001",
            "9999900101200010001",
            "9999900101800010001",
            "9999900101900010001",
        ] {
            assert!(Pnu::parse(pnu).is_ok(), "{pnu}");
        }
    }
}
