//! Building-register source identities shared by exporters and job planning.
use serde::{Deserialize, Serialize};

/// 건축물 원문 종류와 수집 원천 식별자의 공통 대응.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRole {
    /// 건물 표제부.
    Title,
    /// 층별 개요.
    Floor,
    /// 전유부.
    Unit,
    /// 기본 개요.
    Basis,
    /// 전유·공용 면적.
    UnitArea,
}

impl SourceRole {
    /// 구조 자료 선택에서 사용하는 모든 원문 종류.
    pub const ALL: [Self; 5] = [
        Self::Title,
        Self::Floor,
        Self::Unit,
        Self::Basis,
        Self::UnitArea,
    ];
    /// 원천 목록에 등록된 수집 식별자를 반환한다.
    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Title => "hubgokr__building_register_main",
            Self::Floor => "hubgokr__building_register_floor_overview",
            Self::Unit => "hubgokr__building_register_exclusive_unit",
            Self::Basis => "hubgokr__building_register_basis_outline",
            Self::UnitArea => "hubgokr__building_register_exclusive_common_area",
        }
    }
}
