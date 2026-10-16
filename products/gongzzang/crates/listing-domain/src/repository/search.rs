use shared_kernel::id::{Id, UserMarker};
use shared_kernel::land_use_type::LandUseType;
use shared_kernel::listing_type::ListingType;
use shared_kernel::pnu::Pnu;
use shared_kernel::transaction_type::TransactionType;

/// Card list search query.
#[derive(Debug, Clone)]
pub struct CardSearchQuery {
    /// Exact PNU filter.
    pub pnu: Option<Pnu>,
    /// Administrative code prefix filter.
    pub admin_code_prefix: Option<String>,
    /// Land use type filter.
    pub land_use_type: Option<LandUseType>,
    /// Listing type filter.
    pub types: Option<Vec<ListingType>>,
    /// Transaction type filter.
    pub transactions: Option<Vec<TransactionType>>,
    /// Minimum area in square meters.
    pub min_area_m2: Option<f64>,
    /// Maximum area in square meters.
    pub max_area_m2: Option<f64>,
    /// Minimum price in KRW.
    pub min_price_krw: Option<i64>,
    /// Maximum price in KRW.
    pub max_price_krw: Option<i64>,
    /// Zero-based page number.
    pub page: u32,
    /// Page size.
    pub size: u32,
    /// Sort order.
    pub sort: CardSearchSort,
    /// Viewer id used for bookmark joins.
    pub viewer_user_id: Id<UserMarker>,
}

/// Card list sort order. The `snake_case` names are the `sort` query values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CardSearchSort {
    /// Newest listings first.
    #[default]
    CreatedAtDesc,
    /// Lowest price first.
    PriceAsc,
    /// Highest price first.
    PriceDesc,
    /// Smallest area first.
    AreaAsc,
    /// Largest area first.
    AreaDesc,
}

impl CardSearchSort {
    /// Every value, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::CreatedAtDesc,
        Self::PriceAsc,
        Self::PriceDesc,
        Self::AreaAsc,
        Self::AreaDesc,
    ];

    /// The `sort` query value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreatedAtDesc => "created_at_desc",
            Self::PriceAsc => "price_asc",
            Self::PriceDesc => "price_desc",
            Self::AreaAsc => "area_asc",
            Self::AreaDesc => "area_desc",
        }
    }
}
