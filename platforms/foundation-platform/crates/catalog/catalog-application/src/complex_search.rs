//! Paged industrial-complex search: the query shape and the values that bound it.
//!
//! The types here exist so the bounds are enforced by construction rather than by a check somewhere
//! in a handler. [`ComplexSearchPaging`] cannot hold a page size outside `1..=100`, and
//! [`ComplexSearchText`] cannot hold a pattern whose wildcards came from the caller — so a route
//! that forgets to validate does not build a query that skips the bound, it fails to build one.

use catalog_domain::IndustrialComplexStatus;
use thiserror::Error;

/// Page size served when the caller states none.
///
/// Same default as the Gongzzang listing search (`routes/listings/search.rs`), because a caller
/// paging two collections on one screen should not have to remember two defaults.
pub const DEFAULT_PAGE_SIZE: u32 = 20;

/// Largest page size a caller may ask for.
///
/// Also the same bound as the listing search. What it prevents: one request asking for the whole
/// canonical table (1,448 rows today) plus a Gold-pointer lookup per row, on a route any
/// authenticated session can call as often as its rate limit allows.
pub const MAX_PAGE_SIZE: u32 = 100;

/// Two-digit province code length, per the 행정안전부 administrative code standard.
const SIDO_CODE_LEN: usize = 2;

/// Escape character paired with `LIKE ... ESCAPE` in the repository query.
const LIKE_ESCAPE: char = '\\';

/// A caller-supplied search word that has been proven to be a word.
///
/// Holds the trimmed text, never the pattern: the pattern is built by [`Self::contains_pattern`] so
/// that a `%` or `_` typed by a caller is matched literally instead of becoming a wildcard. A user
/// searching for `100_` means those four characters, not "100 followed by anything".
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComplexSearchText(String);

impl ComplexSearchText {
    /// Accepts a caller's search word after trimming.
    ///
    /// # Errors
    ///
    /// Returns [`ComplexSearchQueryError::BlankText`] when the word is empty or only whitespace.
    /// An empty `q` is not "match everything" — it is a caller that sent a parameter with nothing
    /// in it, and answering with the whole table makes `q=` and no `q` at all the same request.
    pub fn try_new(raw: &str) -> Result<Self, ComplexSearchQueryError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(ComplexSearchQueryError::BlankText);
        }
        Ok(Self(trimmed.to_owned()))
    }

    /// The trimmed word as the caller typed it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Builds the `%…%` pattern for a `LIKE`/`ILIKE` with `ESCAPE '\'`.
    ///
    /// The escape character is escaped by the same pass as the wildcards, so no later pass can
    /// double the escapes this one introduced.
    #[must_use]
    pub fn contains_pattern(&self) -> String {
        let mut pattern = String::with_capacity(self.0.len() + 2);
        pattern.push('%');
        for character in self.0.chars() {
            if matches!(character, LIKE_ESCAPE | '%' | '_') {
                pattern.push(LIKE_ESCAPE);
            }
            pattern.push(character);
        }
        pattern.push('%');
        pattern
    }
}

/// A province code the caller may filter by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SidoCodeFilter(String);

impl SidoCodeFilter {
    /// Accepts exactly two ASCII digits.
    ///
    /// # Errors
    ///
    /// Returns [`ComplexSearchQueryError::InvalidSidoCode`] for anything else. A three-digit value
    /// would match no row and read as "there are no complexes there", which is a different answer
    /// from "that is not a province code".
    pub fn try_new(raw: &str) -> Result<Self, ComplexSearchQueryError> {
        if raw.len() != SIDO_CODE_LEN || !raw.chars().all(|c| c.is_ascii_digit()) {
            return Err(ComplexSearchQueryError::InvalidSidoCode(raw.to_owned()));
        }
        Ok(Self(raw.to_owned()))
    }

    /// The two-digit code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A page request that is inside the served bounds by construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComplexSearchPaging {
    page: u32,
    size: u32,
}

impl ComplexSearchPaging {
    /// Accepts a zero-indexed page and a page size, applying the defaults for absent values.
    ///
    /// # Errors
    ///
    /// Returns [`ComplexSearchQueryError::PageSizeOutOfRange`] when the size is `0` or above
    /// [`MAX_PAGE_SIZE`]. Refused rather than clamped: a caller that asked for 100,000 rows and
    /// silently received 100 cannot tell a short page from the end of the collection.
    pub fn try_new(page: Option<u32>, size: Option<u32>) -> Result<Self, ComplexSearchQueryError> {
        let size = size.unwrap_or(DEFAULT_PAGE_SIZE);
        if size == 0 || size > MAX_PAGE_SIZE {
            return Err(ComplexSearchQueryError::PageSizeOutOfRange(size));
        }
        Ok(Self {
            page: page.unwrap_or(0),
            size,
        })
    }

    /// Zero-indexed page number.
    #[must_use]
    pub const fn page(self) -> u32 {
        self.page
    }

    /// Rows per page.
    #[must_use]
    pub const fn size(self) -> u32 {
        self.size
    }

    /// `LIMIT` value for the repository query.
    #[must_use]
    pub fn limit(self) -> i64 {
        i64::from(self.size)
    }

    /// `OFFSET` value for the repository query.
    ///
    /// Widened before multiplying, so a far page number produces a large offset — and an empty
    /// page — rather than wrapping around to the start of the collection.
    #[must_use]
    pub fn offset(self) -> i64 {
        i64::from(self.page) * i64::from(self.size)
    }

    /// Whether a further page exists behind a total row count.
    #[must_use]
    pub fn has_next(self, total: u64) -> bool {
        (u64::from(self.page) + 1) * u64::from(self.size) < total
    }
}

/// Orders the industrial-complex search can serve.
///
/// Three, and each answers a question the screen actually asks. `Name` is the default because the
/// screen exists to find one complex by name, and 가나다순 is the only order in which a reader can
/// predict where a name will be. `AreaDesc` answers "which are the big ones". `OfficialCode` is the
/// order `list_complexes` has always returned, kept so a caller reading the collection in source
/// order does not lose it to the addition of paging. Every order is made total by the same
/// `official_complex_code, id` tiebreak — without it, two rows sharing a sort key can appear on two
/// consecutive pages or on neither.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ComplexSearchSort {
    /// Name ascending.
    #[default]
    Name,
    /// Designated area, largest first.
    AreaDesc,
    /// Source-side official complex code ascending.
    OfficialCode,
}

impl ComplexSearchSort {
    /// Parses the stable wire value used by the HTTP query parameter.
    ///
    /// # Errors
    ///
    /// Returns [`ComplexSearchQueryError::UnknownSort`] for an unsupported value.
    pub fn from_wire(raw: &str) -> Result<Self, ComplexSearchQueryError> {
        match raw {
            "name_asc" => Ok(Self::Name),
            "area_desc" => Ok(Self::AreaDesc),
            "official_complex_code_asc" => Ok(Self::OfficialCode),
            other => Err(ComplexSearchQueryError::UnknownSort(other.to_owned())),
        }
    }

    /// The stable wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Name => "name_asc",
            Self::AreaDesc => "area_desc",
            Self::OfficialCode => "official_complex_code_asc",
        }
    }
}

/// One page request against the canonical industrial-complex table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComplexSearchQuery {
    /// Substring the name or the official complex code must contain.
    pub text: Option<ComplexSearchText>,
    /// Province the complex's resolved address falls in.
    ///
    /// Filtering by it excludes the complexes whose address resolved to no code at all — the honest
    /// answer, because such a complex is not known to be in any province.
    pub sido_code: Option<SidoCodeFilter>,
    /// Development lifecycle values to keep. Empty means every value, including none at all.
    pub statuses: Vec<IndustrialComplexStatus>,
    /// Page and page size.
    pub paging: ComplexSearchPaging,
    /// Row order.
    pub sort: ComplexSearchSort,
}

/// One page of rows plus the size of the filtered collection they were drawn from.
#[derive(Clone, Debug)]
pub struct ComplexSearchResult<T> {
    /// Rows on this page, in the requested order.
    pub rows: Vec<T>,
    /// Rows the filters match in total, not just on this page.
    ///
    /// Counted by the same statement that selects the page, so the two cannot disagree the way a
    /// separate `COUNT(*)` round trip can under a concurrent write.
    pub total: u64,
}

/// A caller's search parameters could not be accepted.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ComplexSearchQueryError {
    /// `q` was present but held no word.
    #[error("q must not be blank")]
    BlankText,
    /// `sido_code` was not two ASCII digits.
    #[error("sido_code must be two digits: got {0:?}")]
    InvalidSidoCode(String),
    /// `size` was `0` or above [`MAX_PAGE_SIZE`].
    #[error("size must be between 1 and {max}: got {size}", max = MAX_PAGE_SIZE, size = .0)]
    PageSizeOutOfRange(u32),
    /// `sort` named an order this route does not serve.
    #[error("unknown sort: {0:?}")]
    UnknownSort(String),
    /// `status` named a lifecycle value the domain does not define.
    #[error("unknown status: {0:?}")]
    UnknownStatus(String),
}

/// Parses a comma-separated `status` filter into domain values.
///
/// # Errors
///
/// Returns [`ComplexSearchQueryError::UnknownStatus`] when an element is not a domain wire value.
pub fn parse_status_filter(
    raw: &str,
) -> Result<Vec<IndustrialComplexStatus>, ComplexSearchQueryError> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            IndustrialComplexStatus::from_wire(value)
                .map_err(|_| ComplexSearchQueryError::UnknownStatus(value.to_owned()))
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_page_size_above_the_maximum_is_refused() {
        // The disabling experiment for this bound: delete the `size > MAX_PAGE_SIZE` arm in
        // `ComplexSearchPaging::try_new` and this assertion is the one that goes red.
        assert_eq!(
            ComplexSearchPaging::try_new(None, Some(100_000)),
            Err(ComplexSearchQueryError::PageSizeOutOfRange(100_000))
        );
        assert_eq!(
            ComplexSearchPaging::try_new(None, Some(MAX_PAGE_SIZE + 1)),
            Err(ComplexSearchQueryError::PageSizeOutOfRange(
                MAX_PAGE_SIZE + 1
            ))
        );
    }

    #[test]
    fn a_zero_page_size_is_refused() {
        // Zero would make `has_next` true forever: `(page + 1) * 0 < total` for any non-empty
        // collection, so a client paging on `has_next` would never stop.
        assert_eq!(
            ComplexSearchPaging::try_new(Some(3), Some(0)),
            Err(ComplexSearchQueryError::PageSizeOutOfRange(0))
        );
    }

    #[test]
    fn the_maximum_page_size_itself_is_served() {
        let paging = ComplexSearchPaging::try_new(None, Some(MAX_PAGE_SIZE))
            .expect("100 is inside the bound");
        assert_eq!(paging.size(), MAX_PAGE_SIZE);
        assert_eq!(paging.limit(), 100);
    }

    #[test]
    fn absent_page_and_size_take_the_documented_defaults() {
        let paging =
            ComplexSearchPaging::try_new(None, None).expect("defaults are inside the bound");
        assert_eq!(paging.page(), 0);
        assert_eq!(paging.size(), DEFAULT_PAGE_SIZE);
        assert_eq!(paging.offset(), 0);
    }

    #[test]
    fn offsets_are_computed_wide_so_a_far_page_does_not_wrap() {
        let paging = ComplexSearchPaging::try_new(Some(u32::MAX), Some(100)).expect("bounded size");
        assert_eq!(paging.offset(), i64::from(u32::MAX) * 100);
        assert!(!paging.has_next(1_448));
    }

    #[test]
    fn has_next_is_false_on_the_last_page() {
        // 73 pages of 20 = 1,460 ≥ 1,448, so page 72 is the last one.
        let last = ComplexSearchPaging::try_new(Some(72), Some(20)).expect("bounded size");
        assert!(!last.has_next(1_448));
        let earlier = ComplexSearchPaging::try_new(Some(71), Some(20)).expect("bounded size");
        assert!(earlier.has_next(1_448));
    }

    #[test]
    fn a_korean_word_becomes_a_contains_pattern_unchanged() {
        let text = ComplexSearchText::try_new(" 반월 ").expect("a word");
        assert_eq!(text.as_str(), "반월");
        assert_eq!(text.contains_pattern(), "%반월%");
    }

    #[test]
    fn caller_wildcards_are_escaped_rather_than_honoured() {
        // Without this, `q=%` would match every row while claiming to be a substring search, and
        // `q=1_1` would match `101`, `111`, … — answers the user did not ask for.
        let wildcards = ComplexSearchText::try_new("100_%").expect("a word");
        assert_eq!(wildcards.contains_pattern(), "%100\\_\\%%");
        let escape = ComplexSearchText::try_new("a\\b").expect("a word");
        assert_eq!(escape.contains_pattern(), "%a\\\\b%");
    }

    #[test]
    fn a_blank_search_word_is_refused() {
        assert_eq!(
            ComplexSearchText::try_new("   "),
            Err(ComplexSearchQueryError::BlankText)
        );
        assert_eq!(
            ComplexSearchText::try_new(""),
            Err(ComplexSearchQueryError::BlankText)
        );
    }

    #[test]
    fn a_sido_code_must_be_two_digits() {
        assert_eq!(
            SidoCodeFilter::try_new("41").expect("two digits").as_str(),
            "41"
        );
        for invalid in ["4", "411", "4a", "", " 41"] {
            assert_eq!(
                SidoCodeFilter::try_new(invalid),
                Err(ComplexSearchQueryError::InvalidSidoCode(invalid.to_owned()))
            );
        }
    }

    #[test]
    fn sort_wire_values_round_trip() {
        for sort in [
            ComplexSearchSort::Name,
            ComplexSearchSort::AreaDesc,
            ComplexSearchSort::OfficialCode,
        ] {
            assert_eq!(
                ComplexSearchSort::from_wire(sort.wire_name()).expect("round trip"),
                sort
            );
        }
        assert_eq!(ComplexSearchSort::default(), ComplexSearchSort::Name);
        assert_eq!(
            ComplexSearchSort::from_wire("price_asc"),
            Err(ComplexSearchQueryError::UnknownSort("price_asc".to_owned()))
        );
    }

    #[test]
    fn a_status_filter_parses_every_domain_value() {
        assert_eq!(
            parse_status_filter("operating, developing ,planned").expect("domain values"),
            vec![
                IndustrialComplexStatus::Operating,
                IndustrialComplexStatus::Developing,
                IndustrialComplexStatus::Planned,
            ]
        );
        // `unknown` is a stored value, not a parse failure, so it must be filterable.
        assert_eq!(
            parse_status_filter("unknown").expect("domain value"),
            vec![IndustrialComplexStatus::Unknown]
        );
        assert_eq!(parse_status_filter("").expect("no values"), Vec::new());
        assert_eq!(
            parse_status_filter("operating,sold_out"),
            Err(ComplexSearchQueryError::UnknownStatus(
                "sold_out".to_owned()
            ))
        );
    }
}
