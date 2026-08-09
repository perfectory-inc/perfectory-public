//! Effective-dated membership of a parcel in an industrial complex.
//!
//! Membership is a fact between two entities, not a column on either of them (ADR-0019), and it is
//! what a record asserts rather than what an overlay computed (ADR-0020). There is no membership
//! *kind* here: without a geometric judgement only one kind of membership remains, and the row's
//! existence is the assertion.
//!
//! The one vocabulary below is written down twice — here and in
//! `parcel_complex_membership_asserted_by_check` — which is the shape ADR-0018 exists for, so the
//! enum carries `ALL` and `a_database_vocabulary_is_spelled_the_same_way_in_both_languages` reads
//! the installed CHECK constraint and compares it against that. The list is not restated by that
//! test.

/// Who asserts that a parcel belongs to an industrial complex.
///
/// Spells `parcel_complex_membership_asserted_by_check`, the way
/// [`CatalogMutationKind`](crate::CatalogMutationKind) spells its own command-kind check.
///
/// The two are not degrees of confidence. They are different kinds of claim, and they can disagree:
/// an official list and a reviewer naming different complexes for one parcel is a question worth
/// being able to ask, and it is unaskable once both have been flattened into a single column on the
/// parcel. Neither value describes geometry — a parcel polygon falling inside a complex polygon is
/// not one of the ways membership may be established (ADR-0020).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MembershipAssertedBy {
    /// Taken from the complex's official parcel list, as published by the authority that owns it.
    OfficialList,
    /// Decided by a human reviewer.
    ManualReview,
}

impl MembershipAssertedBy {
    /// Every asserter, so a caller can enumerate the vocabulary without restating it.
    pub const ALL: [Self; 2] = [Self::OfficialList, Self::ManualReview];

    /// Database spelling of this asserter.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OfficialList => "official_list",
            Self::ManualReview => "manual_review",
        }
    }

    /// Parses the database spelling.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not one of the two covered asserters.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "official_list" => Ok(Self::OfficialList),
            "manual_review" => Ok(Self::ManualReview),
            other => Err(format!("unknown membership asserter: {other}")),
        }
    }
}
