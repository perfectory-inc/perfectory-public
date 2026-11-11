//! Checking one person against the 부동산중개업 roster (root ADR-0550).
//!
//! A broker is identified by name and either the 자격증번호 of their licence or the 등록번호 of
//! the office they are registered to. The check answers whether that one person is on the roster;
//! it never lists people, and a name alone is not a question.

/// The comparable form of a person's name: trimmed, inner whitespace collapsed to one space.
#[must_use]
pub fn name_key(name: &str) -> Option<String> {
    let key = name.split_whitespace().collect::<Vec<_>>().join(" ");
    (!key.is_empty()).then_some(key)
}

/// The comparable form of a 자격증번호 or 등록번호: every space and hyphen removed.
#[must_use]
pub fn number_key(number: &str) -> Option<String> {
    let key: String = number
        .chars()
        .filter(|character| !character.is_whitespace() && *character != '-')
        .collect();
    (!key.is_empty()).then_some(key)
}

/// The second half of the question, next to the name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerCredential {
    /// The person's 자격증번호, as [`number_key`] compares it.
    License(String),
    /// The 등록번호 of an office the person is registered to, as [`number_key`] compares it.
    Registration(String),
}

/// One verification question: a name and exactly one credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerVerifyQuery {
    /// The name, as [`name_key`] compares it.
    pub name_key: String,
    /// The credential the name must come with.
    pub credential: BrokerCredential,
}

/// Why a question is refused before it is asked.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BrokerVerifyQueryError {
    /// The name is absent or blank.
    #[error("a name is required")]
    MissingName,
    /// Neither or both of 자격증번호 and 등록번호 were given.
    #[error("exactly one of license_no and registration_no is required")]
    CredentialCount,
    /// A field is longer than any roster value.
    #[error("a field is longer than {MAX_FIELD_CHARS} characters")]
    TooLong,
}

/// No roster name or number is near this long; a longer field is not a question.
pub const MAX_FIELD_CHARS: usize = 100;

impl BrokerVerifyQuery {
    /// Builds a question from the wire fields.
    ///
    /// # Errors
    /// A blank name, not exactly one non-blank credential, or a field over [`MAX_FIELD_CHARS`].
    pub fn parse(
        name: &str,
        license_no: Option<&str>,
        registration_no: Option<&str>,
    ) -> Result<Self, BrokerVerifyQueryError> {
        let too_long = |value: &str| value.chars().count() > MAX_FIELD_CHARS;
        if too_long(name)
            || license_no.is_some_and(too_long)
            || registration_no.is_some_and(too_long)
        {
            return Err(BrokerVerifyQueryError::TooLong);
        }
        let name_key = name_key(name).ok_or(BrokerVerifyQueryError::MissingName)?;
        let credential = match (
            license_no.and_then(number_key),
            registration_no.and_then(number_key),
        ) {
            (Some(license), None) => BrokerCredential::License(license),
            (None, Some(registration)) => BrokerCredential::Registration(registration),
            _ => return Err(BrokerVerifyQueryError::CredentialCount),
        };
        Ok(Self {
            name_key,
            credential,
        })
    }
}

/// What a match tells about the one person it found and their office.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerMatch {
    /// 중개업자종별 (공인중개사, 중개보조원, 중개인, 법인).
    pub member_kind_name: Option<String>,
    /// 직위 (대표, 이사, 감사, 일반).
    pub position_name: Option<String>,
    /// The office's 등록번호, as the provider writes it.
    pub registration_no: String,
    /// The office's 상호.
    pub office_name: Option<String>,
    /// The office's 상태 (영업, 휴업 …), when the roster holds the office.
    pub office_status_name: Option<String>,
    /// The provider's 데이터기준일자 of the row that matched.
    pub data_reference_date: Option<String>,
}

/// Where a check looked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerSource {
    /// The roster loaded from Silver.
    Registry,
    /// VWorld's live `getEBBrokerInfo`, asked on a roster miss.
    VworldLive,
}

impl BrokerSource {
    /// The wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::VworldLive => "vworld_live",
        }
    }
}

/// The answer to one question.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerVerification {
    /// The person found, or none.
    pub found: Option<BrokerMatch>,
    /// Every source that answered, in the order asked. A miss without `VworldLive` is a miss the
    /// live register could not confirm.
    pub checked: Vec<BrokerSource>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_numbers_compare_without_layout() {
        assert_eq!(name_key("  홍  길동 ").as_deref(), Some("홍 길동"));
        assert_eq!(name_key(" \t "), None);
        assert_eq!(
            number_key(" 99999-2026 -00001 ").as_deref(),
            Some("99999202600001")
        );
        assert_eq!(number_key("--"), None);
    }

    #[test]
    fn a_question_is_a_name_and_exactly_one_credential() {
        let by_license = BrokerVerifyQuery::parse("홍길동", Some("2026-99999"), None);
        assert_eq!(
            by_license.map(|query| query.credential),
            Ok(BrokerCredential::License("202699999".to_owned()))
        );
        let by_registration = BrokerVerifyQuery::parse("홍길동", Some(" "), Some("99999-1"));
        assert_eq!(
            by_registration.map(|query| query.credential),
            Ok(BrokerCredential::Registration("999991".to_owned()))
        );
        assert_eq!(
            BrokerVerifyQuery::parse("홍길동", None, None),
            Err(BrokerVerifyQueryError::CredentialCount)
        );
        assert_eq!(
            BrokerVerifyQuery::parse("홍길동", Some("1"), Some("2")),
            Err(BrokerVerifyQueryError::CredentialCount)
        );
        assert_eq!(
            BrokerVerifyQuery::parse(" ", Some("1"), None),
            Err(BrokerVerifyQueryError::MissingName)
        );
        assert_eq!(
            BrokerVerifyQuery::parse(&"가".repeat(101), Some("1"), None),
            Err(BrokerVerifyQueryError::TooLong)
        );
    }
}
