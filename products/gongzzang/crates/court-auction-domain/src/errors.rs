//! `CourtAuction` Reader 에러.

use thiserror::Error;

/// Court-auction reader error.
#[derive(Debug, Error)]
pub enum ReaderError {
    /// 데이터 없음.
    #[error("court auction not found")]
    NotFound,
    /// Court-auction fetch failure.
    #[error("court auction fetch failed: {0}")]
    Fetch(String),
    /// Court-auction data parse failure.
    #[error("court auction data parse failed: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_displays() {
        assert_eq!(
            format!("{}", ReaderError::NotFound),
            "court auction not found"
        );
    }

    #[test]
    fn fetch_displays() {
        let err = ReaderError::Fetch("timeout".to_owned());
        assert_eq!(format!("{err}"), "court auction fetch failed: timeout");
    }

    #[test]
    fn parse_displays() {
        let err = ReaderError::Parse("malformed JSON".to_owned());
        assert_eq!(
            format!("{err}"),
            "court auction data parse failed: malformed JSON"
        );
    }
}
