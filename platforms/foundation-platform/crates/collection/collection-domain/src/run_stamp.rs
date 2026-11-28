//! The run stamp a Bronze leaf may not carry (root ADR-0626 rule 5, G5).
//!
//! A collector that put `YYYYMMDDtHHMMSSz` in its leaves made every retry a new key and a new ledger
//! row (3,969 of them in 24 sources, F12), and its readers parsed the stamp back as the snapshot.
//! When a run collected an object is its ledger row's `collected_at`. The write boundary refuses the
//! stamp ([`crate::build_bronze_object_key`]); the Bronze epoch finds it to take it out.

/// The first run stamp `YYYYMMDDtHHMMSSz` in `value` (either case), as a byte range.
#[must_use]
pub fn run_stamp_range(value: &str) -> Option<std::ops::Range<usize>> {
    const STAMP: usize = 16;
    let bytes = value.as_bytes();
    (0..=bytes.len().checked_sub(STAMP)?).find_map(|start| {
        let window = &bytes[start..start + STAMP];
        let digits = |range: std::ops::Range<usize>| window[range].iter().all(u8::is_ascii_digit);
        let stamp = digits(0..8)
            && window[8].eq_ignore_ascii_case(&b't')
            && digits(9..15)
            && window[15].eq_ignore_ascii_case(&b'z');
        // A longer digit run around it is a provider number, not a stamp.
        let bounded = (start == 0 || !bytes[start - 1].is_ascii_digit())
            && bytes
                .get(start + STAMP)
                .is_none_or(|next| !next.is_ascii_digit());
        (stamp && bounded).then_some(start..start + STAMP)
    })
}

#[cfg(test)]
mod tests {
    use super::run_stamp_range;

    #[test]
    fn a_stamp_is_found_in_either_case_and_a_longer_number_is_not_one() {
        assert_eq!(
            run_stamp_range("notice-detail-20991231t235959z-000"),
            Some(14..30)
        );
        assert_eq!(run_stamp_range("regcode-20991231T235959Z"), Some(8..24));
        assert_eq!(run_stamp_range("120991231t235959z"), None);
        assert_eq!(run_stamp_range("OPN209912310000000002"), None);
        assert_eq!(run_stamp_range("item--sha256-00"), None);
    }
}
