use serde::Deserialize;

/// Which fields a court auction object's PNU is assembled from, and the ledger-kind digits a
/// standard PNU may carry (root ADR-0290 §8). The values come from the contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PnuAssembly {
    /// The four fields whose values concatenate to the ten-digit legal-dong code, in order
    /// (시도, 시군구, 읍면동, 리).
    pub legal_dong_code_fields: [String; 4],
    /// The field holding the lot, e.g. `"66-8"` or `"산80"`.
    pub lot_field: String,
    /// The ledger-kind (대장구분) digit a standard PNU may carry.
    pub ledger_kind_digits: Vec<char>,
    /// The ledger-kind digit a court auction row implies (land, not a closed ledger).
    pub ledger_kind: char,
}

/// Why a row's fields could not assemble a standard PNU.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PnuUnresolved {
    /// A legal-dong code field was absent or not its expected digit count.
    LegalDongCode,
    /// The lot field was absent, or prose rather than `본번` / `본번-부번` / `산…`.
    Lot,
    /// The assembled ledger-kind digit was not one the contract allows.
    LedgerKind,
}

/// The 19-digit PNU a court auction row assembles to, or why it could not.
///
/// `legal-dong code (10) + ledger-kind digit (1) + 본번 (4) + 부번 (4)`. A `산` lot is not the
/// ledger-kind digit — that stays the land digit from the contract; `산` is carried by the code
/// history elsewhere, so here it only guards the lot's shape. Prose lots ("…외 6필지",
/// "…도시개발구역5블록") do not assemble and are left for the resolver's steward queue.
///
/// # Errors
/// Returns [`PnuUnresolved`] naming the first field that did not fit.
pub fn assemble(
    rule: &PnuAssembly,
    field: impl Fn(&str) -> Option<String>,
) -> Result<String, PnuUnresolved> {
    let mut code = String::with_capacity(10);
    for name in &rule.legal_dong_code_fields {
        let part = field(name).unwrap_or_default();
        code.push_str(part.trim());
    }
    if code.len() != 10 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PnuUnresolved::LegalDongCode);
    }
    let lot = field(&rule.lot_field).unwrap_or_default();
    let (bonbun, bubun) = split_lot(lot.trim()).ok_or(PnuUnresolved::Lot)?;
    if !rule.ledger_kind_digits.contains(&rule.ledger_kind) {
        return Err(PnuUnresolved::LedgerKind);
    }
    Ok(format!("{code}{}{bonbun:04}{bubun:04}", rule.ledger_kind))
}

/// `본번`, `본번-부번`, or the same with a leading `산`, as `(bonbun, bubun)`. Anything else — a
/// comma, a word, an empty string — is not a lot.
fn split_lot(lot: &str) -> Option<(u32, u32)> {
    let digits = lot.strip_prefix('산').map_or(lot, str::trim_start);
    let (bonbun, bubun) = digits.split_once('-').unwrap_or((digits, "0"));
    let bonbun: u32 = bonbun.parse().ok().filter(|n| *n < 10_000)?;
    let bubun: u32 = bubun.parse().ok().filter(|n| *n < 10_000)?;
    Some((bonbun, bubun))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn rule() -> PnuAssembly {
        PnuAssembly {
            legal_dong_code_fields: [
                "sido".to_owned(),
                "sgg".to_owned(),
                "emd".to_owned(),
                "ri".to_owned(),
            ],
            lot_field: "lot".to_owned(),
            ledger_kind_digits: vec!['1', '2', '8', '9'],
            ledger_kind: '1',
        }
    }

    fn row(lot: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("sido".to_owned(), "11".to_owned()),
            ("sgg".to_owned(), "620".to_owned()),
            ("emd".to_owned(), "101".to_owned()),
            ("ri".to_owned(), "00".to_owned()),
            ("lot".to_owned(), lot.to_owned()),
        ])
    }

    fn assemble_row(lot: &str) -> Result<String, PnuUnresolved> {
        let r = row(lot);
        assemble(&rule(), |name| r.get(name).cloned())
    }

    #[test]
    fn code_and_lot_make_a_19_digit_pnu() {
        assert_eq!(assemble_row("66-8").as_deref(), Ok("1162010100100660008"));
        assert_eq!(assemble_row("66").as_deref(), Ok("1162010100100660000"));
        assert_eq!(assemble_row("산80").as_deref(), Ok("1162010100100800000"));
    }

    #[test]
    fn prose_or_a_missing_lot_does_not_assemble() {
        assert_eq!(assemble_row("800-23외 6필지"), Err(PnuUnresolved::Lot));
        assert_eq!(
            assemble_row("식사2도시개발구역5블록"),
            Err(PnuUnresolved::Lot)
        );
        assert_eq!(assemble_row(""), Err(PnuUnresolved::Lot));
    }

    #[test]
    fn a_short_code_does_not_assemble() {
        let mut r = row("66-8");
        r.insert("sido".to_owned(), String::new());
        assert_eq!(
            assemble(&rule(), |name| r.get(name).cloned()),
            Err(PnuUnresolved::LegalDongCode)
        );
    }
}
