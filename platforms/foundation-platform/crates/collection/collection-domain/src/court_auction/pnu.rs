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
    /// The ledger-kind (대장구분) digits a standard PNU may carry (1/2/8/9).
    pub ledger_kind_digits: Vec<char>,
    /// The ledger-kind digit of an ordinary lot (토지대장).
    pub flat_ledger_kind: char,
    /// The ledger-kind digit of a `산` lot (임야대장).
    pub mountain_ledger_kind: char,
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
/// `legal-dong code (10) + ledger-kind digit (1) + 본번 (4) + 부번 (4)`. A `산` lot takes the
/// mountain ledger-kind digit (임야대장 2), an ordinary lot the flat one (토지대장 1) — the measured
/// `pnuNoCtt` does the same (`산80` → `…2 0080 0000`). Prose lots ("…외 6필지", "…도시개발구역5블록")
/// do not assemble and are left for the resolver's steward queue.
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
    let lot = split_lot(lot.trim()).ok_or(PnuUnresolved::Lot)?;
    let ledger_kind = if lot.mountain {
        rule.mountain_ledger_kind
    } else {
        rule.flat_ledger_kind
    };
    if !rule.ledger_kind_digits.contains(&ledger_kind) {
        return Err(PnuUnresolved::LedgerKind);
    }
    Ok(format!(
        "{code}{ledger_kind}{:04}{:04}",
        lot.bonbun, lot.bubun
    ))
}

/// A lot read from the row: whether it is `산`, its 본번 and 부번.
struct Lot {
    mountain: bool,
    bonbun: u32,
    bubun: u32,
}

/// `본번`, `본번-부번`, or the same with a leading `산`. Anything else — a comma, a word, an empty
/// string — is not a lot.
fn split_lot(lot: &str) -> Option<Lot> {
    let (mountain, digits) = lot
        .strip_prefix('산')
        .map_or((false, lot), |rest| (true, rest.trim_start()));
    let (bonbun, bubun) = digits.split_once('-').unwrap_or((digits, "0"));
    let bonbun: u32 = bonbun.parse().ok().filter(|n| *n < 10_000)?;
    let bubun: u32 = bubun.parse().ok().filter(|n| *n < 10_000)?;
    Some(Lot {
        mountain,
        bonbun,
        bubun,
    })
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
            flat_ledger_kind: '1',
            mountain_ledger_kind: '2',
        }
    }

    // The repository-reserved 99999 legal-dong code: a synthetic place, never a real parcel
    // (public-fixture-safety). The assembly is code+lot concatenation, so any code works here.
    fn row(lot: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("sido".to_owned(), "99".to_owned()),
            ("sgg".to_owned(), "999".to_owned()),
            ("emd".to_owned(), "999".to_owned()),
            ("ri".to_owned(), "99".to_owned()),
            ("lot".to_owned(), lot.to_owned()),
        ])
    }

    fn assemble_row(lot: &str) -> Result<String, PnuUnresolved> {
        let r = row(lot);
        assemble(&rule(), |name| r.get(name).cloned())
    }

    #[test]
    fn code_and_lot_make_a_19_digit_pnu() {
        assert_eq!(assemble_row("66-8").as_deref(), Ok("9999999999100660008"));
        assert_eq!(assemble_row("66").as_deref(), Ok("9999999999100660000"));
    }

    #[test]
    fn a_mountain_lot_takes_the_imya_ledger_digit() {
        // The measured pnuNoCtt does the same: a 산 lot gets ledger digit 2, bonbun then bubun.
        assert_eq!(assemble_row("산80").as_deref(), Ok("9999999999200800000"));
        assert_eq!(assemble_row("산17-1").as_deref(), Ok("9999999999200170001"));
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
