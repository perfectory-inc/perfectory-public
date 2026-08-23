//! Provider text that arrives HTML-escaped, and the one place that unescapes it.
//!
//! Three official sources hand this platform text that has been through an HTML escaper: the
//! `hub.go.kr` and `VWorld` inventory pages (whose markup is escaped because it *is* markup), and the
//! industrial-complex profile workbook, whose free-text cells arrive escaped even though a
//! spreadsheet cell is not markup. Two copies of a six-entry `replace` chain used to live in
//! `collection-infrastructure`, and the workbook path had no decoder at all, which is how
//! `전자부품&middot;컴퓨터` reached 580 canonical rows.
//!
//! # Why this is not a general HTML entity decoder
//!
//! A conformant HTML5 decoder is the wrong tool for text that is not HTML. The WHATWG named
//! character reference table contains entries that are matched **without** a trailing semicolon
//! (`&not`, `&amp`, `&sim`, …), so a conformant decoder turns the ordinary sentence `R&notation`
//! into `R¬ation`. Provider free text is full of bare ampersands — `R&D` occurs in 18 rows of the
//! one snapshot measured, 8 of them spelled with a bare `&` — and none of them are markup. Another
//! 30 bare ampersands appear once `&amp;` is resolved. So this decoder requires the terminating
//! semicolon, and it resolves a closed table of references this repository has decided the meaning
//! of. Anything else is left exactly as it arrived and named by [`entity_references`], because a
//! reference nobody decided about must not be silently dropped or silently guessed at.
//!
//! # Exactly once
//!
//! [`decode_provider_html_text`] is a single left-to-right pass. A character it writes is never
//! re-examined, so `&amp;lt;` becomes `&lt;` and never `<`. The old `replace` chains decoded
//! `&amp;` first and then re-scanned their own output, which turned `&amp;lt;` into `<`. Both
//! copies had that defect; this is the reason they are gone.

/// Named character references this platform decodes, and the character each one names.
///
/// Closed on purpose — see the module comment. Counts below are the 2026-08-23 measurement over
/// the 1,443 canonical industrial-complex rows loaded from the `202506` profile snapshot; the
/// `&amp;`-prefixed counts are references the provider escaped twice, which a single decode pass
/// leaves as a bare reference for [`entity_references`] to report.
///
/// | reference | character | evidence |
/// |---|---|---|
/// | `middot` | `·` | 2,093 occurrences |
/// | `amp` | `&` | 30 occurrences |
/// | `quot` | `"` | 8 occurrences |
/// | `ldquo` | `“` | 2 bare, 4 as `&amp;ldquo;` |
/// | `rdquo` | `”` | 2 bare, 4 as `&amp;rdquo;` |
/// | `lsquo` | `‘` | 1 as `&amp;lsquo;` |
/// | `rsquo` | `’` | 1 as `&amp;rsquo;` |
/// | `sim` | `∼` | 2 as `&amp;sim;` |
/// | `lt`, `gt`, `nbsp` | `<`, `>`, U+00A0 | decoded by both inventory-page readers since they were written |
/// | `apos` | `'` | the fifth XML predefined reference, kept beside its four siblings |
///
/// `nbsp` resolves to U+00A0 rather than a plain space: that is the character the reference names,
/// and every caller that wants it collapsed already runs `trim`/`split_whitespace`, both of which
/// treat U+00A0 as whitespace.
///
/// Names are case-sensitive, as HTML references are.
const NAMED_REFERENCES: &[(&str, char)] = &[
    ("amp", '&'),
    ("apos", '\''),
    ("gt", '>'),
    ("ldquo", '\u{201c}'),
    ("lsquo", '\u{2018}'),
    ("lt", '<'),
    ("middot", '\u{b7}'),
    ("nbsp", '\u{a0}'),
    ("quot", '"'),
    ("rdquo", '\u{201d}'),
    ("rsquo", '\u{2019}'),
    ("sim", '\u{223c}'),
];

/// Longest reference name considered. The longest HTML5 name is 31 characters.
const MAX_REFERENCE_NAME_LEN: usize = 32;

/// `1114111` is the largest code point, so more than seven decimal digits cannot name one.
const MAX_DECIMAL_DIGITS: usize = 7;

/// `10FFFF` is the largest code point, so more than six hex digits cannot name one.
const MAX_HEX_DIGITS: usize = 6;

/// Unescapes provider text exactly once.
///
/// Resolves the [`NAMED_REFERENCES`] table and semicolon-terminated numeric references
/// (`&#39;`, `&#x27;`). Everything else — a bare `&`, a reference with no semicolon, a name not in
/// the table, a numeric reference naming no representable character — is copied through byte for
/// byte. Use [`entity_references`] on the result to find out what was left.
#[must_use]
pub fn decode_provider_html_text(raw: &str) -> String {
    if !raw.contains('&') {
        return raw.to_owned();
    }
    let mut decoded = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(offset) = rest.find('&') {
        decoded.push_str(&rest[..offset]);
        let candidate = &rest[offset..];
        if let Some((character, length)) = resolve_reference(candidate) {
            decoded.push(character);
            rest = &candidate[length..];
        } else {
            // Advancing one byte past the `&` is what makes this pass single: the `&` just written
            // is behind the cursor and can never combine with the text after it.
            decoded.push('&');
            rest = &candidate[1..];
        }
    }
    decoded.push_str(rest);
    decoded
}

/// Entity-shaped tokens present in `text`, in order of appearance, duplicates included.
///
/// Run over the output of [`decode_provider_html_text`] this names exactly what the decode did not
/// resolve: a reference outside [`NAMED_REFERENCES`], a numeric reference naming no representable
/// character, and the bare reference a provider's second escaping pass leaves behind
/// (`&amp;ldquo;` decodes once to `&ldquo;`). Callers report these; nobody deletes them.
///
/// "Entity-shaped" is deliberately generous — `&D;` in `R&D; 센터` is reported even though it is
/// ordinary text. A caller that over-reports gets a false line in a summary; a caller that
/// under-reports puts an undecoded reference on a screen.
#[must_use]
pub fn entity_references(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(offset) = rest.find('&') {
        let candidate = &rest[offset..];
        if let Some((_, length)) = reference_body(candidate) {
            found.push(candidate[..length].to_owned());
            rest = &candidate[length..];
        } else {
            rest = &candidate[1..];
        }
    }
    found
}

/// Resolves the reference `candidate` starts with into its character and its byte length.
fn resolve_reference(candidate: &str) -> Option<(char, usize)> {
    let (body, length) = reference_body(candidate)?;
    let character = match body.strip_prefix('#') {
        Some(digits) => numeric_reference(digits)?,
        None => named_reference(body)?,
    };
    Some((character, length))
}

/// Splits `&…;` at the front of `candidate` into its body and the whole token's byte length.
///
/// Returns `None` when the token is not reference-shaped at all, which is what keeps an ordinary
/// ampersand — `R&D`, `삼성전자 & 협력사` — from being read as the start of a reference.
fn reference_body(candidate: &str) -> Option<(&str, usize)> {
    let body = candidate.strip_prefix('&')?;
    for (index, character) in body.char_indices() {
        if character == ';' {
            // `1` for the `&`, `index` bytes of body, `1` for the `;`.
            return (index > 0).then_some((&body[..index], index + 2));
        }
        if index >= MAX_REFERENCE_NAME_LEN
            || !(character.is_ascii_alphanumeric() || character == '#')
        {
            return None;
        }
    }
    None
}

/// Resolves one name against [`NAMED_REFERENCES`].
fn named_reference(name: &str) -> Option<char> {
    NAMED_REFERENCES
        .iter()
        .find(|(reference, _)| *reference == name)
        .map(|(_, character)| *character)
}

/// Resolves the digits of a numeric reference, with or without the `x` hex marker.
fn numeric_reference(digits: &str) -> Option<char> {
    let (body, radix, max_digits) = digits
        .strip_prefix(['x', 'X'])
        .map_or((digits, 10, MAX_DECIMAL_DIGITS), |hexadecimal| {
            (hexadecimal, 16, MAX_HEX_DIGITS)
        });
    if body.is_empty()
        || body.len() > max_digits
        || !body.chars().all(|character| character.is_digit(radix))
    {
        return None;
    }
    let character = char::from_u32(u32::from_str_radix(body, radix).ok()?)?;
    representable_in_provider_text(character).then_some(character)
}

/// Whether a decoded character may enter a provider text column.
///
/// A numeric reference can name a C0 control, and `&#0;` in particular names a byte no Postgres
/// `text` column accepts. Refusing it here leaves the reference verbatim, where
/// [`entity_references`] reports it, rather than writing a value that fails four stages later.
fn representable_in_provider_text(character: char) -> bool {
    !character.is_control() || matches!(character, '\t' | '\n' | '\r')
}

#[cfg(test)]
mod tests {
    use super::{decode_provider_html_text, entity_references, NAMED_REFERENCES};

    /// Every reference the measured `202506` industrial-complex snapshot actually carries.
    #[test]
    fn decodes_every_reference_the_measured_snapshot_carries() {
        assert_eq!(
            decode_provider_html_text(
                "전자부품&middot;컴퓨터&middot;영상&middot;음향및통신장비제조업"
            ),
            "전자부품·컴퓨터·영상·음향및통신장비제조업"
        );
        assert_eq!(decode_provider_html_text("R&amp;D기업"), "R&D기업");
        assert_eq!(decode_provider_html_text("&#39;"), "'");
        assert_eq!(decode_provider_html_text("&quot;"), "\"");
        assert_eq!(decode_provider_html_text("&ldquo;육성&rdquo;"), "“육성”");
    }

    /// The defect both deleted `replace` chains had. `.replace("&amp;", "&")` ran first and its own
    /// output was re-scanned by the four replacements after it, so `&amp;lt;` came out as `<`.
    #[test]
    fn an_escaped_ampersand_is_decoded_exactly_once() {
        assert_eq!(decode_provider_html_text("&amp;lt;"), "&lt;");
        assert_eq!(decode_provider_html_text("&amp;amp;"), "&amp;");
        assert_eq!(decode_provider_html_text("&amp;#39;"), "&#39;");
        // The provider really does escape twice: this is row 244710 of the measured snapshot.
        assert_eq!(
            decode_provider_html_text("아산 북부권에 &amp;ldquo;지역전략산업&amp;rdquo;을"),
            "아산 북부권에 &ldquo;지역전략산업&rdquo;을"
        );
    }

    #[test]
    fn numeric_references_are_decoded_in_both_bases() {
        assert_eq!(decode_provider_html_text("&#39;a&#x27;"), "'a'");
        assert_eq!(decode_provider_html_text("&#183;"), "·");
        assert_eq!(decode_provider_html_text("&#X2018;"), "‘");
    }

    #[test]
    fn an_ordinary_ampersand_is_not_the_start_of_a_reference() {
        assert_eq!(decode_provider_html_text("R&D 센터"), "R&D 센터");
        assert_eq!(decode_provider_html_text("KT&G"), "KT&G");
        assert_eq!(decode_provider_html_text("&"), "&");
        assert_eq!(decode_provider_html_text("a & b"), "a & b");
    }

    /// The WHATWG table matches `&not`, `&amp`, and 104 other names with no semicolon. This decoder
    /// does not, because provider free text is not markup and `R&notation` is not `R¬ation`.
    #[test]
    fn a_reference_without_its_semicolon_is_left_alone() {
        assert_eq!(decode_provider_html_text("R&notation"), "R&notation");
        assert_eq!(decode_provider_html_text("&amp"), "&amp");
        assert_eq!(decode_provider_html_text("&middot"), "&middot");
    }

    #[test]
    fn an_undecidable_reference_is_left_verbatim_and_named() {
        assert_eq!(decode_provider_html_text("&hellip;"), "&hellip;");
        assert_eq!(entity_references("&hellip;"), vec!["&hellip;".to_owned()]);
        // A numeric reference naming no representable character is left as it arrived.
        assert_eq!(decode_provider_html_text("&#0;"), "&#0;");
        assert_eq!(decode_provider_html_text("&#xD800;"), "&#xD800;");
        assert_eq!(decode_provider_html_text("&#99999999;"), "&#99999999;");
        assert_eq!(decode_provider_html_text("&#;"), "&#;");
    }

    /// What the single pass leaves is what the caller has to report — including the bare reference
    /// a provider's second escaping pass produced.
    #[test]
    fn entity_references_names_what_one_decode_pass_left() {
        let decoded = decode_provider_html_text("&amp;ldquo;육성&amp;rdquo; &middot; &hellip;");
        assert_eq!(decoded, "&ldquo;육성&rdquo; · &hellip;");
        assert_eq!(
            entity_references(&decoded),
            vec![
                "&ldquo;".to_owned(),
                "&rdquo;".to_owned(),
                "&hellip;".to_owned()
            ]
        );
        // Nothing survives a decode of text whose references are all in the table.
        assert!(entity_references(&decode_provider_html_text("전자부품&middot;컴퓨터")).is_empty());
    }

    #[test]
    fn entity_references_ignores_an_ampersand_that_starts_nothing() {
        assert!(entity_references("R&D 센터").is_empty());
        assert!(entity_references("KT&G").is_empty());
        assert!(entity_references("&").is_empty());
        assert!(entity_references("&;").is_empty());
    }

    /// A long run of text after an ampersand is not scanned to a distant semicolon.
    #[test]
    fn an_over_long_candidate_is_not_a_reference() {
        let long_name = "a".repeat(64);
        let text = format!("&{long_name};");
        assert_eq!(decode_provider_html_text(&text), text);
        assert!(entity_references(&text).is_empty());
    }

    #[test]
    fn text_without_an_ampersand_is_returned_unchanged() {
        assert_eq!(
            decode_provider_html_text("구로디지털단지"),
            "구로디지털단지"
        );
        assert_eq!(decode_provider_html_text(""), "");
    }

    /// Two entries naming the same reference would make the table's answer depend on its order.
    #[test]
    fn the_reference_table_names_each_reference_once() {
        let mut names = NAMED_REFERENCES
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "a reference name is listed twice");
    }
}
