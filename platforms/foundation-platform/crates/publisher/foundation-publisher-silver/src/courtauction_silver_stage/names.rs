//! The masking of a person's name in the free text a 법원경매정보 Silver table keeps (root ADR-0540 §4).
//!
//! The rules are the source contract's `personal_names`; the patterns are the Spark job's
//! `name_patterns`, which look around a word, so they run on `fancy-regex` (`DuckDB`'s RE2 and the
//! `regex` crate have no look-around). Bronze keeps the text as received.

use std::collections::BTreeSet;

use anyhow::Context;
use fancy_regex::{Captures, Regex};
use serde_json::Value;

/// The one-syllable particles a name may be followed by in the site's text.
const PARTICLES: [char; 10] = ['은', '는', '이', '가', '의', '에', '와', '과', '도', '로'];

/// The contract's rules, compiled.
#[derive(Clone, Debug)]
pub struct NameMask {
    /// A name before a `before` word.
    before: Regex,
    /// A name after an `after` word, and the names a comma joins to it.
    after: Regex,
    /// A Hangul word.
    word: Regex,
    not_names: BTreeSet<String>,
    mask: String,
    shortest: usize,
}

fn strings(rules: &Value, name: &str) -> anyhow::Result<Vec<String>> {
    rules[name]
        .as_array()
        .with_context(|| format!("personal_names.{name} is not a list"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .with_context(|| format!("personal_names.{name} holds a non-string"))
        })
        .collect()
}

impl NameMask {
    /// The source contract's `personal_names` compiled.
    ///
    /// # Errors
    /// Refuses rules that are not the contract's shape or a pattern that does not compile.
    pub fn new(rules: &Value) -> anyhow::Result<Self> {
        let syllables = rules["name_syllables"]
            .as_array()
            .context("personal_names.name_syllables is not a list")?;
        let bound = |index: usize| -> anyhow::Result<usize> {
            Ok(usize::try_from(
                syllables
                    .get(index)
                    .and_then(Value::as_u64)
                    .context("personal_names.name_syllables is not two counts")?,
            )?)
        };
        let (low, high) = (bound(0)?, bound(1)?);
        let word = format!("[가-힣]{{{low},{high}}}");
        let before = strings(rules, "before")?
            .iter()
            .map(|text| fancy_regex::escape(text).into_owned())
            .collect::<Vec<_>>()
            .join("|");
        let mut after_words = strings(rules, "after")?;
        // Longest first, as the job sorts them: the alternation takes the first that matches.
        after_words.sort_by_key(|text| std::cmp::Reverse(text.chars().count()));
        let after = after_words
            .iter()
            .map(|text| fancy_regex::escape(text).into_owned())
            .collect::<Vec<_>>()
            .join("|");
        let particles = PARTICLES.map(String::from).join("|");
        let ends = format!("(?=[^가-힣]|$|{particles}|씨|님)");
        Ok(Self {
            before: Regex::new(&format!("({word})(?=\\s*(?:의\\s*)?(?:{before}))"))?,
            after: Regex::new(&format!(
                "((?<![가-힣])(?:{after})(?:\\s+|\\s*[:：(]\\s*))({word}{ends}(?:\\s*,\\s*(?!{after}){word}{ends})*)"
            ))?,
            word: Regex::new("[가-힣]+")?,
            not_names: strings(rules, "not_names")?.into_iter().collect(),
            mask: rules["mask"]
                .as_str()
                .context("personal_names.mask is not text")?
                .to_owned(),
            shortest: low,
        })
    }

    /// The mask of `word`, or the word when it is not a name: a particle it ends in is kept, and the
    /// word is judged without it ("미상의" is "미상").
    fn masked(&self, word: &str) -> String {
        let (stem, tail) = match word.chars().last() {
            Some(last) if PARTICLES.contains(&last) && word.chars().count() > self.shortest => (
                &word[..word.len() - last.len_utf8()],
                &word[word.len() - last.len_utf8()..],
            ),
            _ => (word, ""),
        };
        if self.not_names.contains(stem) || self.not_names.contains(word) {
            word.to_owned()
        } else {
            format!("{}{tail}", self.mask)
        }
    }

    /// `text` with each name the rules recognise masked (the job's `mask_names`).
    ///
    /// # Errors
    /// Returns the pattern engine's failure (a backtracking limit).
    pub fn apply(&self, text: &str) -> anyhow::Result<String> {
        let once = replace_all(&self.before, text, |found| {
            Ok(self.masked(found.get(1).map_or("", |m| m.as_str())))
        })?;
        replace_all(&self.after, &once, |found| {
            let listed = found.get(2).map_or("", |m| m.as_str());
            let names = replace_all(&self.word, listed, |word| Ok(self.masked(&word[0])))?;
            Ok(format!(
                "{}{names}",
                found.get(1).map_or("", |m| m.as_str())
            ))
        })
    }
}

/// Every match of `pattern` in `text` replaced by what `with` makes of it, left to right.
fn replace_all(
    pattern: &Regex,
    text: &str,
    mut with: impl FnMut(&Captures<'_, str>) -> anyhow::Result<String>,
) -> anyhow::Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for found in pattern.captures_iter(text) {
        let found = found?;
        let whole = found.get(0).context("a match has its text")?;
        out.push_str(&text[last..whole.start()]);
        out.push_str(&with(&found)?);
        last = whole.end();
    }
    out.push_str(&text[last..]);
    Ok(out)
}

#[cfg(test)]
mod tests;
