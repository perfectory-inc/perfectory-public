use chrono::NaiveDate;
use serde::Deserialize;

/// When a case's detail is due again, from the contract (root ADR-0290 §3).
///
/// A court auction case moves on a known schedule, so its detail need not be re-read every night.
/// A case is due when it has never been read, when its list row changed since it was last read, or
/// on the days around its sale date; a closed case is never due again.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RecrawlRule {
    /// Days before the sale date on which the detail is due.
    pub days_before_sale: Vec<i64>,
    /// Days after the sale date on which the detail is due (the result lands).
    pub days_after_sale: Vec<i64>,
    /// `status_code` values that mean the case is closed and never due again.
    pub closed_status_codes: Vec<String>,
    /// How many days after the last read the detail is due even with nothing else (a backstop).
    pub max_age_days: i64,
}

/// What `due` reads about one case: its state, drawn from the list row and the queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseView<'a> {
    /// The case's progress-status code, as the list row carries it.
    pub status_code: &'a str,
    /// The sale date, when the list row carries one.
    pub sale_date: Option<NaiveDate>,
    /// The day the detail was last read, or `None` when it never was.
    pub last_read: Option<NaiveDate>,
    /// Whether the list row's watched fields changed since the detail was last read.
    pub row_changed: bool,
}

/// Whether `case` is due for a detail read on `today`.
#[must_use]
pub fn due(rule: &RecrawlRule, case: &CaseView<'_>, today: NaiveDate) -> bool {
    if rule
        .closed_status_codes
        .iter()
        .any(|code| code == case.status_code)
    {
        return false;
    }
    let Some(last_read) = case.last_read else {
        return true; // never read
    };
    if case.row_changed {
        return true;
    }
    if (today - last_read).num_days() >= rule.max_age_days {
        return true;
    }
    if let Some(sale) = case.sale_date {
        let offset = (today - sale).num_days();
        let on_a_before_day = rule.days_before_sale.iter().any(|d| offset == -d);
        let on_an_after_day = rule.days_after_sale.contains(&offset);
        // Only when the detail was not already read on or after that marker day.
        if (on_a_before_day || on_an_after_day) && last_read < today {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn rule() -> RecrawlRule {
        RecrawlRule {
            days_before_sale: vec![1],
            days_after_sale: vec![1],
            closed_status_codes: vec!["closed".to_owned()],
            max_age_days: 30,
        }
    }

    fn day(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").expect("date")
    }

    fn open(last_read: Option<&str>) -> CaseView<'static> {
        CaseView {
            status_code: "open",
            sale_date: Some(day("2099-02-10")),
            last_read: last_read.map(day),
            row_changed: false,
        }
    }

    #[test]
    fn a_case_never_read_is_due() {
        assert!(due(&rule(), &open(None), day("2099-01-01")));
    }

    #[test]
    fn a_closed_case_is_never_due_again() {
        let mut case = open(Some("2099-01-01"));
        case.status_code = "closed";
        case.row_changed = true;
        assert!(!due(&rule(), &case, day("2099-02-09")));
    }

    #[test]
    fn a_changed_row_is_due() {
        let mut case = open(Some("2099-01-05"));
        case.row_changed = true;
        assert!(due(&rule(), &case, day("2099-01-06")));
    }

    #[test]
    fn the_day_before_and_after_the_sale_are_due_once() {
        assert!(due(&rule(), &open(Some("2099-01-20")), day("2099-02-09")));
        assert!(due(&rule(), &open(Some("2099-01-20")), day("2099-02-11")));
        // Already read today: not due again.
        assert!(!due(&rule(), &open(Some("2099-02-09")), day("2099-02-09")));
        // An ordinary day away from the sale: not due.
        assert!(!due(&rule(), &open(Some("2099-02-01")), day("2099-02-05")));
    }

    #[test]
    fn a_stale_read_is_due_by_the_backstop() {
        assert!(due(&rule(), &open(Some("2099-01-01")), day("2099-02-01")));
    }
}
