use std::time::Duration;

use serde::Deserialize;

/// How one endpoint group's requests-in-flight move, from the contract (AIMD).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PaceRule {
    /// Requests in flight at the start.
    pub start: u32,
    /// The floor a decrease never goes below.
    pub min: u32,
    /// The ceiling an increase never goes above.
    pub max: u32,
    /// Clear answers in a row that earn one more request in flight.
    pub increase_after: u32,
    /// The divisor a setback applies to the current width (2 halves it).
    pub decrease_divisor: u32,
    /// A response slower than this counts as a setback, like a block.
    pub slow_response_millis: u64,
}

/// The live width of one endpoint group, moved by its rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pace {
    rule: PaceRule,
    width: u32,
    clear_streak: u32,
}

impl Pace {
    /// A new pace at the rule's start width.
    #[must_use]
    pub fn new(rule: PaceRule) -> Self {
        let width = rule.start.clamp(rule.min.max(1), rule.max.max(1));
        Self {
            rule,
            width,
            clear_streak: 0,
        }
    }

    /// The number of requests allowed in flight now.
    #[must_use]
    pub const fn width(&self) -> u32 {
        self.width
    }

    /// A clear, timely answer: after `increase_after` of them in a row, widen by one, up to `max`.
    pub fn clear(&mut self, elapsed: Duration) {
        if elapsed >= Duration::from_millis(self.rule.slow_response_millis) {
            self.setback();
            return;
        }
        self.clear_streak += 1;
        if self.clear_streak >= self.rule.increase_after {
            self.clear_streak = 0;
            self.width = (self.width + 1).min(self.rule.max.max(1));
        }
    }

    /// A block or a slow answer: divide the width (never below `min` or 1) and reset the streak.
    pub fn setback(&mut self) {
        self.clear_streak = 0;
        let divisor = self.rule.decrease_divisor.max(2);
        self.width = (self.width / divisor).max(self.rule.min.max(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule() -> PaceRule {
        PaceRule {
            start: 4,
            min: 1,
            max: 8,
            increase_after: 3,
            decrease_divisor: 2,
            slow_response_millis: 2000,
        }
    }

    #[test]
    fn it_starts_at_the_rules_start_width() {
        assert_eq!(Pace::new(rule()).width(), 4);
    }

    #[test]
    fn a_streak_of_clear_answers_widens_by_one_up_to_the_max() {
        let mut pace = Pace::new(rule());
        for _ in 0..3 {
            pace.clear(Duration::from_millis(100));
        }
        assert_eq!(pace.width(), 5);
        for _ in 0..30 {
            pace.clear(Duration::from_millis(100));
        }
        assert_eq!(pace.width(), rule().max);
    }

    #[test]
    fn a_setback_halves_the_width_down_to_the_min() {
        let mut pace = Pace::new(rule());
        pace.setback();
        assert_eq!(pace.width(), 2);
        pace.setback();
        pace.setback();
        assert_eq!(pace.width(), rule().min);
    }

    #[test]
    fn a_slow_answer_is_a_setback_and_breaks_the_streak() {
        let mut pace = Pace::new(rule());
        pace.clear(Duration::from_millis(100));
        pace.clear(Duration::from_millis(100));
        pace.clear(Duration::from_secs(3));
        assert_eq!(pace.width(), 2);
    }
}
