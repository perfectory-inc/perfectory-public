//! Error text that keeps its causes.

/// The error and every cause beneath it, joined with `": "`.
///
/// `reqwest`, `sqlx` and most wrappers print only their outermost layer: "error sending request for
/// url (...)" without the "connection refused" or "dns error" beneath it is not a diagnosis. The
/// 2026-10-16 staff sign-in failure was logged with no reason at all.
#[must_use]
pub fn with_causes(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::with_causes;

    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Self>>);

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1
                .as_deref()
                .map(|layer| layer as &(dyn std::error::Error + 'static))
        }
    }

    #[test]
    fn every_cause_is_kept_in_order() {
        let error = Layer(
            "error sending request",
            Some(Box::new(Layer(
                "dns error",
                Some(Box::new(Layer("try again", None))),
            ))),
        );
        assert_eq!(
            with_causes(&error),
            "error sending request: dns error: try again"
        );
        assert_eq!(with_causes(&Layer("alone", None)), "alone");
    }
}
