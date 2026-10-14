//! COPY text escaping for free-text names streamed into a Postgres stage.

/// Escapes one value for `COPY ... FROM STDIN WITH (FORMAT text)`.
///
/// A building or unit name is free text from a national register, and an unescaped tab or newline in one name would
/// shift every column after it by one for that row, silently. Backslash first, so the escapes
/// this function writes are not themselves re-escaped.
pub(crate) fn copy_text_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tab_in_a_name_does_not_become_a_column_boundary() {
        assert_eq!(copy_text_escape("A\t동"), "A\\t동");
        assert_eq!(copy_text_escape("줄\n바꿈"), "줄\\n바꿈");
        assert_eq!(copy_text_escape("역\\슬래시"), "역\\\\슬래시");
        assert_eq!(copy_text_escape("멀쩡한 이름 101동"), "멀쩡한 이름 101동");
    }

    #[test]
    fn escaping_is_not_applied_twice() {
        // Backslash is replaced first; the sequences the later replacements write must survive.
        assert_eq!(copy_text_escape("\\t"), "\\\\t");
    }
}
