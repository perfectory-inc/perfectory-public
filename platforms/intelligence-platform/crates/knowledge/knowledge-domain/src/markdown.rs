use crate::chunk::{ChunkValidationError, KnowledgeChunk};

/// 마크다운 문서를 제목 기준으로 자른다.
///
/// 규칙:
/// - ATX 제목(`#`~`######`)에서만 자른다. 표와 코드블록은 자르지 않는다.
/// - 펜스 코드블록(``` 또는 ~~~) 안의 `#`는 제목으로 보지 않는다.
/// - YAML frontmatter는 본문에서 제외한다.
/// - 본문이 비어 있는 절은 청크를 만들지 않는다. 제목 경로는 다음 절이 이어받는다.
pub fn split_markdown(
    source_id: &str,
    text: &str,
) -> Result<Vec<KnowledgeChunk>, ChunkValidationError> {
    let mut chunks = Vec::new();
    let mut heading_stack: Vec<String> = Vec::new();
    let mut body = String::new();
    let mut ordinal = 0_i32;
    let mut fence: Option<String> = None;

    for line in strip_frontmatter(text).lines() {
        let trimmed = line.trim_start();

        if let Some(open) = fence.clone() {
            if trimmed.starts_with(&open) {
                fence = None;
            }
            body.push_str(line);
            body.push('\n');
            continue;
        }
        if let Some(marker) = fence_marker(trimmed) {
            fence = Some(marker);
            body.push_str(line);
            body.push('\n');
            continue;
        }

        match heading_level(trimmed) {
            Some((level, title)) => {
                push_chunk(&mut chunks, source_id, &mut ordinal, &heading_stack, &body)?;
                body.clear();
                let parent_depth = level.saturating_sub(1);
                heading_stack.truncate(parent_depth);
                while heading_stack.len() < parent_depth {
                    heading_stack.push(String::new());
                }
                heading_stack.push(title);
            }
            None => {
                body.push_str(line);
                body.push('\n');
            }
        }
    }
    push_chunk(&mut chunks, source_id, &mut ordinal, &heading_stack, &body)?;

    Ok(chunks)
}

fn push_chunk(
    chunks: &mut Vec<KnowledgeChunk>,
    source_id: &str,
    ordinal: &mut i32,
    heading_stack: &[String],
    body: &str,
) -> Result<(), ChunkValidationError> {
    if body.trim().is_empty() {
        return Ok(());
    }
    let heading_path = heading_stack
        .iter()
        .filter(|part| !part.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join(" > ");
    chunks.push(KnowledgeChunk::new(
        source_id,
        *ordinal,
        heading_path,
        body.trim(),
    )?);
    *ordinal += 1;
    Ok(())
}

fn heading_level(trimmed: &str) -> Option<(usize, String)> {
    if !trimmed.starts_with('#') {
        return None;
    }
    let level = trimmed.chars().take_while(|c| *c == '#').count();
    if level == 0 || level > 6 {
        return None;
    }
    let rest = trimmed.get(level..)?;
    if !rest.starts_with(' ') {
        return None;
    }
    Some((level, rest.trim().to_string()))
}

fn fence_marker(trimmed: &str) -> Option<String> {
    for marker in ["```", "~~~"] {
        if trimmed.starts_with(marker) {
            return Some(marker.to_string());
        }
    }
    None
}

fn strip_frontmatter(text: &str) -> &str {
    let Some(rest) = text.strip_prefix("---\n") else {
        return text;
    };
    match rest.find("\n---\n") {
        Some(end) => rest.get(end + 5..).unwrap_or(text),
        None => text,
    }
}

#[cfg(test)]
// test code: panics are failures
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_headings_and_keeps_the_heading_path() {
        let text = "# 제목\n\n앞 문단\n\n## 하위\n\n뒤 문단\n";
        let chunks = split_markdown("doc-1", text).expect("split must succeed");

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].heading_path, "제목");
        assert!(chunks[0].body.contains("앞 문단"));
        assert_eq!(chunks[1].heading_path, "제목 > 하위");
        assert!(chunks[1].body.contains("뒤 문단"));
        assert_eq!(chunks[0].chunk_ordinal, 0);
        assert_eq!(chunks[1].chunk_ordinal, 1);
    }

    #[test]
    fn does_not_split_inside_a_fenced_code_block() {
        let text = "# 제목\n\n```sh\n# 이건 주석이지 제목이 아니다\necho hi\n```\n\n본문\n";
        let chunks = split_markdown("doc-1", text).expect("split must succeed");

        assert_eq!(chunks.len(), 1, "코드블록 안의 # 는 제목이 아니다");
        assert!(chunks[0].body.contains("echo hi"));
    }

    #[test]
    fn keeps_a_table_inside_one_chunk() {
        let text = "# 제목\n\n| a | b |\n|---|---|\n| 1 | 2 |\n";
        let chunks = split_markdown("doc-1", text).expect("split must succeed");

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].body.contains("| 1 | 2 |"));
    }

    #[test]
    fn skips_frontmatter_and_sections_with_no_body() {
        let text = "---\nstatus: current\n---\n\n# 제목\n\n## 바로 하위\n\n본문\n";
        let chunks = split_markdown("doc-1", text).expect("split must succeed");

        assert_eq!(chunks.len(), 1, "본문 없는 제목은 청크가 되지 않는다");
        assert_eq!(chunks[0].heading_path, "제목 > 바로 하위");
        assert!(!chunks[0].body.contains("status: current"));
    }

    #[test]
    fn a_document_with_no_body_yields_no_chunks() {
        let chunks = split_markdown("doc-1", "# 제목만\n").expect("split must succeed");
        assert!(chunks.is_empty());
    }
}
