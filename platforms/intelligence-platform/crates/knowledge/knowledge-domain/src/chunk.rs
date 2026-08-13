use sha2::{Digest, Sha256};
use thiserror::Error;

/// 색인 단위. 하나의 원천 문서를 순서대로 자른 조각이다.
///
/// `content_checksum_sha256`은 색인 입력(제목 경로 + 본문)의 정규화 결과에 대한 해시이며
/// 임베딩 입력이 아니다. Intelligence ADR-0002가 요구하는 네 식별자 중
/// `content_checksum`에 해당한다.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KnowledgeChunk {
    pub source_id: String,
    pub chunk_ordinal: i32,
    pub heading_path: String,
    pub body: String,
    pub content_checksum_sha256: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ChunkValidationError {
    #[error("{message}")]
    Invalid { message: String },
}

impl KnowledgeChunk {
    pub fn new(
        source_id: impl Into<String>,
        chunk_ordinal: i32,
        heading_path: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<Self, ChunkValidationError> {
        let source_id = source_id.into();
        let heading_path = heading_path.into();
        let body = body.into();

        if source_id.trim().is_empty() {
            return Err(invalid("source_id must be non-empty"));
        }
        if chunk_ordinal < 0 {
            return Err(invalid("chunk_ordinal must not be negative"));
        }
        if body.trim().is_empty() {
            return Err(invalid("body must be non-empty"));
        }

        let content_checksum_sha256 = checksum(&heading_path, &body);
        Ok(Self {
            source_id,
            chunk_ordinal,
            heading_path,
            body,
            content_checksum_sha256,
        })
    }
}

/// 색인 입력의 정규화 규칙. 제목 경로와 본문을 `\n` 하나로 잇고 양끝 공백만 제거한다.
/// 규칙을 바꾸면 모든 기존 체크섬이 달라지므로 새 `release_id`로 재색인해야 한다.
fn checksum(heading_path: &str, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(heading_path.trim().as_bytes());
    hasher.update(b"\n");
    hasher.update(body.trim().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn invalid(message: impl Into<String>) -> ChunkValidationError {
    ChunkValidationError::Invalid {
        message: message.into(),
    }
}

#[cfg(test)]
// test code: panics are failures
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn chunk_checksum_is_stable_for_same_content() {
        let first = KnowledgeChunk::new("source-1", 0, "제목", "본문입니다")
            .expect("valid chunk must build");
        let second = KnowledgeChunk::new("source-1", 0, "제목", "본문입니다")
            .expect("valid chunk must build");

        assert_eq!(first.content_checksum_sha256, second.content_checksum_sha256);
        assert_eq!(first.content_checksum_sha256.len(), 64);
    }

    #[test]
    fn chunk_checksum_changes_with_body() {
        let first =
            KnowledgeChunk::new("source-1", 0, "제목", "본문 A").expect("valid chunk must build");
        let second =
            KnowledgeChunk::new("source-1", 0, "제목", "본문 B").expect("valid chunk must build");

        assert_ne!(first.content_checksum_sha256, second.content_checksum_sha256);
    }

    #[test]
    fn empty_source_id_is_rejected() {
        let error = KnowledgeChunk::new("  ", 0, "제목", "본문").expect_err("must reject");
        assert!(matches!(error, ChunkValidationError::Invalid { .. }));
    }

    #[test]
    fn empty_body_is_rejected() {
        let error = KnowledgeChunk::new("source-1", 0, "제목", "   ").expect_err("must reject");
        assert!(matches!(error, ChunkValidationError::Invalid { .. }));
    }

    #[test]
    fn negative_ordinal_is_rejected() {
        let error = KnowledgeChunk::new("source-1", -1, "제목", "본문").expect_err("must reject");
        assert!(matches!(error, ChunkValidationError::Invalid { .. }));
    }
}
