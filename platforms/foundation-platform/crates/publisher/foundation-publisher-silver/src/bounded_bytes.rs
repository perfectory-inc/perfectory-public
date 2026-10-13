//! Byte-limited sink used before JSON serialization can grow an output buffer.
use std::io::{self, Write};

/// Shared maximum decoded row size for native readers and scratch consumers.
pub const MAX_ROW_BYTES: usize = 8 * 1024 * 1024;

/// 직렬화 중 상한을 넘기기 전에 쓰기를 거부하는 바이트 버퍼.
pub struct BoundedBytes {
    bytes: Vec<u8>,
    count: BoundedByteCount,
}

/// Count-only sink: serialization cannot allocate a second row-sized buffer.
pub struct BoundedByteCount {
    written: usize,
    limit: usize,
    error: &'static str,
}

impl BoundedByteCount {
    /// 지정한 상한과 오류 문구를 사용하는 누적 계수기를 만든다.
    #[must_use]
    pub const fn with_error(limit: usize, error: &'static str) -> Self {
        Self {
            written: 0,
            limit,
            error,
        }
    }

    /// 누적 크기를 더한다.
    /// # Errors
    /// 덧셈이 넘치거나 누적 크기가 상한보다 크면 실패한다.
    pub fn add_bytes(&mut self, bytes: usize) -> io::Result<()> {
        self.written = self
            .written
            .checked_add(bytes)
            .filter(|total| *total <= self.limit)
            .ok_or_else(|| io::Error::other(self.error))?;
        Ok(())
    }

    /// 현재 누적한 바이트 수를 반환한다.
    #[must_use]
    pub const fn bytes_written(&self) -> usize {
        self.written
    }
}

impl Write for BoundedByteCount {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.add_bytes(input.len())?;
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BoundedBytes {
    /// 기본 오류 문구와 지정한 바이트 상한으로 버퍼를 만든다.
    #[must_use]
    pub const fn new(limit: usize) -> Self {
        Self::with_error(limit, "serialized row exceeds byte bound")
    }

    /// 지정한 오류 문구와 바이트 상한으로 버퍼를 만든다.
    #[must_use]
    pub const fn with_error(limit: usize, error: &'static str) -> Self {
        Self {
            bytes: Vec::new(),
            count: BoundedByteCount::with_error(limit, error),
        }
    }

    /// 누적한 바이트의 소유권을 호출자에게 넘긴다.
    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedBytes {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.count.add_bytes(input.len())?;
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    #[test]
    fn counting_matches_escaped_json_without_collecting_bytes() -> anyhow::Result<()> {
        let row = serde_json::json!({"raw": "한글\n\"\\\u{0000}"});
        let expected = serde_json::to_vec(&row)?;
        let mut count = BoundedByteCount::with_error(expected.len(), "fixture cap");
        serde_json::to_writer(&mut count, &row)?;
        assert_eq!(count.bytes_written(), expected.len());
        assert_eq!(
            count
                .write(b"x")
                .err()
                .context("byte cap must fail")?
                .to_string(),
            "fixture cap"
        );
        assert_eq!(count.bytes_written(), expected.len());
        Ok(())
    }

    #[test]
    fn byte_collection_and_counting_share_an_atomic_overflow_guard() -> anyhow::Result<()> {
        let mut bytes = BoundedBytes::with_error(3, "fixture cap");
        bytes.write_all(b"ab")?;
        assert_eq!(
            bytes
                .write(b"cd")
                .err()
                .context("byte cap must fail")?
                .to_string(),
            "fixture cap"
        );
        assert_eq!(bytes.into_inner(), b"ab");
        let mut count = BoundedByteCount::with_error(usize::MAX, "fixture overflow");
        count.add_bytes(usize::MAX)?;
        assert!(count.add_bytes(1).is_err());
        assert_eq!(count.bytes_written(), usize::MAX);
        Ok(())
    }
}
