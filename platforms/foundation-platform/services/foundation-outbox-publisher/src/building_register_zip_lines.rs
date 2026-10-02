//! Shared bounded physical-line reader for building-register single-file ZIP inputs.
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use anyhow::{bail, Context};
use tokio::{sync::mpsc, task::JoinHandle};
use zip::ZipArchive;

use crate::serving_scratch::{CURSOR_BYTES, CURSOR_ROWS};
#[cfg(test)]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// 디렉터리를 제외한 유일한 ZIP 파일 항목의 인덱스를 반환한다.
/// # Errors
/// ZIP 항목을 읽을 수 없거나 파일이 정확히 하나가 아니면 실패한다.
pub fn single_file_entry_index<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
) -> anyhow::Result<usize> {
    let mut file_indexes = Vec::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("failed to inspect zip entry {index}"))?;
        if !entry.is_dir() {
            file_indexes.push(index);
        }
    }
    match file_indexes.as_slice() {
        [index] => Ok(*index),
        [] => bail!("HUB Bronze zip must contain one TXT file, found no file entries"),
        indexes => bail!(
            "HUB Bronze zip must contain one TXT file, found {} file entries",
            indexes.len()
        ),
    }
}

/// 공통 바이트 상한 안에서 원문과 실제 줄 번호를 한 줄씩 전달한다.
/// # Errors
/// ZIP 읽기·UTF-8 해석·크기 검사 또는 호출자의 처리가 실패하면 오류를 반환한다.
pub fn decode_zip_lines(
    object_path: &Path,
    max_rows: Option<usize>,
    on_line: impl FnMut(&str, u64) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    decode_zip_lines_with_limit(object_path, max_rows, CURSOR_BYTES, on_line)
}

fn decode_zip_lines_with_limit(
    object_path: &Path,
    max_rows: Option<usize>,
    max_line_bytes: usize,
    mut on_line: impl FnMut(&str, u64) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=CURSOR_BYTES).contains(&max_line_bytes),
        "invalid ZIP line byte budget"
    );
    let file = File::open(object_path)
        .with_context(|| format!("failed to open HUB Bronze zip {}", object_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read HUB Bronze zip {}", object_path.display()))?;
    let entry_index = single_file_entry_index(&mut archive)?;
    let entry = archive
        .by_index(entry_index)
        .with_context(|| format!("failed to open zip entry {entry_index}"))?;
    let mut reader = BufReader::new(entry);
    let mut physical_line = 0_u64;
    let mut decoded = 0_usize;
    loop {
        if matches!(max_rows, Some(limit) if decoded >= limit) {
            break;
        }
        let mut line = String::new();
        // Limit allocation before read_line sees an arbitrarily long provider row.
        let mut bounded = (&mut reader).take(u64::try_from(max_line_bytes)? + 2);
        let read = bounded
            .read_line(&mut line)
            .with_context(|| format!("failed to read line {}", physical_line.saturating_add(1)))?;
        if read == 0 {
            break;
        }
        physical_line = physical_line
            .checked_add(1)
            .context("ZIP physical line number overflow")?;
        if !line.ends_with('\n') && !reader.fill_buf()?.is_empty() {
            bail!("ZIP source line {physical_line} exceeds byte bound");
        }
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }
        if line.len() > max_line_bytes {
            bail!("ZIP source line {physical_line} exceeds byte bound");
        }
        if line.trim().is_empty() {
            continue;
        }
        on_line(&line, physical_line)?;
        decoded = decoded
            .checked_add(1)
            .context("ZIP decoded row count overflow")?;
    }
    Ok(())
}

/// 제한된 채널로 ZIP 판독 작업에 역압력을 전달하는 비동기 소비자.
pub struct AsyncZipLines {
    receiver: mpsc::Receiver<anyhow::Result<(String, u64)>>,
    producer: Option<JoinHandle<anyhow::Result<()>>>,
    pending: Option<(String, u64)>,
    #[cfg(test)]
    join_observed: Arc<AtomicBool>,
}

impl AsyncZipLines {
    /// 동기 ZIP 판독 작업을 시작한다. 오류는 읽기 또는 종료 시 전달된다.
    #[must_use]
    pub fn open(path: &Path, max_rows: Option<usize>) -> Self {
        let path = path.to_owned();
        let (sender, receiver) = mpsc::channel(1);
        #[cfg(test)]
        let join_observed = Arc::new(AtomicBool::new(false));
        let producer = tokio::task::spawn_blocking(move || {
            let result = decode_zip_lines(&path, max_rows, |line, number| {
                sender
                    .blocking_send(Ok((line.to_owned(), number)))
                    .map_err(|_| anyhow::anyhow!("ZIP consumer stopped"))
            });
            if let Err(error) = result {
                if !sender.is_closed() {
                    let _ = sender.blocking_send(Err(error));
                }
            }
            Ok(())
        });
        Self {
            receiver,
            producer: Some(producer),
            pending: None,
            #[cfg(test)]
            join_observed,
        }
    }

    /// 다음 원문과 실제 줄 번호를 받는다.
    /// # Errors
    /// 원문 판독 작업이 실패하면 해당 오류를 반환한다.
    pub async fn next(&mut self) -> anyhow::Result<Option<(String, u64)>> {
        if let Some(line) = self.pending.take() {
            return Ok(Some(line));
        }
        if let Some(decoded) = self.receiver.recv().await {
            return decoded.map(Some);
        }
        self.join().await?;
        Ok(None)
    }

    /// 공통 행 수·바이트 상한을 지키는 다음 묶음을 받는다.
    /// # Errors
    /// 원문 판독 또는 바이트 계산이 실패하면 오류를 반환한다.
    pub async fn next_batch(&mut self) -> anyhow::Result<Vec<(String, u64)>> {
        self.next_batch_with_limits(CURSOR_ROWS, CURSOR_BYTES).await
    }

    async fn next_batch_with_limits(
        &mut self,
        maximum_rows: usize,
        maximum_bytes: usize,
    ) -> anyhow::Result<Vec<(String, u64)>> {
        anyhow::ensure!(
            (1..=CURSOR_ROWS).contains(&maximum_rows)
                && (1..=CURSOR_BYTES).contains(&maximum_bytes),
            "invalid ZIP batch budget"
        );
        let mut batch = Vec::new();
        let mut bytes = 0;
        while batch.len() < maximum_rows {
            let Some(line) = self.next().await? else {
                break;
            };
            anyhow::ensure!(
                line.0.len() <= maximum_bytes,
                "ZIP source line exceeds batch byte bound"
            );
            if line.0.len() > maximum_bytes - bytes {
                self.pending = Some(line);
                break;
            }
            bytes += line.0.len();
            batch.push(line);
        }
        Ok(batch)
    }

    async fn join(&mut self) -> anyhow::Result<()> {
        if let Some(producer) = self.producer.take() {
            producer.await??;
            #[cfg(test)]
            self.join_observed.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// 소비를 닫고 판독 작업의 종료 결과까지 확인한다.
    /// # Errors
    /// 판독 작업의 실패 또는 panic을 오류로 반환한다.
    pub async fn close(mut self) -> anyhow::Result<()> {
        self.receiver.close();
        self.join().await
    }
}

impl Drop for AsyncZipLines {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some(producer) = self.producer.take() {
            // Cancellation transfers join ownership to a task that outlives this consumer.
            // Closing the receiver unblocks a producer stalled at capacity one.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                #[cfg(test)]
                let observed = Arc::clone(&self.join_observed);
                runtime.spawn(async move {
                    match producer.await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            tracing::warn!(%error, "ZIP producer failed during cancellation");
                        }
                        Err(error) => {
                            tracing::warn!(%error, "ZIP producer join failed during cancellation");
                        }
                    }
                    #[cfg(test)]
                    observed.store(true, Ordering::Release);
                });
            } else {
                // Runtime teardown cannot await a blocking task; receiver closure still
                // makes its next send fail, so it releases the archive on termination.
                drop(producer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write as _, time::Duration};

    fn zip_with(path: &Path, content: &[u8]) -> anyhow::Result<()> {
        let mut archive = zip::ZipWriter::new(File::create(path)?);
        archive.start_file("one.txt", zip::write::SimpleFileOptions::default())?;
        archive.write_all(content)?;
        archive.finish()?;
        Ok(())
    }

    #[tokio::test]
    async fn batches_bound_bytes_and_rows_without_losing_carried_physical_lines(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("batch.zip");
        zip_with(&path, b"\r\nab\r\n\ncd\nefg\nh\n")?;
        let mut lines = AsyncZipLines::open(&path, None);
        assert_eq!(
            lines.next_batch_with_limits(3, 4).await?,
            vec![("ab".into(), 2), ("cd".into(), 4)]
        );
        // A scalar consumer also receives the row carried into the next batch.
        assert_eq!(lines.next().await?, Some(("efg".into(), 5)));
        assert_eq!(
            lines.next_batch_with_limits(1, 4).await?,
            vec![("h".into(), 6)]
        );
        assert!(lines.next_batch().await?.is_empty());
        lines.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn batch_errors_join_producer_and_never_become_successful_eof() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("batch.zip");
        zip_with(&path, b"abcde\n")?;
        let mut lines = AsyncZipLines::open(&path, None);
        assert!(lines.next_batch_with_limits(2, 4).await.is_err());
        lines.close().await?;
        zip_with(&path, b"ok\n\xff\n")?;
        let mut lines = AsyncZipLines::open(&path, None);
        assert!(lines.next_batch().await.is_err());
        lines.close().await?;
        Ok(())
    }

    #[test]
    fn tiny_line_budget_rejects_before_callback_and_preserves_physical_blank_ordinal(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("lines.zip");
        zip_with(&path, b"\r\nabc\r\n12345\n")?;
        let mut seen = Vec::new();
        let error = decode_zip_lines_with_limit(&path, None, 4, |line, ordinal| {
            seen.push((line.to_owned(), ordinal));
            Ok(())
        })
        .err()
        .context("line larger than four bytes must fail")?;
        assert!(error.to_string().contains("exceeds byte bound"));
        assert_eq!(seen, [("abc".to_owned(), 2)]);
        Ok(())
    }

    #[tokio::test]
    async fn producer_error_and_early_consumer_cancellation_join() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let bad = root.path().join("invalid.zip");
        zip_with(&bad, b"\xff\n")?;
        let mut invalid = AsyncZipLines::open(&bad, None);
        assert!(
            invalid.next().await.is_err(),
            "invalid UTF-8 must reach the consumer"
        );
        invalid.close().await?;

        let many = root.path().join("many.zip");
        zip_with(&many, "a\n".repeat(1000).as_bytes())?;
        let mut cancelled = AsyncZipLines::open(&many, None);
        assert_eq!(cancelled.next().await?, Some(("a".to_owned(), 1)));
        let observed = Arc::clone(&cancelled.join_observed);
        drop(cancelled);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !observed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }
}
