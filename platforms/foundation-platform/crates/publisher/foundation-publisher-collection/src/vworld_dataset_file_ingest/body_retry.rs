//! A file whose body breaks after its response started is asked for again, whole.
//!
//! The client retries a download only up to the status-validated response
//! (`VWorldDatasetFileClient::send_file_request`); once the stream starts, only the caller can ask
//! again. 2026-11-22 and 11-23 the daily sweep's backfill read about 15,000 files a run and 9,
//! then 5, different ones each day ended on "error decoding response body". Each was a `failed`
//! file, and one `failed` file fails the day's whole sweep, though the next run fetched them.

use std::future::Future;
use std::time::Duration;

use collection_domain::CollectionError;
use collection_infrastructure::VWORLD_FILE_BODY_STREAM_FAILED;

/// How many times one file is asked for when its body breaks mid-download.
const ATTEMPTS: u32 = 3;
/// The pause before the second ask; the third waits twice as long.
const PAUSE: Duration = Duration::from_secs(5);

/// Whether the file's body broke after its response started.
///
/// True wherever in the chain the reader wrapped the client's error.
pub(super) fn body_broke_mid_download(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<CollectionError>(),
            Some(CollectionError::Infrastructure(message))
                if message.starts_with(VWORLD_FILE_BODY_STREAM_FAILED)
        )
    })
}

/// Runs `fetch` again while the file's body broke mid-download.
///
/// `fetch` opens the file and persists its body; it runs up to [`ATTEMPTS`] times in all. Any other
/// failure, and a body that broke every time, is returned as it is: the file is still `failed` and
/// the run still ends as failed.
pub(super) async fn asking_again_for_a_broken_body<T, Fetch, Fetching>(
    provider_file_id: &str,
    fetch: Fetch,
) -> anyhow::Result<T>
where
    Fetch: Fn() -> Fetching,
    Fetching: Future<Output = anyhow::Result<T>>,
{
    asking_again(provider_file_id, PAUSE, fetch).await
}

async fn asking_again<T, Fetch, Fetching>(
    provider_file_id: &str,
    pause: Duration,
    fetch: Fetch,
) -> anyhow::Result<T>
where
    Fetch: Fn() -> Fetching,
    Fetching: Future<Output = anyhow::Result<T>>,
{
    let mut attempt = 1;
    loop {
        match fetch().await {
            Err(error) if attempt < ATTEMPTS && body_broke_mid_download(&error) => {
                tracing::warn!(
                    provider_file_id,
                    attempt,
                    "VWorld dataset file body broke mid-download; asking for the file again"
                );
                tokio::time::sleep(pause * attempt).await;
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    fn broke() -> anyhow::Error {
        // As the spool's reader and the committer wrap the client's stream error.
        anyhow::Error::new(CollectionError::Infrastructure(format!(
            "{VWORLD_FILE_BODY_STREAM_FAILED}: error decoding response body"
        )))
        .context("failed to read provider file synthetic-1")
        .context("failed to commit VWorld dataset Bronze object")
    }

    #[test]
    fn a_body_that_broke_mid_download_is_known_through_the_readers_context() {
        assert!(body_broke_mid_download(&broke()));
        // Any other failure is not asked for again: a refused write, a provider that did not serve.
        let refused = anyhow::Error::new(CollectionError::Infrastructure(
            "failed to write Bronze object to storage".to_owned(),
        ))
        .context("failed to commit VWorld dataset Bronze object");
        assert!(!body_broke_mid_download(&refused));
        let unavailable = anyhow::Error::new(CollectionError::ProviderUnavailable(
            VWORLD_FILE_BODY_STREAM_FAILED.to_owned(),
        ));
        assert!(!body_broke_mid_download(&unavailable));
        assert!(!body_broke_mid_download(&anyhow::anyhow!(
            "{VWORLD_FILE_BODY_STREAM_FAILED}"
        )));
    }

    #[tokio::test]
    async fn a_broken_body_is_asked_for_again_and_the_second_answer_stands() -> anyhow::Result<()> {
        let asked = &AtomicU32::new(0);
        let size = asking_again("synthetic-1", Duration::ZERO, || async move {
            if asked.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(broke())
            } else {
                Ok(7_u64)
            }
        })
        .await?;
        assert_eq!((size, asked.load(Ordering::SeqCst)), (7, 2));
        Ok(())
    }

    #[tokio::test]
    async fn a_body_that_breaks_every_time_fails_after_three_asks() {
        let asked = &AtomicU32::new(0);
        let outcome: anyhow::Result<u64> =
            asking_again("synthetic-1", Duration::ZERO, || async move {
                asked.fetch_add(1, Ordering::SeqCst);
                Err(broke())
            })
            .await;
        assert!(outcome.is_err_and(|error| body_broke_mid_download(&error)));
        assert_eq!(asked.load(Ordering::SeqCst), ATTEMPTS);
    }

    #[tokio::test]
    async fn any_other_failure_is_not_asked_for_again() {
        let asked = &AtomicU32::new(0);
        let outcome: anyhow::Result<u64> =
            asking_again("synthetic-1", Duration::ZERO, || async move {
                asked.fetch_add(1, Ordering::SeqCst);
                Err(anyhow::anyhow!("failed to write Bronze object to storage"))
            })
            .await;
        assert!(outcome.is_err());
        assert_eq!(asked.load(Ordering::SeqCst), 1);
    }
}
