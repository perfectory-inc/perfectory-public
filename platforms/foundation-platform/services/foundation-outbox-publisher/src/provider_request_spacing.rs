use std::time::Duration;

use anyhow::bail;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderRequestSpacing {
    interval: Duration,
}

impl ProviderRequestSpacing {
    pub(crate) fn try_new(interval: Duration) -> anyhow::Result<Self> {
        if interval.is_zero() {
            bail!("provider request spacing interval must be greater than zero");
        }
        Ok(Self { interval })
    }

    pub(crate) fn optional_from_millis(value: Option<u64>) -> anyhow::Result<Option<Self>> {
        value
            .map(|millis| Self::try_new(Duration::from_millis(millis)))
            .transpose()
    }

    pub(crate) fn delay_before_request(&self, request_index: usize) -> Option<Duration> {
        if request_index == 0 {
            None
        } else {
            Some(self.interval)
        }
    }

    pub(crate) async fn wait_before_request(&self, request_index: usize) {
        if let Some(delay) = self.delay_before_request(request_index) {
            tokio::time::sleep(delay).await;
        }
    }
}
