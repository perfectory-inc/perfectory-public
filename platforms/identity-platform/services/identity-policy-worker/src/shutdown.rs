//! Graceful shutdown state for draining one in-progress claim tick.

use std::future::Future;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};

use crate::worker::{DeliveryWorker, TickStats, WorkerError};

/// Runs bounded ticks until shutdown, draining a tick that has already started.
///
/// # Errors
/// Returns the shutdown listener error after any in-progress tick has drained.
pub async fn run_until_shutdown<S, E, F>(
    worker: &DeliveryWorker,
    poll_interval: Duration,
    shutdown: S,
    mut on_tick: F,
) -> Result<(), E>
where
    S: Future<Output = Result<(), E>>,
    F: FnMut(Result<TickStats, WorkerError>),
{
    let mut ticker = interval(poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::pin!(shutdown);

    'cycles: loop {
        tokio::select! {
            biased;
            signal = &mut shutdown => return signal,
            _ = ticker.tick() => {}
        }

        let mut stats = TickStats::default();
        for _ in 0..worker.max_rows_per_cycle() {
            tokio::select! {
                biased;
                signal = &mut shutdown => {
                    on_tick(Ok(stats));
                    return signal;
                }
                () = tokio::task::yield_now() => {}
            }

            let mut row = Box::pin(worker.process_next());
            let mut signal_result = None;
            let result = tokio::select! {
                result = &mut row => result,
                signal = &mut shutdown => {
                    signal_result = Some(signal);
                    row.await
                }
            };

            match result {
                Ok(Some(row_stats)) => stats.add(row_stats),
                Ok(None) => {
                    on_tick(Ok(stats));
                    if let Some(signal) = signal_result {
                        return signal;
                    }
                    continue 'cycles;
                }
                Err(error) => {
                    on_tick(Err(error));
                    if let Some(signal) = signal_result {
                        return signal;
                    }
                    continue 'cycles;
                }
            }

            if let Some(signal) = signal_result {
                on_tick(Ok(stats));
                return signal;
            }
        }
        on_tick(Ok(stats));
    }
}
