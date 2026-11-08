//! Asking 법원경매정보 (courtauction.go.kr) one request until it answers (root ADR-0289 §2-§4).
//!
//! The port is a pool of browser sessions ([`CourtAuctionSessions`]); infrastructure implements it.
//! [`ask`] is the use case: it reads every answer with the contract's block signals
//! (`collection_domain::court_auction::classify`), and on a block it waits the contract's next
//! step, renews the session that was blocked and asks again, with no bound on how many times. A
//! failure that is not a block is retried the contract's number of times, then handed back so the
//! caller records it. The only thing that ends the asking early is the run's own deadline: the work
//! stays undone and the next run asks it again.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, PoisonError,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use collection_domain::court_auction::{classify, Answer, BlockSignals, Pace};
use serde::Serialize;
use serde_json::Value;
use tokio::{sync::Notify, time::Instant};

/// The HTTP method of one site request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SiteMethod {
    /// A page or file read.
    Get,
    /// A search or detail call with a JSON body.
    Post,
}

/// One request, shaped as the site's own screen sends it.
#[derive(Clone, Debug, PartialEq)]
#[allow(
    clippy::derive_partial_eq_without_eq,
    reason = "body holds serde_json::Value, which is not Eq"
)]
pub struct SiteRequest {
    /// GET or POST.
    pub method: SiteMethod,
    /// The path under the contract's base URL.
    pub path: String,
    /// Headers this endpoint sends besides the browser profile's own.
    pub headers: Vec<(String, String)>,
    /// The JSON body of a POST.
    pub body: Option<Value>,
}

/// One answer as received.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SiteReply {
    /// The HTTP status.
    pub status: u16,
    /// The body, as text.
    pub body: String,
}

/// Which pooled session sent a request. Renewing an older generation than the slot holds does
/// nothing: another request already renewed it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionTicket {
    /// The pool slot.
    pub slot: usize,
    /// The session generation the slot held when the request was sent.
    pub generation: u64,
}

/// What one session did while it lived, for the run's manifest (root ADR-0289 §2).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionRecord {
    /// The pool slot.
    pub slot: usize,
    /// The generation of that slot.
    pub generation: u64,
    /// When the session was opened.
    pub opened_at: DateTime<Utc>,
    /// Requests sent on it, the opening request not counted.
    pub requests: u64,
    /// When it was renewed because the site blocked it.
    pub blocked_at: Option<DateTime<Utc>>,
}

/// The connection failed: no answer arrived.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct TransportError(pub String);

/// A pool of browser sessions to the site.
#[async_trait]
pub trait CourtAuctionSessions: Send + Sync {
    /// Sends one request on a pooled session, opening one first when the slot has none.
    async fn send(
        &self,
        request: &SiteRequest,
    ) -> Result<(SessionTicket, SiteReply), TransportError>;

    /// Drops the session `ticket` names, if the slot still holds it; the next request on the slot
    /// opens a fresh one, with new cookies.
    async fn renew(&self, ticket: SessionTicket);

    /// Every session the pool opened so far.
    fn sessions(&self) -> Vec<SessionRecord>;
}

/// How [`ask`] waits and retries, from the contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AskRules {
    /// What makes an answer a block.
    pub signals: BlockSignals,
    /// The waits after the first, second, ... block of one request; the last repeats for ever.
    pub block_waits: Vec<Duration>,
    /// How many times a failure that is not a block is asked again.
    pub failure_retries: u32,
    /// The first wait after such a failure; each next one doubles.
    pub failure_wait: Duration,
}

/// The live width of one endpoint group (AIMD, `collection_domain::court_auction::Pace`) and the
/// requests in flight under it.
#[derive(Debug)]
pub struct PaceGate {
    state: Mutex<(Pace, u32)>,
    freed: Notify,
}

/// One request's place under a [`PaceGate`]; dropping it frees the place.
#[derive(Debug)]
pub struct PacePermit<'a> {
    gate: &'a PaceGate,
}

impl Drop for PacePermit<'_> {
    fn drop(&mut self) {
        self.gate.lock().1 -= 1;
        self.gate.freed.notify_waiters();
    }
}

impl PaceGate {
    /// A gate at the pace's start width.
    #[must_use]
    pub fn new(pace: Pace) -> Self {
        Self {
            state: Mutex::new((pace, 0)),
            freed: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (Pace, u32)> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The width allowed now.
    #[must_use]
    pub fn width(&self) -> u32 {
        self.lock().0.width()
    }

    /// Waits until fewer requests than the width are in flight, then takes a place.
    pub async fn acquire(&self) -> PacePermit<'_> {
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            let taken = {
                let mut state = self.lock();
                let free = state.1 < state.0.width();
                state.1 += u32::from(free);
                free
            };
            if taken {
                return PacePermit { gate: self };
            }
            freed.await;
        }
    }

    fn clear(&self, elapsed: Duration) {
        self.lock().0.clear(elapsed);
        self.freed.notify_waiters();
    }

    fn setback(&self) {
        self.lock().0.setback();
    }
}

/// What one run's asking came to, counted as it happens (root ADR-0289 §2-§3).
#[derive(Debug, Default)]
pub struct AskTally {
    requests: AtomicU64,
    answered: AtomicU64,
    blocks: AtomicU64,
    renewals: AtomicU64,
    failure_retries: AtomicU64,
    failed: AtomicU64,
    unfinished: AtomicU64,
}

/// A snapshot of an [`AskTally`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AskCounts {
    /// Requests sent, every retry counted.
    pub requests: u64,
    /// Requests that ended with the site's data.
    pub answered: u64,
    /// Answers that were blocks.
    pub blocks: u64,
    /// Sessions renewed after a block.
    pub renewals: u64,
    /// Failures that were not blocks and were asked again.
    pub failure_retries: u64,
    /// Requests that failed past their retries and went to the failure ledger.
    pub failed: u64,
    /// Requests the run's deadline ended before they were answered.
    pub unfinished: u64,
}

impl AskTally {
    /// What was counted so far.
    #[must_use]
    pub fn counts(&self) -> AskCounts {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        AskCounts {
            requests: read(&self.requests),
            answered: read(&self.answered),
            blocks: read(&self.blocks),
            renewals: read(&self.renewals),
            failure_retries: read(&self.failure_retries),
            failed: read(&self.failed),
            unfinished: read(&self.unfinished),
        }
    }
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// How one request ended.
#[derive(Clone, Debug, PartialEq)]
#[allow(
    clippy::derive_partial_eq_without_eq,
    reason = "Asked::Data holds serde_json::Value, which is not Eq"
)]
pub enum Asked {
    /// The site's payload object, with the answer it came in.
    Data {
        /// The payload (`data`).
        data: Value,
        /// The answer as received, for Bronze.
        reply: SiteReply,
    },
    /// A failure that is not a block, past its retries: the caller records it in the ledger.
    Failed {
        /// Why the last attempt failed.
        reason: String,
        /// The last answer, when one arrived.
        reply: Option<SiteReply>,
    },
    /// The run's deadline came first. Not a failure: the next run asks again.
    Unfinished,
}

/// Asks `request` until the site answers it, the failure retries run out, or `deadline` passes.
pub async fn ask(
    sessions: &dyn CourtAuctionSessions,
    gate: &PaceGate,
    rules: &AskRules,
    tally: &AskTally,
    request: &SiteRequest,
    deadline: Instant,
) -> Asked {
    let mut blocks = 0_usize;
    let mut failures = 0_u32;
    loop {
        if Instant::now() >= deadline {
            bump(&tally.unfinished);
            return Asked::Unfinished;
        }
        let sent = {
            let _permit = gate.acquire().await;
            bump(&tally.requests);
            let started = Instant::now();
            sessions
                .send(request)
                .await
                .map(|(ticket, reply)| (ticket, reply, started.elapsed()))
        };
        let (reason, reply) = match sent {
            Ok((ticket, reply, elapsed)) => {
                match classify(&rules.signals, reply.status, &reply.body) {
                    Answer::Data(data) => {
                        gate.clear(elapsed);
                        bump(&tally.answered);
                        return Asked::Data { data, reply };
                    }
                    Answer::Blocked(why) => {
                        gate.setback();
                        bump(&tally.blocks);
                        let wait = rules
                            .block_waits
                            .get(blocks)
                            .or_else(|| rules.block_waits.last())
                            .copied()
                            .unwrap_or_default();
                        blocks += 1;
                        tracing::warn!(path = %request.path, ?why, blocks, wait_seconds = wait.as_secs(), "courtauction.go.kr blocked the session; renewing it after the wait");
                        tokio::time::sleep(
                            wait.min(deadline.saturating_duration_since(Instant::now())),
                        )
                        .await;
                        sessions.renew(ticket).await;
                        bump(&tally.renewals);
                        continue;
                    }
                    Answer::Failed(why) => (format!("{why:?}"), Some(reply)),
                }
            }
            Err(error) => (error.0, None),
        };
        if failures >= rules.failure_retries {
            bump(&tally.failed);
            return Asked::Failed { reason, reply };
        }
        bump(&tally.failure_retries);
        let wait = rules
            .failure_wait
            .saturating_mul(2_u32.saturating_pow(failures));
        failures += 1;
        tracing::warn!(path = %request.path, %reason, failures, "courtauction.go.kr request failed; asking again");
        tokio::time::sleep(wait.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::VecDeque;

    use collection_domain::court_auction::PaceRule;

    use super::*;

    /// Answers from a script; a block's renewal bumps the generation, as a real pool does.
    struct Scripted {
        replies: Mutex<VecDeque<Result<SiteReply, TransportError>>>,
        generation: Mutex<u64>,
        renewed: Mutex<Vec<SessionTicket>>,
    }

    impl Scripted {
        fn new(replies: Vec<Result<(u16, &str), &str>>) -> Self {
            Self {
                replies: Mutex::new(
                    replies
                        .into_iter()
                        .map(|reply| {
                            reply
                                .map(|(status, body)| SiteReply {
                                    status,
                                    body: body.to_owned(),
                                })
                                .map_err(|error| TransportError(error.to_owned()))
                        })
                        .collect(),
                ),
                generation: Mutex::new(0),
                renewed: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl CourtAuctionSessions for Scripted {
        async fn send(
            &self,
            _request: &SiteRequest,
        ) -> Result<(SessionTicket, SiteReply), TransportError> {
            let generation = *self.generation.lock().unwrap();
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("script ran out");
            reply.map(|reply| {
                (
                    SessionTicket {
                        slot: 0,
                        generation,
                    },
                    reply,
                )
            })
        }

        async fn renew(&self, ticket: SessionTicket) {
            self.renewed.lock().unwrap().push(ticket);
            *self.generation.lock().unwrap() += 1;
        }

        fn sessions(&self) -> Vec<SessionRecord> {
            Vec::new()
        }
    }

    const BLOCK: &str = r#"{"status":200,"message":"blocked","data":{"ipcheck":false}}"#;
    const GOOD: &str = r#"{"status":200,"data":{"rows":[1]}}"#;

    fn rules() -> AskRules {
        AskRules {
            signals: BlockSignals {
                http_statuses: vec![302, 403, 429],
                ipcheck_pointer: "/data/ipcheck".to_owned(),
                data_pointer: "/data".to_owned(),
                status_pointer: "/status".to_owned(),
                ok_status: 200,
            },
            block_waits: vec![Duration::from_millis(5), Duration::from_millis(15)],
            failure_retries: 2,
            failure_wait: Duration::from_millis(1),
        }
    }

    fn gate() -> PaceGate {
        PaceGate::new(Pace::new(PaceRule {
            start: 4,
            min: 1,
            max: 8,
            increase_after: 100,
            decrease_divisor: 2,
            slow_response_millis: 60_000,
        }))
    }

    fn request() -> SiteRequest {
        SiteRequest {
            method: SiteMethod::Post,
            path: "/search".to_owned(),
            headers: Vec::new(),
            body: None,
        }
    }

    fn far() -> Instant {
        Instant::now() + Duration::from_hours(24)
    }

    #[tokio::test]
    async fn an_ipcheck_block_renews_the_session_and_asks_again_until_answered() {
        // More blocks than the wait ladder has steps: there is no bound; the last wait repeats.
        let sessions = Scripted::new(vec![
            Ok((200, BLOCK)),
            Ok((200, BLOCK)),
            Ok((200, BLOCK)),
            Ok((302, "")),
            Ok((200, GOOD)),
        ]);
        let (gate, tally) = (gate(), AskTally::default());
        let started = Instant::now();
        let asked = ask(&sessions, &gate, &rules(), &tally, &request(), far()).await;
        assert_eq!(
            asked,
            Asked::Data {
                data: serde_json::json!({"rows": [1]}),
                reply: SiteReply {
                    status: 200,
                    body: GOOD.to_owned()
                },
            }
        );
        // The ladder ran out after two steps and its last step repeated.
        assert!(started.elapsed() >= Duration::from_millis(5 + 15 + 15 + 15));
        let generations: Vec<u64> = sessions
            .renewed
            .lock()
            .unwrap()
            .iter()
            .map(|ticket| ticket.generation)
            .collect();
        assert_eq!(generations, vec![0, 1, 2, 3]);
        let counts = tally.counts();
        assert_eq!(
            (
                counts.requests,
                counts.blocks,
                counts.renewals,
                counts.answered
            ),
            (5, 4, 4, 1)
        );
        // Every block halved the width, down to the floor.
        assert_eq!(gate.width(), 1);
    }

    #[tokio::test]
    async fn a_failure_that_is_not_a_block_is_retried_then_handed_back() {
        let sessions = Scripted::new(vec![Err("reset"), Ok((500, "oops")), Ok((500, "oops"))]);
        let tally = AskTally::default();
        let asked = ask(&sessions, &gate(), &rules(), &tally, &request(), far()).await;
        assert_eq!(
            asked,
            Asked::Failed {
                reason: "Status(500)".to_owned(),
                reply: Some(SiteReply {
                    status: 500,
                    body: "oops".to_owned()
                }),
            }
        );
        assert!(sessions.renewed.lock().unwrap().is_empty());
        let counts = tally.counts();
        assert_eq!(
            (counts.requests, counts.failure_retries, counts.failed),
            (3, 2, 1)
        );
    }

    #[tokio::test]
    async fn the_deadline_leaves_a_blocked_request_unfinished_not_failed() {
        let sessions = Scripted::new(vec![Ok((200, BLOCK)), Ok((200, BLOCK))]);
        let tally = AskTally::default();
        // Shorter than the first two waits: the second wait is cut at the deadline.
        let deadline = Instant::now() + Duration::from_millis(10);
        let asked = ask(&sessions, &gate(), &rules(), &tally, &request(), deadline).await;
        assert_eq!(asked, Asked::Unfinished);
        assert_eq!(tally.counts().unfinished, 1);
        assert_eq!(tally.counts().failed, 0);
    }

    #[tokio::test]
    async fn the_gate_holds_requests_beyond_its_width() {
        let gate = PaceGate::new(Pace::new(PaceRule {
            start: 1,
            min: 1,
            max: 1,
            increase_after: 1,
            decrease_divisor: 2,
            slow_response_millis: 60_000,
        }));
        let first = gate.acquire().await;
        let second = gate.acquire();
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), second.as_mut())
                .await
                .is_err()
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("a freed place lets the next request in");
    }
}
