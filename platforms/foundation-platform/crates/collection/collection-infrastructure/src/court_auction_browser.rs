//! A pool of browser sessions to 법원경매정보 (courtauction.go.kr), over `wreq` (root ADR-0289 §1-§2).
//!
//! A session is one `wreq` client with the contract's browser profile and its own cookie jar. It is
//! opened by reading the site's first page, which hands out fresh session cookies; the site blocks
//! cookies, not addresses, so renewing a blocked session is dropping its client. The pool never
//! uses a proxy (environment proxies are switched off) and never follows a redirect: a redirect is
//! a block signal the caller reads.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex as StdMutex, PoisonError,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use collection_application::{
    CourtAuctionSessions, SessionRecord, SessionTicket, SiteMethod, SiteReply, SiteRequest,
    TransportError,
};
use tokio::sync::Mutex;
use wreq_util::Emulation;

/// What the pool needs from the contract.
#[derive(Clone, Debug)]
pub struct BrowserSessionConfig {
    /// The site's origin, without a trailing slash.
    pub base_url: String,
    /// The browser profile name, as `wreq-util` spells it (`chrome_149`).
    pub browser_profile: String,
    /// How many sessions run side by side.
    pub pool_size: usize,
    /// The page whose GET opens a session.
    pub opening_path: String,
    /// The time one request may take.
    pub request_timeout: Duration,
}

struct Slot {
    generation: u64,
    client: Option<(wreq::Client, usize)>,
}

/// The `wreq` pool behind [`CourtAuctionSessions`].
pub struct BrowserSessionPool {
    config: BrowserSessionConfig,
    emulation: Emulation,
    slots: Vec<Mutex<Slot>>,
    next: AtomicUsize,
    records: StdMutex<Vec<SessionRecord>>,
}

fn transport(error: impl std::fmt::Display) -> TransportError {
    TransportError(error.to_string())
}

impl BrowserSessionPool {
    /// A pool with no session open yet; the first request on each slot opens one.
    ///
    /// # Errors
    /// Returns an error when the profile is not one `wreq-util` knows or the pool size is 0.
    pub fn new(config: BrowserSessionConfig) -> Result<Self, TransportError> {
        let emulation: Emulation =
            serde_json::from_value(serde_json::Value::String(config.browser_profile.clone()))
                .map_err(|error| {
                    transport(format!(
                        "browser profile {:?} is not one wreq-util knows: {error}",
                        config.browser_profile
                    ))
                })?;
        if config.pool_size == 0 {
            return Err(transport("the session pool needs at least one session"));
        }
        let slots = (0..config.pool_size)
            .map(|_| {
                Mutex::new(Slot {
                    generation: 0,
                    client: None,
                })
            })
            .collect();
        Ok(Self {
            config,
            emulation,
            slots,
            next: AtomicUsize::new(0),
            records: StdMutex::new(Vec::new()),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.config.base_url.trim_end_matches('/'))
    }

    fn records(&self) -> std::sync::MutexGuard<'_, Vec<SessionRecord>> {
        self.records.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn open(
        &self,
        slot: usize,
        generation: u64,
    ) -> Result<(wreq::Client, usize), TransportError> {
        let client = wreq::Client::builder()
            .emulation(self.emulation)
            .cookie_store(true)
            .redirect(wreq::redirect::Policy::none())
            .no_proxy()
            .timeout(self.config.request_timeout)
            .build()
            .map_err(transport)?;
        client
            .get(self.url(&self.config.opening_path))
            .send()
            .await
            .map_err(transport)?
            .bytes()
            .await
            .map_err(transport)?;
        let mut records = self.records();
        records.push(SessionRecord {
            slot,
            generation,
            opened_at: Utc::now(),
            requests: 0,
            blocked_at: None,
        });
        tracing::info!(slot, generation, "courtauction.go.kr: opened a session");
        Ok((client, records.len() - 1))
    }
}

#[async_trait]
impl CourtAuctionSessions for BrowserSessionPool {
    async fn send(
        &self,
        request: &SiteRequest,
    ) -> Result<(SessionTicket, SiteReply), TransportError> {
        let slot = self.next.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let (client, record, generation) = {
            let mut held = self.slots[slot].lock().await;
            if held.client.is_none() {
                held.client = Some(self.open(slot, held.generation).await?);
            }
            let (client, record) = held.client.clone().expect("opened above");
            (client, record, held.generation)
        };
        let url = self.url(&request.path);
        let mut builder = match request.method {
            SiteMethod::Get => client.get(url),
            SiteMethod::Post => client.post(url),
        };
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if let Some(body) = &request.body {
            builder = builder.body(serde_json::to_vec(body).map_err(transport)?);
        }
        self.records()[record].requests += 1;
        let response = builder.send().await.map_err(transport)?;
        let status = response.status().as_u16();
        let body = response.bytes().await.map_err(transport)?;
        Ok((
            SessionTicket { slot, generation },
            SiteReply {
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            },
        ))
    }

    async fn renew(&self, ticket: SessionTicket) {
        let mut held = self.slots[ticket.slot].lock().await;
        if held.generation != ticket.generation {
            return;
        }
        if let Some((_, record)) = held.client.take() {
            self.records()[record].blocked_at = Some(Utc::now());
        }
        held.generation += 1;
    }

    fn sessions(&self) -> Vec<SessionRecord> {
        self.records().clone()
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        matchers::{header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use super::*;

    fn pool(server: &MockServer) -> BrowserSessionPool {
        BrowserSessionPool::new(BrowserSessionConfig {
            base_url: server.uri(),
            browser_profile: "chrome_149".to_owned(),
            pool_size: 1,
            opening_path: "/pgj/index.on".to_owned(),
            request_timeout: Duration::from_secs(5),
        })
        .expect("a known profile")
    }

    fn search() -> SiteRequest {
        SiteRequest {
            method: SiteMethod::Post,
            path: "/search".to_owned(),
            headers: vec![("SC-Userid".to_owned(), "NONUSER".to_owned())],
            body: Some(serde_json::json!({"page": 1})),
        }
    }

    #[test]
    fn an_unknown_profile_or_an_empty_pool_is_refused() {
        let config = |profile: &str, size| BrowserSessionConfig {
            base_url: "http://127.0.0.1:9".to_owned(),
            browser_profile: profile.to_owned(),
            pool_size: size,
            opening_path: "/".to_owned(),
            request_timeout: Duration::from_secs(1),
        };
        assert!(BrowserSessionPool::new(config("netscape_4", 1)).is_err());
        assert!(BrowserSessionPool::new(config("chrome_149", 0)).is_err());
    }

    #[tokio::test]
    async fn a_session_opens_on_the_first_page_carries_its_cookie_and_renewal_opens_a_new_one() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pgj/index.on"))
            .respond_with(
                ResponseTemplate::new(200).insert_header("set-cookie", "JSESSIONID=s1; Path=/"),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/search"))
            .and(header("cookie", "JSESSIONID=s1"))
            .and(header("SC-Userid", "NONUSER"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"status":200,"data":{}}"#))
            .expect(3)
            .mount(&server)
            .await;
        let pool = pool(&server);
        let (first, reply) = pool.send(&search()).await.expect("answered");
        assert_eq!(reply.status, 200);
        let (second, _) = pool.send(&search()).await.expect("answered");
        assert_eq!(
            first, second,
            "one open session serves every request on its slot"
        );

        pool.renew(first).await;
        // A second renewal of the same generation (another request saw the same block) is a no-op.
        pool.renew(first).await;
        let (third, _) = pool.send(&search()).await.expect("answered");
        assert_eq!(third.generation, first.generation + 1);

        let sessions = pool.sessions();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].requests, 2);
        assert!(sessions[0].blocked_at.is_some());
        assert_eq!(sessions[1].requests, 1);
        assert!(sessions[1].blocked_at.is_none());
    }

    #[tokio::test]
    async fn a_redirect_is_handed_back_not_followed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pgj/index.on"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/blocked"))
            .mount(&server)
            .await;
        let (_, reply) = pool(&server).send(&search()).await.expect("answered");
        assert_eq!(reply.status, 302);
    }
}
