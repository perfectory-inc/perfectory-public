//! Where signed-in staff sessions live (root ADR-0116 §1, §7).
//!
//! The first console is one process on one laptop, so sessions are in memory behind one lock.
//! A shared store replaces this when Dawneer runs as several processes; callers only see the
//! methods below.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::oidc::Identity;

/// A sign-in that has left for the issuer and not come back yet.
const PENDING_TTL: Duration = Duration::from_mins(10);
/// The longest a session lives, however active (a working day with margin).
const SESSION_TTL: Duration = Duration::from_hours(12);

/// A sign-in in flight, keyed by its `state`.
#[derive(Clone, Debug)]
pub struct PendingLogin {
    /// PKCE verifier.
    pub verifier: String,
    created: Instant,
}

/// One signed-in staff member.
#[derive(Clone, Debug)]
pub struct Session {
    /// Who.
    pub identity: Identity,
    /// Access token Foundation verifies.
    pub access_token: String,
    /// Refresh token.
    pub refresh_token: Option<String>,
    /// When the access token stops working.
    pub access_expires: Instant,
    /// ID token, only to hint the sign-out.
    pub id_token: Option<String>,
    /// The value every state-changing request must echo (root ADR-0116 §3).
    pub csrf: String,
    created: Instant,
}

impl Session {
    /// A new session created now.
    #[must_use]
    pub fn new(
        identity: Identity,
        access_token: String,
        refresh_token: Option<String>,
        expires_in: Duration,
        id_token: Option<String>,
        csrf: String,
    ) -> Self {
        let now = Instant::now();
        Self {
            identity,
            access_token,
            refresh_token,
            access_expires: now + expires_in,
            id_token,
            csrf,
            created: now,
        }
    }
}

/// Sessions and sign-ins in flight.
#[derive(Default)]
pub struct SessionStore {
    pending: Mutex<HashMap<String, PendingLogin>>,
    sessions: Mutex<HashMap<String, Session>>,
}

impl SessionStore {
    /// Remembers a sign-in that is leaving for the issuer.
    pub async fn begin(&self, state: String, verifier: String) {
        let mut pending = self.pending.lock().await;
        pending.retain(|_, p| p.created.elapsed() < PENDING_TTL);
        pending.insert(
            state,
            PendingLogin {
                verifier,
                created: Instant::now(),
            },
        );
    }

    /// Takes the sign-in for `state` — once; a replayed callback finds nothing.
    pub async fn finish(&self, state: &str) -> Option<PendingLogin> {
        self.pending
            .lock()
            .await
            .remove(state)
            .filter(|p| p.created.elapsed() < PENDING_TTL)
    }

    /// Stores a session under `id`.
    pub async fn insert(&self, id: String, session: Session) {
        let mut sessions = self.sessions.lock().await;
        sessions.retain(|_, s| s.created.elapsed() < SESSION_TTL);
        sessions.insert(id, session);
    }

    /// The live session for `id`.
    pub async fn get(&self, id: &str) -> Option<Session> {
        let mut sessions = self.sessions.lock().await;
        match sessions.get(id) {
            Some(s) if s.created.elapsed() < SESSION_TTL => Some(s.clone()),
            Some(_) => {
                sessions.remove(id);
                None
            }
            None => None,
        }
    }

    /// Replaces the tokens of a live session after a refresh.
    pub async fn update_tokens(
        &self,
        id: &str,
        access_token: String,
        refresh_token: Option<String>,
        expires_in: Duration,
    ) {
        if let Some(session) = self.sessions.lock().await.get_mut(id) {
            session.access_token = access_token;
            if refresh_token.is_some() {
                session.refresh_token = refresh_token;
            }
            session.access_expires = Instant::now() + expires_in;
        }
    }

    /// Ends a session.
    pub async fn remove(&self, id: &str) -> Option<Session> {
        self.sessions.lock().await.remove(id)
    }
}
