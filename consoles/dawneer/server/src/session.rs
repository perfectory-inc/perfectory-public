//! Where signed-in staff sessions live (root ADR-0116 §1, §7).
//!
//! Records in a `tower-sessions` store: Postgres on the host (`tower-sessions-sqlx-store`, table
//! `tower_sessions.session`, made by the compose bootstrap — the role cannot create a schema, so
//! the store's own `migrate()` is not called), so a restart or a new release keeps everyone signed
//! in; memory for tests and a local http run. A record's key is the SHA-256 of the random value the
//! browser holds (the sign-in's `state`, or the session cookie), so the table never holds a value
//! that would sign anyone in. Callers only see the methods below.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tower_sessions_core::session::{Id, Record};
use tower_sessions_core::session_store::{self, SessionStore as Store};

use crate::oidc::{Identity, Tokens};

/// A sign-in that has left for the issuer and not come back yet.
const PENDING_TTL: Duration = Duration::from_mins(10);
/// The longest a session lives, however active (a working day with margin).
pub const SESSION_TTL: Duration = Duration::from_hours(12);
/// The one key a record's data holds.
const VALUE: &str = "v";

/// A sign-in in flight, keyed by its `state`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PendingLogin {
    /// PKCE verifier.
    pub verifier: String,
    /// The nonce the ID token must carry.
    pub nonce: String,
}

/// One signed-in staff member.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Session {
    /// Who.
    pub identity: Identity,
    /// Access token Foundation verifies.
    pub access_token: String,
    /// Refresh token.
    pub refresh_token: Option<String>,
    /// When the access token stops working.
    pub access_expires: SystemTime,
    /// ID token, only to hint the sign-out.
    pub id_token: Option<String>,
    /// The value every state-changing request must echo (root ADR-0116 §3).
    pub csrf: String,
    /// When the session began; it ends `SESSION_TTL` later.
    created: SystemTime,
}

impl Session {
    /// A new session created now from a verified sign-in; an access token whose lifetime the
    /// issuer did not say is taken to live `default_lifetime`.
    #[must_use]
    pub fn new(
        identity: Identity,
        tokens: Tokens,
        csrf: String,
        default_lifetime: Duration,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            identity,
            access_token: tokens.access_token,
            refresh_token: tokens.refresh_token,
            access_expires: now + tokens.expires_in.unwrap_or(default_lifetime),
            id_token: tokens.id_token,
            csrf,
            created: now,
        }
    }

    /// How long the access token still works (zero once it has stopped).
    #[must_use]
    pub fn access_remaining(&self) -> Duration {
        self.access_expires
            .duration_since(SystemTime::now())
            .unwrap_or_default()
    }
}

/// The store could not be read or written.
#[derive(Debug, thiserror::Error)]
#[error("session store: {0}")]
pub struct StoreError(String);

impl From<session_store::Error> for StoreError {
    fn from(error: session_store::Error) -> Self {
        Self(error.to_string())
    }
}

/// Sessions and sign-ins in flight.
#[derive(Clone, Debug)]
pub struct SessionStore {
    store: Arc<dyn Store>,
}

impl Default for SessionStore {
    /// In memory: tests and a local http run.
    fn default() -> Self {
        Self::new(tower_sessions_memory_store::MemoryStore::default())
    }
}

impl SessionStore {
    /// Sessions kept in `store`.
    pub fn new(store: impl Store) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    /// Remembers a sign-in that is leaving for the issuer.
    ///
    /// # Errors
    ///
    /// The store could not be written; the sign-in cannot start.
    pub async fn begin(
        &self,
        state: &str,
        verifier: String,
        nonce: String,
    ) -> Result<(), StoreError> {
        self.put(
            state,
            &PendingLogin { verifier, nonce },
            SystemTime::now() + PENDING_TTL,
        )
        .await
    }

    /// Takes the sign-in for `state` — once; a replayed callback finds nothing.
    pub async fn finish(&self, state: &str) -> Option<PendingLogin> {
        let pending = self.read(state).await;
        self.forget(state).await;
        pending
    }

    /// Stores a session under `id` (the cookie's value) until it ends.
    ///
    /// # Errors
    ///
    /// The store could not be written; the browser must not get a cookie for it.
    pub async fn insert(&self, id: &str, session: &Session) -> Result<(), StoreError> {
        self.put(id, session, session.created + SESSION_TTL).await
    }

    /// The live session for `id`.
    pub async fn get(&self, id: &str) -> Option<Session> {
        self.read(id).await
    }

    /// Replaces the tokens of a live session after a refresh.
    pub async fn update_tokens(
        &self,
        id: &str,
        access_token: String,
        refresh_token: Option<String>,
        expires_in: Duration,
    ) {
        let Some(mut session) = self.read::<Session>(id).await else {
            return;
        };
        session.access_token = access_token;
        if refresh_token.is_some() {
            session.refresh_token = refresh_token;
        }
        session.access_expires = SystemTime::now() + expires_in;
        if let Err(error) = self.insert(id, &session).await {
            tracing::error!(%error, "refreshed tokens were not stored");
        }
    }

    /// Ends a session.
    pub async fn remove(&self, id: &str) -> Option<Session> {
        let session = self.read(id).await;
        self.forget(id).await;
        session
    }

    async fn put<T: Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
        until: SystemTime,
    ) -> Result<(), StoreError> {
        let value = serde_json::to_value(value).map_err(|e| StoreError(e.to_string()))?;
        let record = Record {
            id: record_id(key),
            data: HashMap::from([(VALUE.to_owned(), value)]),
            expiry_date: OffsetDateTime::from(until),
        };
        self.store.save(&record).await?;
        Ok(())
    }

    async fn read<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        match self.store.load(&record_id(key)).await {
            Ok(Some(record)) if record.expiry_date > OffsetDateTime::now_utc() => record
                .data
                .get(VALUE)
                .and_then(|value| serde_json::from_value(value.clone()).ok()),
            Ok(_) => None,
            Err(error) => {
                tracing::error!(%error, "session store could not be read");
                None
            }
        }
    }

    async fn forget(&self, key: &str) {
        if let Err(error) = self.store.delete(&record_id(key)).await {
            tracing::error!(%error, "session store could not delete a record");
        }
    }
}

/// The record key for a browser-held value: the first 128 bits of its SHA-256.
fn record_id(key: &str) -> Id {
    let digest = Sha256::digest(key.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Id(i128::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(created: SystemTime) -> Session {
        Session {
            identity: Identity {
                sub: "1".to_owned(),
                name: "Synthetic".to_owned(),
                email: "s@example.test".to_owned(),
            },
            access_token: "at".to_owned(),
            refresh_token: Some("rt".to_owned()),
            access_expires: created + Duration::from_mins(5),
            id_token: None,
            csrf: "c".to_owned(),
            created,
        }
    }

    #[tokio::test]
    async fn a_sign_in_is_taken_once() -> Result<(), StoreError> {
        let store = SessionStore::default();
        store
            .begin("state-1", "v".to_owned(), "n".to_owned())
            .await?;
        assert!(store.finish("state-1").await.is_some());
        assert!(
            store.finish("state-1").await.is_none(),
            "a replay finds nothing"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_session_lives_until_it_ends_and_tokens_refresh_in_place() -> Result<(), StoreError> {
        let store = SessionStore::default();
        store
            .insert("cookie-1", &session(SystemTime::now()))
            .await?;
        store
            .update_tokens("cookie-1", "at2".to_owned(), None, Duration::from_mins(5))
            .await;
        let read = store.get("cookie-1").await;
        assert_eq!(read.as_ref().map(|s| s.access_token.as_str()), Some("at2"));
        assert_eq!(read.and_then(|s| s.refresh_token).as_deref(), Some("rt"));
        assert!(store.remove("cookie-1").await.is_some());
        assert!(store.get("cookie-1").await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_session_older_than_its_lifetime_is_gone() -> Result<(), StoreError> {
        let store = SessionStore::default();
        let old = SystemTime::now() - SESSION_TTL - Duration::from_secs(1);
        store.insert("cookie-old", &session(old)).await?;
        assert!(store.get("cookie-old").await.is_none());
        Ok(())
    }

    #[test]
    fn the_key_is_not_the_browser_value() {
        let Id(id) = record_id("cookie-1");
        assert_ne!(id.to_string(), "cookie-1");
        assert_eq!(record_id("cookie-1"), record_id("cookie-1"));
        assert_ne!(record_id("cookie-1"), record_id("cookie-2"));
    }
}
