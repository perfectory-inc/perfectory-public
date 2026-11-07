//! What the Dawneer server needs to know, read once at start.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context};
use url::Url;

/// Server settings. Every value comes from the environment. The client credentials come under the
/// names `configure-zitadel.sh` writes into the app's credentials file, which the host hands to the
/// container as an env file (root ADR-0434 §1), so the secret is never copied anywhere else.
#[derive(Clone, Debug)]
pub struct Config {
    /// Where the server listens.
    pub bind: SocketAddr,
    /// The origin staff open in the browser; the OIDC redirect URI hangs off it.
    pub public_base: Url,
    /// The Zitadel issuer on the loopback; the server's own calls go here, the browser never does
    /// (root ADR-0434 §2).
    pub issuer: Url,
    /// The Zitadel project id; tokens must carry it as audience for Foundation to accept them.
    pub project_id: String,
    /// OIDC client id of the Dawneer web app.
    pub client_id: String,
    /// OIDC client secret of the Dawneer web app.
    pub client_secret: String,
    /// Foundation API base.
    pub foundation_base: Url,
    /// Built screens (`web/dist`).
    pub web_dir: PathBuf,
}

impl Config {
    /// Reads the settings from the environment.
    ///
    /// # Errors
    /// Returns an error naming the missing or malformed setting.
    pub fn from_env() -> anyhow::Result<Self> {
        let var = |name: &str| std::env::var(name).with_context(|| format!("{name} is required"));
        let or =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
        let url = |name: &str, value: String| {
            Url::parse(&value).with_context(|| format!("{name} is not a URL"))
        };
        let config = Self {
            bind: or("DAWNEER_BIND", "127.0.0.1:3120")
                .parse()
                .context("DAWNEER_BIND is not an address")?,
            public_base: url(
                "DAWNEER_PUBLIC_BASE_URL",
                or("DAWNEER_PUBLIC_BASE_URL", "http://127.0.0.1:3120"),
            )?,
            issuer: url(
                "DAWNEER_ZITADEL_ISSUER_URL",
                var("DAWNEER_ZITADEL_ISSUER_URL")?,
            )?,
            project_id: var("DAWNEER_ZITADEL_PROJECT_ID")?,
            client_id: var("OIDC_CLIENT_ID")?,
            client_secret: var("OIDC_CLIENT_SECRET")?,
            foundation_base: url(
                "DAWNEER_FOUNDATION_API_BASE_URL",
                or("DAWNEER_FOUNDATION_API_BASE_URL", "http://127.0.0.1:18080"),
            )?,
            web_dir: PathBuf::from(or("DAWNEER_WEB_DIR", "web/dist")),
        };
        if !config.project_id.bytes().all(|b| b.is_ascii_digit()) || config.project_id.is_empty() {
            bail!("DAWNEER_ZITADEL_PROJECT_ID is a Zitadel project id (digits)");
        }
        Ok(config)
    }

    /// The redirect URI registered for this console.
    ///
    /// # Errors
    /// Returns an error when the public base cannot carry a path.
    pub fn redirect_uri(&self) -> anyhow::Result<Url> {
        self.public_base
            .join("/auth/callback")
            .context("DAWNEER_PUBLIC_BASE_URL cannot carry a path")
    }

    /// Whether cookies must be `Secure` (any origin other than plain http).
    #[must_use]
    pub fn secure_cookies(&self) -> bool {
        self.public_base.scheme() == "https"
    }
}
