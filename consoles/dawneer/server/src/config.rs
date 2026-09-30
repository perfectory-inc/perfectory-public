//! What the Dawneer server needs to know, read once at start.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context};
use url::Url;

/// Server settings. Every value comes from the environment; the client secret from a file.
#[derive(Clone, Debug)]
pub struct Config {
    /// Where the server listens.
    pub bind: SocketAddr,
    /// The origin staff open in the browser; the OIDC redirect URI hangs off it.
    pub public_base: Url,
    /// The Zitadel issuer (root ADR-0116 §5: the production issuer through a tunnel).
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
        let client_file = var("DAWNEER_OIDC_CLIENT_FILE")?;
        let (client_id, client_secret) = read_client_file(&client_file)?;
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
            client_id,
            client_secret,
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

/// Reads `OIDC_CLIENT_ID` and `OIDC_CLIENT_SECRET` from the file `configure-zitadel.sh` wrote.
fn read_client_file(path: &str) -> anyhow::Result<(String, String)> {
    let text = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    let value = |key: &str| {
        text.lines()
            .find_map(|line| {
                line.strip_prefix(key)
                    .and_then(|rest| rest.strip_prefix('='))
            })
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
            .with_context(|| format!("{path} has no {key}"))
    };
    Ok((value("OIDC_CLIENT_ID")?, value("OIDC_CLIENT_SECRET")?))
}
