//! The address a request came from: the rate-limit key and the audit row's `ip_address`.
//!
//! `X-Forwarded-For` is never read. Its first entry is whatever the caller wrote, and Cloudflare
//! only appends to it (Cloudflare docs, "Cloudflare HTTP headers": for the client address "look at
//! `CF-Connecting-IP` … instead of `X-Forwarded-For`").
//!
//! - `CLIENT_ADDRESS_HEADER` names the one header the deployment's edge sets to the client address:
//!   `cf-connecting-ip` behind Cloudflare. Name it only when the origin accepts nothing but the edge
//!   (Cloudflare docs: restrict the origin to Cloudflare's addresses); otherwise a caller writes it.
//! - Unset, or the header absent or not an address: the TCP peer (`ConnectInfo`).
//!
//! Production refuses to start without it (`startup::check_client_address_header`): behind a load
//! balancer the TCP peer is the balancer, and every user would share one rate-limit key.

use std::net::{IpAddr, SocketAddr};
use std::sync::LazyLock;

use axum::extract::ConnectInfo;
use axum::http::{Extensions, HeaderMap, HeaderName};

/// The environment variable naming the trusted edge header.
pub const CLIENT_ADDRESS_HEADER_ENV: &str = "CLIENT_ADDRESS_HEADER";

static TRUSTED_HEADER: LazyLock<Option<HeaderName>> = LazyLock::new(|| {
    trusted_header(std::env::var(CLIENT_ADDRESS_HEADER_ENV).ok().as_deref())
        .ok()
        .flatten()
});

/// The configured header name: `Ok(None)` when unset or blank, `Err` when it is not a header name.
pub fn trusted_header(value: Option<&str>) -> Result<Option<HeaderName>, String> {
    value
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map_or_else(
            || Ok(None),
            |name| {
                HeaderName::try_from(name.to_ascii_lowercase())
                    .map(Some)
                    .map_err(|_| format!("{CLIENT_ADDRESS_HEADER_ENV}={name} is not a header name"))
            },
        )
}

/// The request's client address, or `None` when neither the edge nor the socket gives one.
#[must_use]
pub fn client_address(headers: &HeaderMap, extensions: &Extensions) -> Option<IpAddr> {
    let peer = extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr);
    client_address_with(TRUSTED_HEADER.as_ref(), headers, peer)
}

fn client_address_with(
    trusted: Option<&HeaderName>,
    headers: &HeaderMap,
    peer: Option<SocketAddr>,
) -> Option<IpAddr> {
    trusted
        .and_then(|name| headers.get(name))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| parse_address(value.trim()))
        .or_else(|| peer.map(|addr| addr.ip()))
}

/// An address, with or without a port (`CloudFront-Viewer-Address` carries `ip:port`).
fn parse_address(value: &str) -> Option<IpAddr> {
    value
        .parse::<IpAddr>()
        .ok()
        .or_else(|| value.parse::<SocketAddr>().ok().map(|addr| addr.ip()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_static(value));
        }
        map
    }

    const PEER: [u8; 4] = [198, 51, 100, 20];
    const EDGE: [u8; 4] = [203, 0, 113, 7];

    fn peer() -> SocketAddr {
        SocketAddr::from((PEER, 51234))
    }

    #[test]
    fn a_spoofed_forwarded_for_never_becomes_the_key() {
        for spoof in ["6.6.6.6", "7.7.7.7, 198.51.100.20"] {
            let map = headers(&[("x-forwarded-for", spoof)]);
            assert_eq!(
                client_address_with(None, &map, Some(peer())),
                Some(IpAddr::from(PEER)),
                "{spoof}"
            );
        }
    }

    #[test]
    fn behind_cloudflare_the_edge_header_wins_over_forwarded_for() {
        let cf = HeaderName::from_static("cf-connecting-ip");
        let map = headers(&[
            ("cf-connecting-ip", "203.0.113.7"),
            ("x-forwarded-for", "6.6.6.6, 203.0.113.7"),
        ]);
        assert_eq!(
            client_address_with(Some(&cf), &map, Some(peer())),
            Some(IpAddr::from(EDGE))
        );
    }

    #[test]
    fn an_untrusted_edge_header_is_ignored() {
        let map = headers(&[("cf-connecting-ip", "6.6.6.6")]);
        assert_eq!(
            client_address_with(None, &map, Some(peer())),
            Some(IpAddr::from(PEER))
        );
    }

    #[test]
    fn a_missing_or_malformed_edge_header_falls_back_to_the_socket() {
        let cf = HeaderName::from_static("cf-connecting-ip");
        assert_eq!(
            client_address_with(Some(&cf), &HeaderMap::new(), Some(peer())),
            Some(IpAddr::from(PEER))
        );
        let map = headers(&[("cf-connecting-ip", "not-an-address")]);
        assert_eq!(
            client_address_with(Some(&cf), &map, Some(peer())),
            Some(IpAddr::from(PEER))
        );
        assert_eq!(client_address_with(Some(&cf), &map, None), None);
    }

    #[test]
    fn an_edge_header_with_a_port_gives_its_address() {
        let viewer = HeaderName::from_static("cloudfront-viewer-address");
        let map = headers(&[("cloudfront-viewer-address", "203.0.113.7:46532")]);
        assert_eq!(
            client_address_with(Some(&viewer), &map, None),
            Some(IpAddr::from(EDGE))
        );
    }
}
