//! Shared address policy for immutable static tile publication and production promotion.
use crate::runtime_environment::RuntimeEnvironment;
use anyhow::{ensure, Context as _};

/// The base of the tile URL written into the release. That URL is immutable once published and
/// is what browsers are told to fetch (ADR-0037: changing a serving address is a new publication),
/// so it must be an address a browser outside this host can reach. Platform ADR-0004 requires the
/// publish gate to refuse plain HTTP, loopback included; the first national bake recorded
/// `http://127.0.0.1:3111` because nothing did.
pub(crate) fn public_tiles_base_url(raw: &str) -> anyhow::Result<String> {
    let value = base_url(raw)?;
    let url = reqwest::Url::parse(&value).context("not a URL")?;
    ensure!(url.scheme() == "https", "must use https");
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "must not carry credentials"
    );
    let host = url
        .host_str()
        .context("has no host")?
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let internal = match host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
    {
        Ok(ip) => internal_ip(ip),
        Err(_) => host == "localhost" || host.ends_with(".localhost") || !host.contains('.'),
    };
    ensure!(
        !internal,
        "must name a host reachable from outside, not {value}"
    );
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "must not carry a query or fragment"
    );
    Ok(value)
}

fn internal_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_broadcast()
        }
        std::net::IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or_else(
            || {
                ip.is_loopback()
                    || ip.is_unspecified()
                    || ip.is_unicast_link_local()
                    || ip.is_multicast()
                    || (ip.segments()[0] & 0xfe00) == 0xfc00
            },
            |ipv4| internal_ip(std::net::IpAddr::V4(ipv4)),
        ),
    }
}

pub(crate) fn base_url(raw: &str) -> anyhow::Result<String> {
    let value = raw.trim_end_matches('/');
    ensure!(
        value.starts_with("http://") || value.starts_with("https://"),
        "Martin base URLs must use http or https"
    );
    Ok(value.to_owned())
}

/// Keep local/CI proofs usable while checking production static selections at the final gate.
pub(crate) fn guard_static_promotion_url(source_kind: &str, template: &str) -> anyhow::Result<()> {
    if source_kind != "static_pmtiles" {
        return Ok(());
    }
    let environment = RuntimeEnvironment::from_env()?;
    validate_static_promotion_url(environment, source_kind, template)
}

fn validate_static_promotion_url(
    environment: RuntimeEnvironment,
    source_kind: &str,
    template: &str,
) -> anyhow::Result<()> {
    if !matches!(
        environment,
        RuntimeEnvironment::Local | RuntimeEnvironment::Ci
    ) && source_kind == "static_pmtiles"
    {
        let address = template
            .strip_suffix("/{z}/{x}/{y}")
            .context("static tile template must end with /{z}/{x}/{y}")?;
        public_tiles_base_url(address)
            .context("staging/production static promotion requires a public tile address")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_environment::RUNTIME_ENVIRONMENT_ENV;
    #[test]
    fn static_guard_requires_explicit_environment_and_public_staging_address() {
        const CHILD: &str = "STATIC_URL_GUARD_TEST_CHILD";
        if let Ok(environment) = std::env::var(CHILD) {
            let result = guard_static_promotion_url(
                "static_pmtiles",
                "http://127.0.0.1:3111/parcels-test/{z}/{x}/{y}",
            );
            assert_eq!(
                result.is_ok(),
                matches!(environment.as_str(), "local" | "ci")
            );
            if environment == "missing" {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains(RUNTIME_ENVIRONMENT_ENV));
            }
            return;
        }
        for environment in ["missing", "staging", "production", "local", "ci", "invalid"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.args(["--exact", "static_release_url::tests::static_guard_requires_explicit_environment_and_public_staging_address", "--nocapture"])
                .env(CHILD, environment)
                .env_remove(RUNTIME_ENVIRONMENT_ENV);
            if environment != "missing" {
                command.env(RUNTIME_ENVIRONMENT_ENV, environment);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{environment}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    #[test]
    fn public_tile_address_refuses_what_a_browser_cannot_reach() {
        for refused in [
            "http://tiles.example.com",
            "https://127.0.0.1:3111",
            "https://localhost/tiles",
            "https://localhost./tiles",
            "https://tiles.localhost",
            "https://tiles.localhost./tiles",
            "https://169.254.1.1",
            "https://0.0.0.0",
            "https://[::1]:3111",
            "https://martin-static:3000",
            "https://operator:secret@tiles.example.com",
            "https://tiles.example.com/?cache=0",
            "ftp://tiles.example.com",
        ] {
            assert!(
                public_tiles_base_url(refused).is_err(),
                "{refused} must be refused as a public tile address"
            );
        }
        // The private ranges are built from octets: this repository is public and keeps no
        // private-network address in its text.
        for private in [
            std::net::Ipv4Addr::new(10, 0, 0, 5),
            std::net::Ipv4Addr::new(172, 16, 4, 1),
            std::net::Ipv4Addr::new(192, 168, 1, 1),
        ] {
            let refused = format!("https://{private}:3111");
            assert!(
                public_tiles_base_url(&refused).is_err(),
                "{refused} must be refused as a public tile address"
            );
            assert!(
                public_tiles_base_url(&format!("https://[{}]", private.to_ipv6_mapped())).is_err()
            );
        }
        for address in [
            std::net::Ipv4Addr::LOCALHOST.to_ipv6_mapped(),
            std::net::Ipv6Addr::new(0xfd12, 0, 0, 0, 0, 0, 0, 1),
            std::net::Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
        ] {
            assert!(public_tiles_base_url(&format!("https://[{address}]")).is_err());
        }
        assert_eq!(
            public_tiles_base_url("https://tiles.example.com/v1/")
                .ok()
                .as_deref(),
            Some("https://tiles.example.com/v1")
        );
    }

    #[test]
    fn production_guard_refuses_static_loopback_but_keeps_local_proofs() {
        let template = "http://127.0.0.1:3111/parcels-test/{z}/{x}/{y}";
        assert!(validate_static_promotion_url(
            RuntimeEnvironment::Production,
            "static_pmtiles",
            template
        )
        .is_err());
        for environment in [RuntimeEnvironment::Local, RuntimeEnvironment::Ci] {
            assert!(validate_static_promotion_url(environment, "static_pmtiles", template).is_ok());
        }
        assert!(validate_static_promotion_url(
            RuntimeEnvironment::Production,
            "dynamic_postgis",
            template
        )
        .is_ok());
        assert!(validate_static_promotion_url(
            RuntimeEnvironment::Production,
            "static_pmtiles",
            "https://tiles.example.com/parcels-test/{z}/{x}/{y}"
        )
        .is_ok());
    }
}
