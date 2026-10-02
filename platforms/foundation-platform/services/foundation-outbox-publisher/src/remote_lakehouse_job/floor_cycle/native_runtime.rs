//! Deployment addresses change between host and Docker; credentials have one source.
use std::path::PathBuf;

use anyhow::{ensure, Context};
use foundation_outbox_publisher::building_register_floor_silver_export::{
    HistoryWitness, HISTORY_CONTAINER_PATH, HISTORY_PATH_ENV, HISTORY_SHA256_ENV,
};
use reqwest::Url;

use super::LocalExecution;

pub(super) fn configuration(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<(String, String)> {
    let network =
        super::super::required_lookup(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK")?;
    ensure!(
        !matches!(network.as_str(), "host" | "none" | "bridge")
            && network
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b)),
        "FLOOR requires an existing user-defined database network"
    );
    let endpoint =
        super::super::required_lookup(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_ENDPOINT")?;
    parse_endpoint(&endpoint)?;
    Ok((network, endpoint))
}

fn parse_endpoint(endpoint: &str) -> anyhow::Result<Url> {
    let url =
        Url::parse(&format!("postgres://{endpoint}")).context("invalid FLOOR database endpoint")?;
    ensure!(
        url.host_str().is_some()
            && url.port().is_some_and(|port| port != 0)
            && url.username().is_empty()
            && url.password().is_none()
            && url.path().is_empty()
            && url.query().is_none()
            && url.fragment().is_none()
            && !endpoint.chars().any(char::is_whitespace),
        "FLOOR database endpoint must contain only host:port"
    );
    ensure!(
        !matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")),
        "FLOOR database endpoint must be reachable on the database network"
    );
    Ok(url)
}

pub(super) fn database_url(source: &str, endpoint: &str) -> anyhow::Result<String> {
    let target = parse_endpoint(endpoint)?;
    let mut url = Url::parse(source).context("invalid host DATABASE_URL")?;
    ensure!(
        matches!(url.scheme(), "postgres" | "postgresql")
            && url.host_str().is_some()
            && !url
                .query_pairs()
                .any(|(key, _)| matches!(key.as_ref(), "host" | "hostaddr" | "port")),
        "DATABASE_URL must use its authority for routing"
    );
    url.set_host(target.host_str())
        .context("invalid database hostname")?;
    url.set_port(target.port())
        .map_err(|()| anyhow::anyhow!("invalid database port"))?;
    Ok(url.into())
}

pub(super) fn override_path(local: &LocalExecution) -> PathBuf {
    local.outcome.with_file_name("native-compose.json")
}

pub(super) fn write_override(
    local: &LocalExecution,
    history: &HistoryWitness,
) -> anyhow::Result<()> {
    // JSON is also valid Compose YAML. Only validated deployment bindings are projected;
    // The same raw witness is pinned into both children; no second copy or authority.
    // Escape Compose interpolation, not shell quoting, for the operator's absolute path.
    let mount = serde_json::json!({
        "type": "bind", "source": history.path().to_str().context("FLOOR history path is not UTF-8")?.replace('$', "$$"),
        "target": HISTORY_CONTAINER_PATH, "read_only": true,
        "bind": {"create_host_path": false}
    });
    let environment = serde_json::json!({
        (HISTORY_PATH_ENV): HISTORY_CONTAINER_PATH,
        (HISTORY_SHA256_ENV): history.sha256()
    });
    let value = serde_json::json!({
        "services": {"lakehouse-control": {
            "image": local.image,
            "networks": ["floor-database"],
            "volumes": [mount], "environment": environment
        }, "spark": {"volumes": [mount], "environment": environment}},
        "networks": {"floor-database": {
            "external": true,
            "name": local.database_network
        }}
    });
    std::fs::write(override_path(local), serde_json::to_vec(&value)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_children_receive_the_same_read_only_witness_and_parent_digest() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let root = root.path().canonicalize()?;
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../../infra/lakehouse/spark/tests/fixtures/building_register_floor_history.json",
        );
        let path = root.join("history $literal.json");
        std::fs::copy(fixture, &path)?;
        let history = HistoryWitness::load(&path, None)?;
        let local = LocalExecution {
            state_root: root.clone(),
            ivy_cache: root.join("ivy"),
            image: format!("sha256:{}", "a".repeat(64)),
            database_network: "fixture_default".into(),
            database_endpoint: "postgres:5432".into(),
            project: "fixture".into(),
            outcome: root.join("outcome.json"),
        };
        write_override(&local, &history)?;
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(override_path(&local))?)?;
        for service in ["lakehouse-control", "spark"] {
            let service = &value["services"][service];
            assert_eq!(
                service["environment"][HISTORY_PATH_ENV],
                HISTORY_CONTAINER_PATH
            );
            assert_eq!(service["environment"][HISTORY_SHA256_ENV], history.sha256());
            assert_eq!(service["volumes"].as_array().context("mounts")?.len(), 1);
            let mount = &service["volumes"][0];
            assert_eq!(mount["type"], "bind");
            assert_eq!(
                mount["source"],
                path.to_str().context("path")?.replace('$', "$$")
            );
            assert_eq!(mount["target"], HISTORY_CONTAINER_PATH);
            assert_eq!(mount["read_only"], true);
            assert_eq!(mount["bind"]["create_host_path"], false);
        }
        assert_eq!(value["services"]["lakehouse-control"]["image"], local.image);
        assert!(value["services"]["spark"].get("image").is_none());
        assert_eq!(
            value["networks"]["floor-database"]["name"],
            local.database_network
        );
        Ok(())
    }

    #[test]
    fn container_address_preserves_one_credential_and_database_source() -> anyhow::Result<()> {
        let original = "postgresql://reader:p%40ss@127.0.0.1:15434/foundation?sslmode=require&application_name=floor";
        let actual = database_url(original, "postgres:5432")?;
        assert_eq!(actual, "postgresql://reader:p%40ss@postgres:5432/foundation?sslmode=require&application_name=floor");
        assert_eq!(Url::parse(original)?.host_str(), Some("127.0.0.1"));
        Ok(())
    }

    #[test]
    fn ambiguous_routes_and_credentials_in_endpoint_are_rejected() {
        for endpoint in [
            "postgres",
            "postgres:0",
            "u:p@postgres:5432",
            "postgres:5432/db",
            "postgres:5432?x=y",
            "postgres:5432#x",
            "localhost:5432",
            "127.0.0.1:5432",
            "[::1]:5432",
            "postgres:5432\n",
        ] {
            assert!(parse_endpoint(endpoint).is_err(), "accepted {endpoint}");
        }
        for source in [
            "https://host/db",
            "postgres://host/db?host=other",
            "postgres://host/db?port=1",
            "postgres://host/db?hostaddr=127.0.0.1",
        ] {
            assert!(database_url(source, "postgres:5432").is_err());
        }
    }

    #[test]
    fn database_network_cannot_disable_container_isolation() {
        for network in ["host", "none", "bridge", "${OTHER}", "net\nname"] {
            let mut lookup = |key: &str| {
                Some(
                    if key.ends_with("NETWORK") {
                        network
                    } else {
                        "postgres:5432"
                    }
                    .to_owned(),
                )
            };
            assert!(configuration(&mut lookup).is_err());
        }
    }
}
