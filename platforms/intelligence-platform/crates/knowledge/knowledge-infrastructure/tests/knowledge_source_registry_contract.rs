// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use knowledge_application::KnowledgeSourceRegistryPort;
use knowledge_domain::KnowledgeSourceUpserted;
use knowledge_infrastructure::{
    InMemoryKnowledgeSourceRegistry, PostgresKnowledgeSourceRegistry,
    PostgresKnowledgeSourceRegistryConfig,
};

#[tokio::test]
async fn memory_registry_upserts_sources_by_tenant_product_and_source() {
    knowledge_registry_contract_suite(
        InMemoryKnowledgeSourceRegistry::default(),
        "tenant-memory-contract",
    )
    .await;
}

#[tokio::test]
async fn postgres_registry_upserts_sources_by_tenant_product_and_source() {
    let Some(registry) = pg_registry_or_skip().await else {
        eprintln!(
            "skipping postgres_registry_upserts_sources_by_tenant_product_and_source: INTELLIGENCE_TEST_DATABASE_URL not set"
        );
        return;
    };

    let tenant_id = unique_tenant_id("pg-contract");
    knowledge_registry_contract_suite(registry, &tenant_id).await;
}

async fn knowledge_registry_contract_suite<R>(registry: R, tenant_id: &str)
where
    R: KnowledgeSourceRegistryPort,
{
    let first = registry
        .upsert_source(source_event("event-1", tenant_id, "product-1", "source-1"))
        .await
        .expect("first upsert must succeed");

    assert_eq!(first.tenant_id, tenant_id);
    assert_eq!(first.product_id, "product-1");
    assert_eq!(first.source_id, "source-1");
    assert_eq!(first.source_uri, "iceberg://foundation.silver.source_1");
    assert_eq!(first.last_event_id, "event-1");
    assert_eq!(first.version, 1);

    let replayed = registry
        .upsert_source(source_event("event-1", tenant_id, "product-1", "source-1"))
        .await
        .expect("same event replay must be idempotent");
    assert_eq!(
        replayed.version, 1,
        "redelivered Kafka event must not advance registry version"
    );

    let updated_event = KnowledgeSourceUpserted {
        event_id: "event-2".to_string(),
        source_uri: "iceberg://foundation.silver.source_1".to_string(),
        content_checksum_sha256: Some(
            "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        metadata: BTreeMap::from([
            ("schema_version".to_string(), "fixture.v2".to_string()),
            ("quality_tier".to_string(), "silver".to_string()),
        ]),
        ..source_event("event-1", tenant_id, "product-1", "source-1")
    };

    let updated = registry
        .upsert_source(updated_event)
        .await
        .expect("second upsert must succeed");

    assert_eq!(updated.source_uri, "iceberg://foundation.silver.source_1");
    assert_eq!(updated.last_event_id, "event-2");
    assert_eq!(updated.version, 2);
    assert_eq!(
        updated.metadata.get("quality_tier").map(String::as_str),
        Some("silver")
    );

    let other_tenant = registry
        .upsert_source(source_event("event-3", "tenant-2", "product-1", "source-1"))
        .await
        .expect("same source id in another tenant must be isolated");
    assert_eq!(other_tenant.version, 1);

    let fetched = registry
        .get_source(tenant_id, "product-1", "source-1")
        .await
        .expect("read must succeed")
        .expect("tenant source must exist");
    assert_eq!(fetched.last_event_id, "event-2");

    let missing = registry
        .get_source(tenant_id, "product-1", "missing")
        .await
        .expect("missing read must succeed");
    assert!(missing.is_none());
}

fn unique_tenant_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

async fn pg_registry_or_skip() -> Option<PostgresKnowledgeSourceRegistry> {
    let url = std::env::var("INTELLIGENCE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())?;

    let config = PostgresKnowledgeSourceRegistryConfig::new(url, 10)
        .expect("INTELLIGENCE_TEST_DATABASE_URL produced an invalid config");
    let registry = PostgresKnowledgeSourceRegistry::connect(config)
        .await
        .expect("failed to connect to test database");

    Some(registry)
}

fn source_event(
    event_id: &str,
    tenant_id: &str,
    product_id: &str,
    source_id: &str,
) -> KnowledgeSourceUpserted {
    KnowledgeSourceUpserted {
        event_id: event_id.to_string(),
        tenant_id: tenant_id.to_string(),
        product_id: product_id.to_string(),
        source_id: source_id.to_string(),
        source_kind: "document".to_string(),
        source_uri: format!(
            "iceberg://foundation.silver.{}",
            source_id.replace('-', "_")
        ),
        content_uri: Some(format!("s3://foundation/documents/{source_id}.pdf")),
        content_checksum_sha256: Some(
            "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        occurred_at_millis: 1_783_641_600_000,
        metadata: BTreeMap::from([("schema_version".to_string(), "fixture.v1".to_string())]),
    }
}
