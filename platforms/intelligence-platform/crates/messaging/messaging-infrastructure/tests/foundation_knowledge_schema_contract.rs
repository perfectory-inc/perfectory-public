//! The consumer codec against the schema Foundation owns.
//!
//! Foundation publishes `foundation-platform/schemas/<topic>.avsc`; Intelligence decodes it.
//! These tests fail when the codec and that file drift apart (a renamed, retyped or new required
//! field) or when the topic stops naming its schema file.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use apache_avro::{Reader, Schema, Writer};
use intelligence_contracts::FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_TOPIC;
use messaging_infrastructure::foundation_knowledge_avro::{
    FoundationKnowledgeSourceUpsertedEvent, FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_SCHEMA,
};

#[test]
fn the_topic_names_the_foundation_schema_file_the_codec_embeds() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../foundation-platform/schemas")
        .join(format!("{FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_TOPIC}.avsc"));
    let on_disk = std::fs::read_to_string(&path).expect("Foundation owns <topic>.avsc");

    assert_eq!(on_disk, FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_SCHEMA);
}

#[test]
fn the_codec_writes_and_reads_the_foundation_schema() {
    let schema = Schema::parse_str(FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_SCHEMA).unwrap();
    for event in [event(None), event(Some("sha256-of-content"))] {
        let value = event.to_avro_value();
        assert!(
            value.validate(&schema),
            "codec record does not match the Foundation schema"
        );

        let mut writer = Writer::new(&schema, Vec::new());
        writer.append(value).unwrap();
        let bytes = writer.into_inner().unwrap();
        let read = Reader::with_schema(&schema, &bytes[..])
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(
            FoundationKnowledgeSourceUpsertedEvent::from_avro_value(read).unwrap(),
            event
        );
    }
}

fn event(checksum: Option<&str>) -> FoundationKnowledgeSourceUpsertedEvent {
    FoundationKnowledgeSourceUpsertedEvent {
        event_id: "018f7c6a-0000-7000-8000-000000000001".to_string(),
        tenant_id: "tenant:scaffold".to_string(),
        product_id: "product:scaffold".to_string(),
        source_id: "source-1".to_string(),
        source_kind: "markdown".to_string(),
        source_uri: "s3://example/source-1.md".to_string(),
        content_uri: checksum.map(|_| "s3://example/source-1.md".to_string()),
        content_checksum_sha256: checksum.map(str::to_string),
        occurred_at_millis: 1_783_036_800_000,
        metadata: BTreeMap::from([("schema_version".to_string(), "example.v1".to_string())]),
    }
}
