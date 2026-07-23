use std::collections::BTreeMap;

use knowledge_domain::{
    validate_knowledge_source_event, KnowledgeSourceUpserted, KnowledgeSourceValidationError,
};

fn source_event() -> KnowledgeSourceUpserted {
    KnowledgeSourceUpserted {
        event_id: "event-1".to_owned(),
        tenant_id: "tenant-1".to_owned(),
        product_id: "foundation-platform".to_owned(),
        source_id: "building-register-floor".to_owned(),
        source_kind: "silver-table".to_owned(),
        source_uri: "s3://foundation-platform-lakehouse-prod/warehouse/silver".to_owned(),
        content_uri: None,
        content_checksum_sha256: None,
        occurred_at_millis: 1,
        metadata: BTreeMap::new(),
    }
}

#[derive(Clone, Copy)]
enum UriField {
    Source,
    Content,
}

#[test]
fn accepts_supported_source_and_content_uri_schemes() {
    let cases = [
        (UriField::Source, "s3://foundation-platform/source"),
        (UriField::Source, "http://foundation-platform/source"),
        (UriField::Source, "https://foundation-platform/source"),
        (UriField::Content, "s3://foundation-platform/content"),
        (UriField::Content, "http://foundation-platform/content"),
        (UriField::Content, "https://foundation-platform/content"),
    ];

    for (field, uri) in cases {
        let mut event = source_event();
        match field {
            UriField::Source => event.source_uri = uri.to_owned(),
            UriField::Content => event.content_uri = Some(uri.to_owned()),
        }

        assert!(
            validate_knowledge_source_event(&event).is_ok(),
            "{uri} must be accepted"
        );
    }
}

struct InvalidCase {
    name: &'static str,
    mutate: fn(&mut KnowledgeSourceUpserted),
    message: &'static str,
}

#[test]
fn rejects_invalid_source_events_with_exact_messages() {
    let cases = [
        InvalidCase {
            name: "blank event_id",
            mutate: blank_event_id,
            message: "event_id must be non-empty",
        },
        InvalidCase {
            name: "blank tenant_id",
            mutate: blank_tenant_id,
            message: "tenant_id must be non-empty",
        },
        InvalidCase {
            name: "blank product_id",
            mutate: blank_product_id,
            message: "product_id must be non-empty",
        },
        InvalidCase {
            name: "blank source_id",
            mutate: blank_source_id,
            message: "source_id must be non-empty",
        },
        InvalidCase {
            name: "blank source_kind",
            mutate: blank_source_kind,
            message: "source_kind must be non-empty",
        },
        InvalidCase {
            name: "invalid source_uri",
            mutate: invalid_source_uri,
            message: "source_uri must use s3, http, or https scheme",
        },
        InvalidCase {
            name: "invalid content_uri",
            mutate: invalid_content_uri,
            message: "content_uri must use s3, http, or https scheme",
        },
        InvalidCase {
            name: "wrong-length checksum",
            mutate: wrong_length_checksum,
            message: "content_checksum_sha256 must be hex sha256",
        },
        InvalidCase {
            name: "non-hex checksum",
            mutate: non_hex_checksum,
            message: "content_checksum_sha256 must be hex sha256",
        },
        InvalidCase {
            name: "zero occurred_at_millis",
            mutate: zero_occurred_at_millis,
            message: "occurred_at_millis must be positive",
        },
        InvalidCase {
            name: "negative occurred_at_millis",
            mutate: negative_occurred_at_millis,
            message: "occurred_at_millis must be positive",
        },
    ];

    for case in cases {
        let mut event = source_event();
        (case.mutate)(&mut event);

        let result = validate_knowledge_source_event(&event);
        let Err(error) = result else {
            panic!("{} must be rejected", case.name);
        };

        assert_eq!(
            error,
            KnowledgeSourceValidationError::InvalidEvent {
                message: case.message.to_owned(),
            },
            "{}",
            case.name
        );
    }
}

fn blank_event_id(event: &mut KnowledgeSourceUpserted) {
    event.event_id = " ".to_owned();
}

fn blank_tenant_id(event: &mut KnowledgeSourceUpserted) {
    event.tenant_id = " ".to_owned();
}

fn blank_product_id(event: &mut KnowledgeSourceUpserted) {
    event.product_id = " ".to_owned();
}

fn blank_source_id(event: &mut KnowledgeSourceUpserted) {
    event.source_id = " ".to_owned();
}

fn blank_source_kind(event: &mut KnowledgeSourceUpserted) {
    event.source_kind = " ".to_owned();
}

fn invalid_source_uri(event: &mut KnowledgeSourceUpserted) {
    event.source_uri = "invalid-uri".to_owned();
}

fn invalid_content_uri(event: &mut KnowledgeSourceUpserted) {
    event.content_uri = Some("invalid-uri".to_owned());
}

fn wrong_length_checksum(event: &mut KnowledgeSourceUpserted) {
    event.content_checksum_sha256 = Some("a".repeat(63));
}

fn non_hex_checksum(event: &mut KnowledgeSourceUpserted) {
    event.content_checksum_sha256 = Some("g".repeat(64));
}

fn zero_occurred_at_millis(event: &mut KnowledgeSourceUpserted) {
    event.occurred_at_millis = 0;
}

fn negative_occurred_at_millis(event: &mut KnowledgeSourceUpserted) {
    event.occurred_at_millis = -1;
}
