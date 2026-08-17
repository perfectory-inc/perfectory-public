//! Contract tests for the object URL template that makes an object key fetchable.

use foundation_shared_kernel::{ObjectKey, ObjectUrlTemplate};

#[test]
fn template_materializes_an_object_key_into_an_absolute_url(
) -> Result<(), Box<dyn std::error::Error>> {
    let template = ObjectUrlTemplate::parse("https://lakehouse.example.com/{object_key}")?;
    let key = ObjectKey::parse(
        "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json",
    )?;

    assert_eq!(
        template.materialize(&key),
        "https://lakehouse.example.com/gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json"
    );
    Ok(())
}

#[test]
fn template_without_the_object_key_placeholder_is_rejected() {
    for raw in [
        "https://lakehouse.example.com/gold/profiles.json",
        "https://lakehouse.example.com/{object_key}/{object_key}",
    ] {
        assert!(
            ObjectUrlTemplate::parse(raw).is_err(),
            "template without exactly one object key placeholder was accepted: {raw}"
        );
    }
}

#[test]
fn template_rejects_unknown_placeholders_and_query_or_fragment() {
    for raw in [
        "https://lakehouse.example.com/{object_key_prefix}/{object_key}",
        "https://lakehouse.example.com/{object_key}?token=x",
        "https://lakehouse.example.com/{object_key}#fragment",
    ] {
        assert!(
            ObjectUrlTemplate::parse(raw).is_err(),
            "template with an unknown placeholder, query, or fragment was accepted: {raw}"
        );
    }
}

#[test]
fn template_requires_https_except_on_loopback() -> Result<(), Box<dyn std::error::Error>> {
    ObjectUrlTemplate::parse("http://localhost:9000/bucket/{object_key}")?;
    ObjectUrlTemplate::parse("http://127.0.0.1:9000/bucket/{object_key}")?;

    for raw in [
        "http://lakehouse.example.com/{object_key}",
        "s3://foundation-platform-lakehouse-prod/{object_key}",
        "/{object_key}",
        "{object_key}",
    ] {
        assert!(
            ObjectUrlTemplate::parse(raw).is_err(),
            "non-https template was accepted: {raw}"
        );
    }
    Ok(())
}

#[test]
fn template_requires_a_host_and_keeps_the_placeholder_in_the_path() {
    for raw in [
        "https://{object_key}",
        "https://{object_key}.example.com/profile.json",
        "https:///{object_key}",
    ] {
        assert!(
            ObjectUrlTemplate::parse(raw).is_err(),
            "template that dissolves its host was accepted: {raw}"
        );
    }
}

#[test]
fn template_rejects_empty_and_padded_values() {
    for raw in ["", " ", " https://lakehouse.example.com/{object_key} "] {
        assert!(
            ObjectUrlTemplate::parse(raw).is_err(),
            "empty or padded template was accepted: {raw:?}"
        );
    }
}
