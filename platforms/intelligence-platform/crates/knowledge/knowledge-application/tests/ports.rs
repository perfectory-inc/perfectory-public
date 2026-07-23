use knowledge_application::{
    KnowledgeProjectionError, KnowledgeProjectionPort, KnowledgeSourceRegistryPort,
};

#[test]
fn application_owns_knowledge_ports() {
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}

    assert_send_sync::<dyn KnowledgeProjectionPort>();
    assert_send_sync::<dyn KnowledgeSourceRegistryPort>();
}

#[test]
fn application_owns_projection_error_messages() {
    let error = KnowledgeProjectionError::InvalidEvent {
        message: "source_uri must use s3, http, or https scheme".to_owned(),
    };

    assert_eq!(error.safe_message(), "knowledge event is invalid");
    assert_eq!(
        error.to_string(),
        "knowledge event is invalid: source_uri must use s3, http, or https scheme"
    );

    let store_error = KnowledgeProjectionError::StoreUnavailable {
        message: "registry connection refused".to_owned(),
    };

    assert_eq!(
        store_error.safe_message(),
        "knowledge projection durable effect failed"
    );
    assert_eq!(
        store_error.to_string(),
        "knowledge projection durable effect failed: registry connection refused"
    );
}
