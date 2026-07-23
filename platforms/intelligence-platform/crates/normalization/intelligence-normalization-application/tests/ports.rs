use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, ModelGateway, NormalizationOutboxPort,
    NormalizationProposalGenerator, RateLimiterPort,
};

#[test]
fn application_owns_all_normalization_ports() {
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn FoundationNormalizationSubmitter>();
    assert_send_sync::<dyn ModelGateway>();
    assert_send_sync::<dyn NormalizationOutboxPort>();
    assert_send_sync::<dyn NormalizationProposalGenerator>();
    assert_send_sync::<dyn RateLimiterPort>();
}
