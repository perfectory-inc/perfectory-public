use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, ModelGateway, NormalizationOutboxPort,
    NormalizationProposalGenerator, RateLimiterPort,
};
use intelligence_normalization_infrastructure::{
    FoundationPlatformNormalizationClient, InMemoryWorkflowState, MemoryRateLimiter,
    ModelBackedNormalizationProposalGenerator, OllamaNativeModelGateway,
};

fn assert_submitter<T: FoundationNormalizationSubmitter>() {}
fn assert_gateway<T: ModelGateway>() {}
fn assert_outbox<T: NormalizationOutboxPort>() {}
fn assert_generator<T: NormalizationProposalGenerator>() {}
fn assert_limiter<T: RateLimiterPort>() {}

#[test]
fn concrete_adapters_implement_application_ports() {
    assert_submitter::<FoundationPlatformNormalizationClient>();
    assert_gateway::<OllamaNativeModelGateway>();
    assert_outbox::<InMemoryWorkflowState>();
    assert_generator::<ModelBackedNormalizationProposalGenerator>();
    assert_limiter::<MemoryRateLimiter>();
}
