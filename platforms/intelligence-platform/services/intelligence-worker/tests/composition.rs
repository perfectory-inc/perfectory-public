use intelligence_worker::{knowledge_consumer, outbox_worker};

#[test]
fn worker_exposes_background_modules() {
    let _knowledge_step_type = std::any::TypeId::of::<knowledge_consumer::KnowledgeConsumerStep>();
    let _drain_config_type = std::any::TypeId::of::<outbox_worker::DrainConfig>();
}
