use intelligence_worker::outbox_worker;

#[test]
fn worker_exposes_background_modules() {
    let _drain_config_type = std::any::TypeId::of::<outbox_worker::DrainConfig>();
}
