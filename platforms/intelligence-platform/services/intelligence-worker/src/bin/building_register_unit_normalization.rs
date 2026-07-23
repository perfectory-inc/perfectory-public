use std::error::Error;
use std::io;

use intelligence_worker::floor_proposal_job::proposal_generator_from_env;
use intelligence_worker::outbox_worker::foundation_submitter_from_env;
use intelligence_worker::unit_proposal_job::{
    building_register_unit_proposal_dry_run_enabled_from_env,
    run_building_register_unit_proposal_dry_run, run_building_register_unit_proposal_job,
    BuildingRegisterUnitProposalJobConfig,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = BuildingRegisterUnitProposalJobConfig::from_env()
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    let dry_run = building_register_unit_proposal_dry_run_enabled_from_env()
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    let generator = proposal_generator_from_env()?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "MODEL_RUNTIME_BASE_URL and MODEL_RUNTIME_DEFAULT_MODEL are required",
        )
    })?;
    if dry_run {
        let summary = run_building_register_unit_proposal_dry_run(config, generator).await?;
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }

    let submitter = foundation_submitter_from_env()?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "FOUNDATION_PLATFORM_BASE_URL is required",
        )
    })?;

    let summary = run_building_register_unit_proposal_job(config, generator, submitter).await?;
    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}
