//! Secret-safe entry point for one-shot service-principal provisioning.

use identity_service_provisioner::{
    provision_with_config, ProvisionConfig, ProvisionError, ProvisionErrorCategory,
};
use serde::Serialize;
use std::io::{self, Write};
use std::process::ExitCode;

#[derive(Serialize)]
struct SafeFailure {
    status: &'static str,
    category: ProvisionErrorCategory,
    message: &'static str,
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match ProvisionConfig::from_env() {
        Ok(config) => provision_with_config(&config).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(report) => write_json(&report, ExitCode::SUCCESS),
        Err(error) => write_json(
            &SafeFailure {
                status: "error",
                category: error.category(),
                message: safe_message(&error),
            },
            ExitCode::FAILURE,
        ),
    }
}

fn write_json(value: &impl Serialize, success: ExitCode) -> ExitCode {
    let mut stdout = io::stdout().lock();
    if serde_json::to_writer(&mut stdout, value).is_err() || stdout.write_all(b"\n").is_err() {
        return ExitCode::FAILURE;
    }
    success
}

const fn safe_message(error: &ProvisionError) -> &'static str {
    match error {
        ProvisionError::Configuration => "required provisioning configuration is missing",
        ProvisionError::BindingsRead => "workload principal bindings could not be read",
        ProvisionError::ManifestValidation(_) => "workload principal policy resolution failed",
        ProvisionError::Database(_) => "service-principal database operation failed",
    }
}
