//! Shared provider/lane resolution SSOT (used by the resume command and the async rate limiter).

use anyhow::{bail, Context};

use crate::public_provider_rate_policy::{LanePolicy, ProviderRatePolicyDocument};

/// Derive a lane endpoint-group key from a job's provider + endpoint (policy-JSON lane model).
pub(crate) fn endpoint_group_for(provider: &str, endpoint: &str) -> anyhow::Result<String> {
    if provider == "VWorld" {
        return Ok("vworld_dataset".to_owned());
    }
    if provider == "data.go.kr" && endpoint.starts_with("getBr") {
        return Ok("building_register_open_api".to_owned());
    }
    if provider == "data.go.kr"
        && ["Real", "Trade", "RTMS", "Transaction"]
            .iter()
            .any(|token| endpoint.contains(token))
    {
        return Ok("real_transaction_open_api".to_owned());
    }
    bail!("No provider rate endpoint group mapping for provider={provider} endpoint={endpoint}");
}

/// Resolve a job's provider + endpoint to its `LanePolicy` (provider + endpoint group match).
pub(crate) fn find_lane<'a>(
    policy: &'a ProviderRatePolicyDocument,
    provider: &str,
    endpoint: &str,
) -> anyhow::Result<&'a LanePolicy> {
    let endpoint_group = endpoint_group_for(provider, endpoint)?;
    policy
        .lanes
        .iter()
        .find(|lane| lane.provider == provider && lane.endpoint_groups.contains(&endpoint_group))
        .with_context(|| {
            format!("No provider rate lane for provider={provider} endpoint_group={endpoint_group}")
        })
}

/// Minimum inter-request interval (ms) for a lane running at `rps` requests/second.
pub(crate) fn min_page_interval_ms(rps: f64) -> anyhow::Result<u32> {
    if rps <= 0.0 || !rps.is_finite() {
        bail!("provider rate lane rps must be positive");
    }
    Ok((1000.0 / rps).ceil() as u32)
}

#[cfg(test)]
mod tests {
    use super::{endpoint_group_for, min_page_interval_ms};

    #[test]
    fn endpoint_group_maps_known_providers() -> anyhow::Result<()> {
        assert_eq!(endpoint_group_for("VWorld", "anything")?, "vworld_dataset");
        assert_eq!(
            endpoint_group_for("data.go.kr", "getBrTitleInfo")?,
            "building_register_open_api"
        );
        assert_eq!(
            endpoint_group_for("data.go.kr", "getRTMSDataSvcAptTradeDev")?,
            "real_transaction_open_api"
        );
        Ok(())
    }

    #[test]
    fn endpoint_group_rejects_unknown() {
        assert!(endpoint_group_for("data.go.kr", "getSomethingElse").is_err());
        assert!(endpoint_group_for("Unknown", "x").is_err());
    }

    #[test]
    fn min_page_interval_is_ceil_of_inverse_rps() -> anyhow::Result<()> {
        assert_eq!(min_page_interval_ms(20.0)?, 50);
        assert_eq!(min_page_interval_ms(3.0)?, 334);
        assert!(min_page_interval_ms(0.0).is_err());
        assert!(min_page_interval_ms(-1.0).is_err());
        assert!(min_page_interval_ms(f64::NAN).is_err());
        Ok(())
    }
}
