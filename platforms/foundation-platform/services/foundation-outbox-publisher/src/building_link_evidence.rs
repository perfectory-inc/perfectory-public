//! Relationship evidence shared by canonical loads and the Gold serving boundary.
use std::{collections::HashMap, sync::LazyLock};

use anyhow::{ensure, Context};
use foundation_normalization_application::ActiveBuildingRegisterUnitOverrideReader;
use foundation_normalization_infrastructure::PgActiveBuildingRegisterUnitOverrideReader;
use lakehouse_application::building_register_unit_silver_override_from_application_snapshot;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

pub(crate) fn required_nullable_key<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildingLinkEvidence {
    pub(crate) unit_row_id: Option<String>,
    building_link_method: Option<String>,
    building_link_source_record_id: Option<String>,
    building_link_input_sha256: Option<String>,
    building_link_reason: Option<String>,
    pub(crate) normalization_application_id: Option<String>,
}

#[derive(Deserialize)]
struct PolicyWire {
    source_method: String,
    sha256_pattern: String,
    application_id_pattern: String,
}

struct Policy {
    source_method: String,
    sha256: Regex,
    application_id: Regex,
}

static POLICY: LazyLock<Result<Policy, String>> = LazyLock::new(|| {
    fn load() -> anyhow::Result<Policy> {
        #[derive(Deserialize)]
        struct Contract {
            relationship_evidence_policy: PolicyWire,
        }
        let wire = serde_json::from_str::<Contract>(
            crate::handoff_manifest_support::BUILDING_UNIT_HANDOFF_CONTRACT,
        )?
        .relationship_evidence_policy;
        Ok(Policy {
            source_method: wire.source_method,
            sha256: Regex::new(&wire.sha256_pattern)?,
            application_id: Regex::new(&wire.application_id_pattern)?,
        })
    }
    load().map_err(|error| format!("invalid building relationship evidence contract: {error:#}"))
});

fn text(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

impl BuildingLinkEvidence {
    /// A present relation or an approved withdrawal needs complete, distinct evidence.
    pub(crate) fn validate(&self, building_pk: Option<&str>) -> anyhow::Result<()> {
        let application = text(&self.normalization_application_id);
        if building_pk.is_none() && application.is_none() {
            return Ok(());
        }
        ensure!(
            building_pk.is_none_or(|pk| !pk.is_empty() && pk.trim() == pk),
            "building_register_pk must be a nonempty canonical key"
        );
        let policy = POLICY
            .as_ref()
            .map_err(|error| anyhow::anyhow!(error.clone()))?;
        let method = text(&self.building_link_method);
        let source = text(&self.building_link_source_record_id);
        let digest = text(&self.building_link_input_sha256);
        let automatic = method == Some(policy.source_method.as_str())
            && source.is_some()
            && digest.is_some_and(|digest| policy.sha256.is_match(digest))
            && application.is_none();
        let approved = application.is_some_and(|application| {
            policy.application_id.is_match(application)
                && uuid::Uuid::parse_str(application).is_ok_and(|id| !id.is_nil())
        }) && source.is_none()
            && digest.is_none();
        ensure!(
            text(&self.unit_row_id).is_some()
                && method.is_some()
                && text(&self.building_link_reason).is_none()
                && (automatic || approved),
            "building relationship lacks verified source or approval evidence"
        );
        Ok(())
    }
}

/// Reuses the existing active application-chain reader, including rollback handling.
struct ApprovedBuildingLink {
    unit_row_id: String,
    unit_register_pk: String,
    parent: Option<String>,
}

#[derive(Default)]
pub(crate) struct ApprovedBuildingLinks {
    by_application: HashMap<String, ApprovedBuildingLink>,
    by_unit_row: HashMap<String, String>,
    by_register_pk: HashMap<String, String>,
}

impl ApprovedBuildingLinks {
    pub(crate) async fn load_current() -> anyhow::Result<Self> {
        Self::load(
            &std::env::var("DATABASE_URL").context(
                "DATABASE_URL is required to verify current building links before serving",
            )?,
        )
        .await
    }

    /// A single execution-time snapshot of the existing active application reader.
    /// This is an independent validation boundary, not a distributed transaction lock.
    pub(crate) async fn load(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url)
            .await
            .context("connect for approved building links")?;
        let reader = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone());
        let result = Self::load_from(&reader).await;
        pool.close().await;
        result
    }

    async fn load_from(
        reader: &dyn ActiveBuildingRegisterUnitOverrideReader,
    ) -> anyhow::Result<Self> {
        let applications = reader
            .list_active_building_register_unit_overrides()
            .await?;
        let mut links = Self::default();
        for application in applications {
            let approved = building_register_unit_silver_override_from_application_snapshot(
                &application.snapshot,
            )?;
            links.insert(
                application.application_id.to_string(),
                approved.target_unit_row_id,
                approved.target_mgm_bldrgst_pk,
                approved.building_mgm_bldrgst_pk,
            )?;
        }
        Ok(links)
    }

    fn insert(
        &mut self,
        application: String,
        unit_row_id: String,
        unit_register_pk: String,
        parent: Option<String>,
    ) -> anyhow::Result<()> {
        ensure!(
            !self.by_application.contains_key(&application)
                && !self.by_unit_row.contains_key(&unit_row_id)
                && !self.by_register_pk.contains_key(&unit_register_pk),
            "active building link overrides have duplicate application or unit identities"
        );
        self.by_unit_row
            .insert(unit_row_id.clone(), application.clone());
        self.by_register_pk
            .insert(unit_register_pk.clone(), application.clone());
        self.by_application.insert(
            application,
            ApprovedBuildingLink {
                unit_row_id,
                unit_register_pk,
                parent,
            },
        );
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        evidence: &BuildingLinkEvidence,
        unit_register_pk: &str,
        building_pk: Option<&str>,
    ) -> anyhow::Result<()> {
        ensure!(
            !unit_register_pk.is_empty() && unit_register_pk.trim() == unit_register_pk,
            "unit register PK must be a nonempty canonical key"
        );
        ensure!(
            building_pk != Some(unit_register_pk),
            "unit cannot be its own parent"
        );
        evidence.validate(building_pk)?;
        let application = text(&evidence.normalization_application_id);
        // Both source-row and public unit identity matter: an old source-only artifact must
        // not erase a current approval, even if its evidence omits or changes the row id.
        for current in [
            text(&evidence.unit_row_id).and_then(|id| self.by_unit_row.get(id)),
            self.by_register_pk.get(unit_register_pk),
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                application == Some(current.as_str()),
                "building link artifact does not carry the unit's current active override"
            );
        }
        let Some(application) = application else {
            return Ok(());
        };
        let approved = self
            .by_application
            .get(application)
            .context("building link application is not an active approved override")?;
        ensure!(
            Some(approved.unit_row_id.as_str()) == text(&evidence.unit_row_id)
                && approved.unit_register_pk == unit_register_pk
                && approved.parent.as_deref() == building_pk,
            "building link disagrees with its approved source row, unit register PK or parent"
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fixture(
        application: &str,
        target: &str,
        unit_register_pk: &str,
        parent: Option<&str>,
    ) -> Self {
        let mut links = Self::default();
        links
            .insert(
                application.to_owned(),
                target.to_owned(),
                unit_register_pk.to_owned(),
                parent.map(str::to_owned),
            )
            .expect("valid active approval fixture");
        links
    }
}

#[cfg(test)]
pub(crate) fn source_fixture() -> serde_json::Value {
    serde_json::json!({"unit_row_id":"source-row:UNIT-1", "building_link_method":"parent_key",
        "building_link_source_record_id":"bronze/basis.txt#line=1", "building_link_input_sha256":"a".repeat(64)})
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn existing_active_reader_binds_all_approved_identities() -> anyhow::Result<()> {
        use foundation_normalization_application::ActiveBuildingRegisterUnitOverride;
        struct Reader(Vec<ActiveBuildingRegisterUnitOverride>);
        #[async_trait::async_trait]
        impl ActiveBuildingRegisterUnitOverrideReader for Reader {
            async fn list_active_building_register_unit_overrides(
                &self,
            ) -> Result<
                Vec<ActiveBuildingRegisterUnitOverride>,
                foundation_normalization_domain::NormalizationError,
            > {
                Ok(self.0.clone())
            }
        }
        let application_id = uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111")?;
        let active =
            ApprovedBuildingLinks::load_from(&Reader(vec![ActiveBuildingRegisterUnitOverride {
                application_id,
                snapshot: json!({
                    "target_identity": {"raw_record_id": "source-row:UNIT-1"},
                    "proposed_record": {
                        "mgm_bldrgst_pk": "UNIT-1", "building_mgm_bldrgst_pk": "BLDG-1",
                        "building_link_method": "parent_key", "normalization_status": "accepted",
                        "normalization_reason": "accepted_numeric_unit"
                    }
                }),
            }]))
            .await?;
        let evidence: BuildingLinkEvidence = serde_json::from_value(json!({
            "unit_row_id": "source-row:UNIT-1", "building_link_method": "parent_key",
            "normalization_application_id": application_id.to_string()
        }))?;
        active.validate(&evidence, "UNIT-1", Some("BLDG-1"))?;
        assert!(active
            .validate(&evidence, "UNIT-2", Some("BLDG-1"))
            .is_err());
        assert!(active
            .validate(&evidence, "UNIT-1", Some("BLDG-2"))
            .is_err());
        let inactive = ApprovedBuildingLinks::load_from(&Reader(vec![])).await?;
        assert!(inactive
            .validate(&evidence, "UNIT-1", Some("BLDG-1"))
            .is_err());
        Ok(())
    }

    #[test]
    fn self_parent_and_noncanonical_unit_keys_are_refused() -> anyhow::Result<()> {
        let evidence = serde_json::from_value(source_fixture())?;
        let active = ApprovedBuildingLinks::default();
        assert!(active
            .validate(&evidence, "UNIT-1", Some("UNIT-1"))
            .is_err());
        for unit in ["", " UNIT-1", "UNIT-1 "] {
            assert!(active.validate(&evidence, unit, Some("BLDG-1")).is_err());
        }
        Ok(())
    }

    #[test]
    fn an_active_override_cannot_be_replaced_by_stale_source_evidence() -> anyhow::Result<()> {
        let evidence: BuildingLinkEvidence = serde_json::from_value(source_fixture())?;
        let active = ApprovedBuildingLinks::fixture(
            "11111111-1111-4111-8111-111111111111",
            "source-row:UNIT-1",
            "UNIT-1",
            Some("APPROVED-PARENT"),
        );
        assert!(active
            .validate(&evidence, "UNIT-1", Some("OLD-PARENT"))
            .is_err());
        assert!(active.validate(&evidence, "UNIT-1", None).is_err());
        let mut changed_row: BuildingLinkEvidence = serde_json::from_value(source_fixture())?;
        changed_row.unit_row_id = Some("different-row".to_owned());
        assert!(active
            .validate(&changed_row, "UNIT-1", Some("OLD-PARENT"))
            .is_err());
        assert!(active
            .validate(&evidence, "UNIT-OTHER", Some("OLD-PARENT"))
            .is_err());
        Ok(())
    }

    #[test]
    fn approved_null_withdrawal_keeps_approval_binding_and_cannot_reuse_source_evidence(
    ) -> anyhow::Result<()> {
        let application = "11111111-1111-4111-8111-111111111111";
        let active =
            ApprovedBuildingLinks::fixture(application, "source-row:UNIT-1", "UNIT-1", None);
        let approved = json!({"unit_row_id":"source-row:UNIT-1", "building_link_method":"staff_unlinked",
            "normalization_application_id":application});
        let evidence: BuildingLinkEvidence = serde_json::from_value(approved.clone())?;
        active.validate(&evidence, "UNIT-1", None)?;
        assert!(active.validate(&evidence, "UNIT-OTHER", None).is_err());
        let mut stale = approved;
        stale["building_link_source_record_id"] = json!("old-source-row");
        assert!(active
            .validate(&serde_json::from_value(stale)?, "UNIT-1", None)
            .is_err());
        Ok(())
    }

    #[test]
    fn active_approvals_cannot_claim_the_same_unit_register_key() {
        let mut active = ApprovedBuildingLinks::fixture(
            "11111111-1111-4111-8111-111111111111",
            "row-1",
            "UNIT-1",
            None,
        );
        assert!(active
            .insert(
                "22222222-2222-4222-8222-222222222222".to_owned(),
                "row-2".to_owned(),
                "UNIT-1".to_owned(),
                Some("BLDG-2".to_owned())
            )
            .is_err());
    }

    #[test]
    fn source_and_approval_evidence_are_distinct_and_complete() -> anyhow::Result<()> {
        let source = json!({"unit_row_id":"source-row:UNIT-1", "building_link_method":"parent_key",
            "building_link_source_record_id":"bronze/basis.txt#line=1", "building_link_input_sha256":"a".repeat(64)});
        let parse = |value| serde_json::from_value::<BuildingLinkEvidence>(value);
        parse(source.clone())?.validate(Some("BLDG-1"))?;
        for (field, value) in [
            ("building_link_method", json!("canonical_dong")),
            ("building_link_source_record_id", json!(" ")),
            ("building_link_input_sha256", json!("invalid")),
            ("building_link_reason", json!("conflicting_parent")),
            (
                "normalization_application_id",
                json!("11111111-1111-4111-8111-111111111111"),
            ),
        ] {
            let mut invalid = source.clone();
            invalid[field] = value;
            assert!(parse(invalid)?.validate(Some("BLDG-1")).is_err(), "{field}");
        }
        let approved = json!({"unit_row_id":"source-row:UNIT-1", "building_link_method":"canonical_dong",
            "normalization_application_id":"11111111-1111-4111-8111-111111111111"});
        let evidence = parse(approved)?;
        evidence.validate(Some("BLDG-1"))?;
        let active = ApprovedBuildingLinks::fixture(
            "11111111-1111-4111-8111-111111111111",
            "source-row:UNIT-1",
            "UNIT-1",
            Some("BLDG-1"),
        );
        active.validate(&evidence, "UNIT-1", Some("BLDG-1"))?;
        assert!(active.validate(&evidence, "UNIT-1", Some("OTHER")).is_err());
        assert!(active
            .validate(&evidence, "UNIT-OTHER", Some("BLDG-1"))
            .is_err());
        assert!(ApprovedBuildingLinks::default()
            .validate(&evidence, "UNIT-1", Some("BLDG-1"))
            .is_err());
        assert!(parse(json!({}))?.validate(Some("BLDG-1")).is_err());
        parse(json!({}))?.validate(None)?;
        Ok(())
    }
    #[test]
    fn evidence_round_trip_preserves_all_fields_and_refuses_unknown_claims() -> anyhow::Result<()> {
        let source = source_fixture();
        let evidence: BuildingLinkEvidence = serde_json::from_value(source.clone())?;
        let encoded = serde_json::to_value(&evidence)?;
        assert_eq!(encoded["unit_row_id"], source["unit_row_id"]);
        assert_eq!(
            encoded["building_link_source_record_id"],
            source["building_link_source_record_id"]
        );
        assert_eq!(
            encoded["building_link_input_sha256"],
            source["building_link_input_sha256"]
        );
        assert!(encoded
            .get("building_link_reason")
            .is_some_and(serde_json::Value::is_null));
        assert!(encoded
            .get("normalization_application_id")
            .is_some_and(serde_json::Value::is_null));
        assert_eq!(
            evidence,
            serde_json::from_value::<BuildingLinkEvidence>(encoded)?
        );
        let mut unknown = source;
        unknown["alternate_parent"] = serde_json::json!("BLDG-2");
        assert!(serde_json::from_value::<BuildingLinkEvidence>(unknown).is_err());
        Ok(())
    }
}
