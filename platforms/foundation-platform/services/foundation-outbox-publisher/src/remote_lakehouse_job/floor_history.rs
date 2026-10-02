//! Read-only historical preflight for the full FLOOR pipeline.
use super::{shell_quote, RemoteLakehouseJobConfig};
use anyhow::{ensure, Context};
use serde_json::Value;

const HISTORY_SCHEMA: &str = "foundation-platform.floor-history-check.v1";

pub(super) fn preflight_script(config: &RemoteLakehouseJobConfig, native_export: &str) -> String {
    let Some(source) = &config.floor_source else {
        return String::new();
    };
    let source_root = source.root();
    let root = shell_quote(&super::floor_cycle::host_path(config, &source_root));
    let container_root = shell_quote(&source_root);
    let run_options = if config.local.is_some() {
        " --no-deps --pull never"
    } else {
        ""
    };
    let capture_outcome =
        super::floor_cycle::capture_outcome(config, "\"$floor_preflight_dir/history.json\"");
    let packages = &config.iceberg_packages;
    let compose = super::floor_cycle::spark_compose(config);
    let derivation = source
        .derivation
        .as_ref()
        .map_or_else(String::new, |label| {
            format!("  --derivation {} \\\n", shell_quote(label))
        });
    format!(
        "\
mkdir -p {root}
floor_preflight_dir=\"$(mktemp -d {root}/source-check.XXXXXXXX)\"
chmod 770 \"$floor_preflight_dir\"
floor_preflight_container_dir={container_root}/\"${{floor_preflight_dir##*/}}\"
export FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH=\"$floor_preflight_container_dir/source.json\"
{native_export}
chmod 640 \"$floor_preflight_dir/source.json\"
unset FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH
if {compose} --profile lakehouse-batch run --rm{run_options} \\
  --user \"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}:$(id -g)\" \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \\
  spark spark-submit \\
  --master 'local[2]' --driver-memory 1g \\
  --conf spark.jars.ivy=/home/spark/.ivy2 \\
  --packages {packages} \\
  /workspace/infra/lakehouse/spark/jobs/building_register_floor_history.py \\
  --source-receipt \"/workspace/$floor_preflight_container_dir/source.json\" \\
{derivation}  --summary-output \"/workspace/$floor_preflight_container_dir/history.json\"; then
{capture_outcome}  echo '{begin}'
  cat \"$floor_preflight_dir/history.json\"
  echo '{end}'
  exit 0
else
  floor_preflight_status=$?
  if [ \"$floor_preflight_status\" -ne 10 ]; then
    exit \"$floor_preflight_status\"
  fi
fi
",
        begin = super::SUMMARY_BEGIN_MARKER,
        end = super::SUMMARY_END_MARKER,
    )
}

pub(super) fn retained_outcome(
    raw: &str,
    source: &super::floor_source::FloorSource,
) -> anyhow::Result<Option<u64>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("schema_version").and_then(Value::as_str) != Some(HISTORY_SCHEMA) {
        return Ok(None);
    }
    let inputs = &value["inputs"];
    for (role, name, slug) in [
        (
            "floor",
            &source.floor,
            foundation_outbox_publisher::building_register_source_role::SourceRole::Floor.slug(),
        ),
        (
            "title",
            &source.title,
            foundation_outbox_publisher::building_register_source_role::SourceRole::Title.slug(),
        ),
    ] {
        ensure!(
            inputs[role]["provider_file_id"].as_str() == name.strip_suffix(".zip")
                && inputs[role]["role_slug"] == slug,
            "retained FLOOR outcome belongs to different inputs"
        );
    }
    ensure!(
        value.get("derivation") == Some(&serde_json::json!(source.derivation)),
        "retained FLOOR derivation differs"
    );
    let identity = foundation_outbox_publisher::building_register_floor_silver_export::source_identity_from_semantic(inputs.clone(), &source.history)?;
    ensure!(
        value["source_snapshot_id"] == identity,
        "retained FLOOR content identity differs"
    );
    let witness = source.history.value()?;
    if value["retention_kind"] == "registered_append" {
        return registered_outcome(&value, &witness).map(Some);
    }
    ensure!(
        value.get("retention_kind").is_none() && source.derivation.is_none(),
        "historical base cannot authenticate a derivation"
    );
    ensure!(
        value["action"] == "already_retained"
            && value["persisted_row_count"].as_u64() == Some(0)
            && [
                "source_snapshot_id",
                "table_uuid",
                "snapshot_id",
                "retained_row_count"
            ]
            .iter()
            .all(|key| {
                let witness_key = if *key == "retained_row_count" {
                    "row_count"
                } else {
                    key
                };
                value[*key] == witness[witness_key]
            }),
        "invalid historical FLOOR retention outcome"
    );
    let head = value["observed_head_snapshot_id"]
        .as_str()
        .context("historical FLOOR outcome has no observed head")?;
    ensure!(
        head.parse::<u64>().is_ok_and(|id| id > 0),
        "invalid observed Iceberg head"
    );
    Ok(Some(
        witness["row_count"]
            .as_u64()
            .context("missing retained row count")?,
    ))
}

fn registered_outcome(value: &Value, witness: &Value) -> anyhow::Result<u64> {
    let source = value["source_snapshot_id"]
        .as_str()
        .context("missing retained FLOOR source")?;
    let new_source = source
        .strip_prefix("building-register-floor-content-v1-")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    ensure!(
        value["action"] == "already_retained"
            && value["persisted_row_count"].as_u64() == Some(0)
            && value["table_uuid"] == witness["table_uuid"]
            && (new_source || value["source_snapshot_id"] == witness["source_snapshot_id"]),
        "invalid registered FLOOR retention outcome"
    );
    for key in ["snapshot_id", "observed_head_snapshot_id"] {
        ensure!(
            value[key]
                .as_str()
                .is_some_and(|id| id.parse::<u64>().is_ok_and(|id| id > 0)),
            "invalid retained FLOOR snapshot"
        );
    }
    chrono::DateTime::parse_from_rfc3339(
        value["retained_ingested_at_utc"]
            .as_str()
            .context("missing retained FLOOR time")?,
    )?;
    let rows = value["retained_row_count"]
        .as_u64()
        .context("missing retained FLOOR rows")?;
    ensure!(rows > 0, "empty retained FLOOR append");
    Ok(rows)
}
