use super::*;

pub(super) fn build_silver_scalar_remote_script(
    config: &RemoteLakehouseJobConfig,
    spec: SilverScalarRemoteJobSpec,
) -> String {
    let root = shell_quote(&config.remote_root);
    let spark_compose = floor_cycle::spark_compose(config);
    let environment = floor_cycle::environment_script(config);
    let spec_input_path = config
        .input_path_override
        .as_deref()
        .unwrap_or(spec.input_path);
    let input_path = shell_quote(spec_input_path);
    let input_file_batch_size = config
        .input_file_batch_size_override
        .unwrap_or(spec.default_input_file_batch_size);
    let input_preflight =
        if spec.require_non_empty_input && config.job != RemoteLakehouseJob::PipelineFull {
            format!(
                "\
if [ -f {input_path} ]; then
  if ! test -s {input_path}; then
    echo 'missing or empty Silver handoff input' >&2
    exit 3
  fi
elif [ -d {input_path} ]; then
  if [ -z \"$(find -L {input_path} -type f -size +0c -print -quit)\" ]; then
    echo 'missing or empty Silver handoff input' >&2
    exit 3
  fi
else
  echo 'missing or empty Silver handoff input' >&2
  exit 3
fi
"
            )
        } else {
            String::new()
        };
    let expected_count_arg = spec
        .expected_count
        .map(|count| format!("  --expected-count {count} \\\n"))
        .unwrap_or_default();
    let java_extra_options_args =
        spec.spark_java_extra_options
            .map_or_else(String::new, |java_extra_options| {
                format!(
                    "  --conf spark.driver.extraJavaOptions={java_extra_options} \\\n  --conf spark.executor.extraJavaOptions={java_extra_options} \\\n"
                )
            });
    let allow_non_smoke_overwrite_arg = if spec.allow_non_smoke_overwrite {
        "  --allow-non-smoke-overwrite \\\n"
    } else {
        ""
    };
    // Read off the engine contract, not written here. A remote submission that pinned its own
    // Iceberg would load a different jar than the job it submits expects (root ADR-0064). The
    // config resolved it while it could still report a bad contract; this only formats it.
    let full_floor = config.job == RemoteLakehouseJob::PipelineFull;
    let floor_root = config
        .floor_source
        .as_ref()
        .map(floor_source::FloorSource::root);
    let floor_input = floor_root.as_ref().map(|root| format!("{root}/handoff"));
    let floor_summary = floor_root
        .as_ref()
        .map(|root| format!("{root}/spark-summary.json"));
    let spec_input_path = floor_input.as_deref().unwrap_or(spec_input_path);
    let spec_summary_path = floor_summary.as_deref().unwrap_or(spec.summary_path);
    let summary_path = shell_quote(&floor_cycle::host_path(config, spec_summary_path));
    let container_input = shell_quote(&format!("/workspace/{spec_input_path}"));
    let container_summary = shell_quote(&format!("/workspace/{spec_summary_path}"));
    let capture_outcome = floor_cycle::capture_outcome(config, &summary_path);
    let run_options = if config.local.is_some() {
        " --no-deps --pull never"
    } else {
        ""
    };
    let spark_user = if full_floor {
        "  --user \"${FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}:$(id -g)\" \\\n"
    } else {
        ""
    };
    let ivy = if full_floor {
        "/home/spark/.ivy2"
    } else {
        "/tmp/.ivy2"
    };
    let smoke_directory = if full_floor {
        ""
    } else {
        "mkdir -p 'target/lakehouse/smoke'\n"
    };
    let input_file_batch_size = if full_floor { 0 } else { input_file_batch_size };
    let write_mode = if full_floor { "append" } else { "overwrite" };
    let deferred_readback = if full_floor {
        ""
    } else {
        "  --defer-iceberg-readback-validation \\\n"
    };
    let derivation_arg = config
        .floor_source
        .as_ref()
        .and_then(|s| s.derivation.as_ref())
        .map_or_else(String::new, |label| {
            format!("  --derivation {} \\\n", shell_quote(label))
        });
    let allow_non_smoke_overwrite_arg = if full_floor {
        ""
    } else {
        allow_non_smoke_overwrite_arg
    };
    let trino_setup = if full_floor {
        String::new()
    } else {
        "render_trino_catalog_from_env() {
  mkdir -p 'infra/lakehouse/trino/catalog'
  cat > 'infra/lakehouse/trino/catalog/r2.properties' <<TRINO_CATALOG
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI}
iceberg.rest-catalog.warehouse=${FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE}
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN}
iceberg.rest-catalog.oauth2.server-uri=${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI%/}/v1/oauth/tokens
fs.s3.enabled=true
s3.region=${FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION:-auto}
s3.endpoint=${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT}
s3.aws-access-key=${FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID}
s3.aws-secret-key=${FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY}
s3.path-style-access=true
TRINO_CATALOG
  chmod 600 'infra/lakehouse/trino/catalog/r2.properties'
}
render_trino_catalog_from_env
".to_owned()
    };
    let trino_readback = if full_floor {
        String::new()
    } else {
        format!("expected_rows=\"$(grep -o '\"row_count\":[0-9][0-9]*' {summary_path} | tail -n 1 | sed 's/[^0-9]//g')\"
if [ -z \"$expected_rows\" ]; then
  echo 'missing row_count in Spark summary' >&2
  exit 6
fi
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-query up -d --force-recreate trino
for attempt in $(seq 1 60); do
  if {LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-query exec -T trino trino --catalog r2 --schema silver --execute \"SELECT 1\" >/dev/null 2>&1; then
    break
  fi
  if [ \"$attempt\" -eq 60 ]; then
    echo 'trino did not become ready for Iceberg readback validation' >&2
    exit 7
  fi
  sleep 2
done
actual_rows=\"$({LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-query exec -T trino trino --catalog r2 --schema silver --execute \"SELECT count(*) FROM {spec_iceberg_table}\" | tr -d '\"[:space:]')\"
if [ \"$actual_rows\" != \"$expected_rows\" ]; then
  echo \"trino row count mismatch table={spec_iceberg_table} expected=$expected_rows actual=$actual_rows\" >&2
  exit 8
fi
", spec_iceberg_table = spec.iceberg_table)
    };
    let iceberg_packages = &config.iceberg_packages;
    format!(
        "\
set -euo pipefail
cd {root}
{input_preflight}\
{environment}\
if [ -z \"${{FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET:-}}\" ]; then
  echo 'lakehouse catalog bucket mismatch: FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET is missing' >&2
  exit 9
fi
catalog_bucket=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI##*/}}\"
if [ \"$catalog_bucket\" != \"$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET\" ]; then
  echo \"lakehouse catalog bucket mismatch: catalog_uri_bucket=$catalog_bucket r2_bucket=$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET\" >&2
  exit 9
fi
case \"${{FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE:-}}\" in
  *_\"$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET\") ;;
  *)
    echo \"lakehouse warehouse bucket mismatch: warehouse=$FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE r2_bucket=$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET\" >&2
    exit 9
    ;;
esac
if [ -n \"${{FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI:-}}\" ]; then
  expected_oauth_uri=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI%/}}/v1/oauth/tokens\"
  if [ \"$FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI\" != \"$expected_oauth_uri\" ]; then
    echo 'lakehouse oauth uri mismatch: expected catalog_uri + /v1/oauth/tokens' >&2
    exit 9
  fi
fi
{trino_setup}{smoke_directory}{spark_compose} --profile lakehouse-batch run --rm{run_options} \\
{spark_user}\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \\
  -e FOUNDATION_PLATFORM_SPARK_SKIP_STOP_ON_SUCCESS=1 \\
  spark spark-submit \\
  --master {spec_spark_master} \\
  --driver-memory {spec_spark_driver_memory} \\
  --conf spark.jars.ivy={ivy} \\
{java_extra_options_args}\
  --packages {iceberg_packages} \\
  /workspace/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py \\
  --input {container_input} \\
  --input-format {spec_input_format} \\
  --contract {spec_contract} \\
  --write-mode iceberg \\
  --iceberg-write-mode {write_mode} \\
  --iceberg-table {spec_iceberg_table} \\
{expected_count_arg}{allow_non_smoke_overwrite_arg}  --input-file-batch-size {input_file_batch_size} \\
{derivation_arg}{deferred_readback}  --summary-output {container_summary}
{trino_readback}{capture_outcome}echo '{SUMMARY_BEGIN_MARKER}'
cat {summary_path}
echo '{SUMMARY_END_MARKER}'
",
        spec_input_format = spec.input_format,
        spec_contract = spec.contract,
        spec_iceberg_table = spec.iceberg_table,
        spec_spark_master = spec.spark_master,
        spec_spark_driver_memory = spec.spark_driver_memory,
        java_extra_options_args = java_extra_options_args
    )
}
