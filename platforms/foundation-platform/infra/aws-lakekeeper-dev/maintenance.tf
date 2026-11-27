# T11b's runner: the stock Spark image with Iceberg's own runtime (owner decision 2026-11-27, maintenance "가": the
# stock procedures in one stock Spark container that Dagster starts when needed; here started by probe.sh). It runs
# probes/maintenance.py as the operator, reaching storage with vended credentials only: no S3 key of its own. The
# Iceberg packages are fetched from Maven Central at start, by pinned version, into the task's scratch volume.
# s3.delete-enabled=true overrides the false that Lakekeeper sends to clients of a soft-deletion warehouse
# (push-s3-delete-disabled, default true; upstream storage-s3 and concepts "Soft Deletion"): without it S3FileIO
# skips every delete in silence, and expiry and orphan removal report files they never removed (T11b, 2026-11-27).
# Dev only; ADR-0624 decides prod's image (a mirrored digest, root ADR-0619 §7).

locals {
  spark_image     = replace("public.ecr.aws/docker/library/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:8901b8427694267ed719f27aa1498815dbd3573efb3cde447246fbff29d07272", "/:[^:/@]+@/", "@")
  iceberg_version = "1.12.0"
  # Writes the script and Spark's settings to the scratch volume (the credential goes to a file there, never onto a
  # command line), then runs spark-submit. $0 is the task's name, $1 and $2 the table and the table to drop.
  maintenance_script = <<-EOT
    set -euo pipefail
    mkdir -p /work/ivy /work/spark /work/conf
    printf '%s' "$MAINTENANCE_SOURCE" > /work/maintenance.py
    cat > /work/conf/log4j2.properties <<'LOG'
    rootLogger.level = warn
    rootLogger.appenderRef.stderr.ref = console
    appender.console.type = Console
    appender.console.name = console
    appender.console.target = SYSTEM_ERR
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %d %p %c{1}: %m%n
    LOG
    {
      echo "spark.master local[2]"
      echo "spark.driver.memory 2g"
      echo "spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${local.iceberg_version},org.apache.iceberg:iceberg-aws-bundle:${local.iceberg_version}"
      echo "spark.jars.ivy /work/ivy"
      echo "spark.local.dir /work/spark"
      echo "spark.testing true"
      echo "spark.redaction.regex (?i)secret|password|token|access[.]?key|credential"
      echo "spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      echo "spark.sql.catalog.lk org.apache.iceberg.spark.SparkCatalog"
      echo "spark.sql.catalog.lk.type rest"
      echo "spark.sql.catalog.lk.uri $CATALOG_URL/catalog"
      echo "spark.sql.catalog.lk.warehouse $WAREHOUSE_NAME"
      echo "spark.sql.catalog.lk.credential $CLIENT_ID:$CLIENT_SECRET"
      echo "spark.sql.catalog.lk.oauth2-server-uri $TOKEN_URL"
      echo "spark.sql.catalog.lk.scope $SCOPE"
      echo "spark.sql.catalog.lk.header.X-Iceberg-Access-Delegation vended-credentials"
      echo "spark.sql.catalog.lk.io-impl org.apache.iceberg.aws.s3.S3FileIO"
      echo "spark.sql.catalog.lk.rest-catalog-purge true"
      echo "spark.sql.catalog.lk.s3.delete-enabled true"
    } > /work/conf/spark-defaults.conf
    export SPARK_CONF_DIR=/work/conf HOME=/work
    exec /opt/spark/bin/spark-submit /work/maintenance.py "$1" "$2"
  EOT
}

resource "aws_iam_role" "maintenance_execution" {
  name               = "${local.name}-probe-maintenance-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "maintenance_execution" {
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dev.arn}:log-stream:probe/maintenance/*"]
  }
  statement {
    sid       = "InjectTheOperatorsClientSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.principal["bootstrap"].arn]
  }
}

resource "aws_iam_role_policy" "maintenance_execution" {
  name   = "maintenance"
  role   = aws_iam_role.maintenance_execution.id
  policy = data.aws_iam_policy_document.maintenance_execution.json
}

resource "aws_ecs_task_definition" "maintenance" {
  family                   = "${local.name}-probe-maintenance"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 4096
  execution_role_arn       = aws_iam_role.maintenance_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  volume {
    name = "work"
  }
  container_definitions = jsonencode([{
    name       = "maintenance"
    image      = local.spark_image
    essential  = true
    user       = "0"
    entryPoint = ["/bin/bash", "-c", local.maintenance_script, "maintenance"]
    command    = ["silver.w109_smoke_maint", "silver.w109_smoke_maint_drop"]
    environment = [
      { name = "CATALOG_URL", value = local.catalog_url },
      { name = "TOKEN_URL", value = local.token_url },
      { name = "SCOPE", value = local.catalog_scope },
      { name = "WAREHOUSE_NAME", value = local.dev.warehouse.bucket },
      { name = "MAINTENANCE_SOURCE", value = file("${path.module}/probes/maintenance.py") },
    ]
    secrets = [
      { name = "CLIENT_ID", valueFrom = "${aws_secretsmanager_secret.principal["bootstrap"].arn}:client_id::" },
      { name = "CLIENT_SECRET", valueFrom = "${aws_secretsmanager_secret.principal["bootstrap"].arn}:client_secret::" },
    ]
    mountPoints      = [{ sourceVolume = "work", containerPath = "/work", readOnly = false }]
    logConfiguration = local.logs["probe"]
  }])
}
