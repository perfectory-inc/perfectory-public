# What the catalog's load is measured by (root ADR-0624 §7) and what alarms on it (prod: the contract's alarms;
# dev has none, so its tests never page). Every signal is AWS's own metric or a metric filter on the logs the tasks
# already write; none is our code.
#
# - Pool saturation: a Lakekeeper pool that waited out its timeout logs sqlx's "pool timed out while waiting for an
#   open connection" (2026-11-27, the write pool of 1), and the database's connections near its max_connections.
# - Check latency: OpenFGA logs every check it answers (msg grpc_req_complete, query_duration_ms); a p95 near
#   Lakekeeper's ~3 s authorization deadline comes before the 503s (T13 on the shared micro).
# - A refused authorization: Lakekeeper's internal_authorization_error, the 503 every call gets while OpenFGA fails.
# - The database's freeable memory near its floor (the shared micro fell to 84 MB in the tests).
#
# The metric filters exist in both environments (they cost nothing); the alarms publish to the platform's alarm topic
# (infra/aws), whose messages carry names only.

locals {
  alarms        = local.env.alarms
  log_namespace = "Perfectory/${local.name}"
  log_signals = {
    pool-timeouts = {
      pattern = "\"pool timed out while waiting for an open connection\""
      value   = "1"
    }
    authorization-errors = {
      pattern = "?\"internal_authorization_error\" ?\"AuthorizationBackendError\""
      value   = "1"
    }
    check-latency = {
      pattern = "{ $.msg = \"grpc_req_complete\" && ($.grpc_method = \"Check\" || $.grpc_method = \"BatchCheck\") }"
      value   = "$.query_duration_ms"
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "signal" {
  for_each       = local.log_signals
  name           = "${local.name}-${each.key}"
  log_group_name = aws_cloudwatch_log_group.lakekeeper.name
  pattern        = each.value.pattern
  metric_transformation {
    namespace = local.log_namespace
    name      = each.key
    value     = each.value.value
    unit      = each.key == "check-latency" ? "Milliseconds" : "Count"
  }
}

data "aws_sns_topic" "alarms" {
  count = local.alarms == null ? 0 : 1
  name  = local.platform.alarms.topic
}

resource "aws_cloudwatch_metric_alarm" "log_signal" {
  for_each = local.alarms == null ? {} : {
    pool-timeouts        = { statistic = "Sum", extended = null, threshold = 1, why = "a Lakekeeper pool waited out its timeout for a database connection" }
    authorization-errors = { statistic = "Sum", extended = null, threshold = 1, why = "Lakekeeper could not reach or use OpenFGA, so calls got 503" }
    check-latency        = { statistic = null, extended = "p95", threshold = local.alarms.check_latency_p95_ms, why = "OpenFGA's check p95 is near Lakekeeper's authorization deadline" }
  }
  alarm_name          = "${local.name}-${each.key}"
  alarm_description   = "${local.env.title}: ${each.value.why} (root ADR-0624 §7)."
  namespace           = local.log_namespace
  metric_name         = each.key
  statistic           = each.value.statistic
  extended_statistic  = each.value.extended
  period              = 60
  evaluation_periods  = local.alarms.window_minutes
  datapoints_to_alarm = each.key == "check-latency" ? local.alarms.window_minutes : 1
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  # No matching line is no datapoint: nothing is wrong.
  treat_missing_data = "notBreaching"
  alarm_actions      = [data.aws_sns_topic.alarms[0].arn]
}

resource "aws_cloudwatch_metric_alarm" "database" {
  for_each = local.alarms == null || local.own == null ? {} : {
    connections = {
      metric    = "DatabaseConnections", statistic = "Maximum", operator = "GreaterThanThreshold"
      threshold = floor(local.own.max_connections * local.alarms.database_connections_percent / 100)
      why       = "the catalog database's connections are above ${local.alarms.database_connections_percent}% of its max_connections (${local.own.max_connections})"
    }
    memory = {
      metric    = "FreeableMemory", statistic = "Minimum", operator = "LessThanThreshold"
      threshold = local.alarms.freeable_memory_floor_mb * 1024 * 1024
      why       = "the catalog database's freeable memory is below ${local.alarms.freeable_memory_floor_mb} MB"
    }
  }
  alarm_name          = "${local.name}-database-${each.key}"
  alarm_description   = "${local.env.title}: ${each.value.why} (root ADR-0624 §7)."
  namespace           = "AWS/RDS"
  metric_name         = each.value.metric
  dimensions          = { DBInstanceIdentifier = aws_db_instance.catalog[0].identifier }
  statistic           = each.value.statistic
  period              = 60
  evaluation_periods  = local.alarms.window_minutes
  datapoints_to_alarm = local.alarms.window_minutes
  threshold           = each.value.threshold
  comparison_operator = each.value.operator
  alarm_actions       = [data.aws_sns_topic.alarms[0].arn]
}
