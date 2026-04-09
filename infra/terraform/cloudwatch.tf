# ==============================================================================
# CloudWatch Monitoring
# ==============================================================================
# Alarms for critical pipeline components. SNS topic delivers alerts
# to email and can be extended to PagerDuty or Slack.

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.prefix}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alerts" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = "data-alerts@fraudshield.io"
  # endpoint would come from a variable in production
}

# ── Lambda Scorer Alarms ──────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.prefix}-lambda-scorer-errors"
  alarm_description   = "Lambda scoring function error rate exceeded threshold"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = "${local.prefix}-fraud-scorer"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]
}

# P95 latency alarm — scoring must complete under 1 second
resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  alarm_name          = "${local.prefix}-lambda-scorer-latency"
  alarm_description   = "Fraud scoring P95 latency exceeded 1 second"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 300
  extended_statistic  = "p95"
  threshold           = 1000    # milliseconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = "${local.prefix}-fraud-scorer"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# ── Kinesis Alarms ────────────────────────────────────────────────────────────

# High iterator age means consumers are falling behind.
# If this exceeds 5 minutes, transactions are being scored late.
resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "${local.prefix}-kinesis-iterator-age"
  alarm_description   = "Kinesis consumer falling behind — iterator age > 5 min"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300000  # 5 minutes in milliseconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.transactions.name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# Write throttling means producers are sending faster than
# the stream can accept. Need more shards or backpressure.
resource "aws_cloudwatch_metric_alarm" "kinesis_write_throttle" {
  alarm_name          = "${local.prefix}-kinesis-write-throttled"
  alarm_description   = "Kinesis write throughput exceeded — consider adding shards"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.transactions.name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# ── DynamoDB Alarms ───────────────────────────────────────────────────────────

# Throttled reads on the fraud decisions table would mean
# downstream services cannot look up scoring results.
resource "aws_cloudwatch_metric_alarm" "dynamodb_throttle" {
  alarm_name          = "${local.prefix}-dynamodb-read-throttle"
  alarm_description   = "DynamoDB fraud_decisions table read throttling detected"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ReadThrottleEvents"
  namespace           = "AWS/DynamoDB"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = aws_dynamodb_table.fraud_decisions.name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# ── Glue Job Alarms ───────────────────────────────────────────────────────────

# Glue job failure triggers an alarm. The Airflow DAG also retries,
# but this catches cases where Airflow itself is down.
resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${local.prefix}-glue-job-failed"
  alarm_description   = "A Glue ETL job has failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 3600    # hourly check
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# ── SQS Dead Letter Queue Alarm ──────────────────────────────────────────────

# DLQ depth > 100 means many Lambda invocations are failing.
# Likely a systemic issue rather than transient errors.
resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${local.prefix}-dlq-depth-high"
  alarm_description   = "Scoring DLQ depth exceeds 100 — systemic Lambda failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = "${local.prefix}-scoring-dlq"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# ── CloudWatch Dashboard ──────────────────────────────────────────────────────
# Single-pane view of platform health for the on-call engineer.

resource "aws_cloudwatch_dashboard" "platform" {
  dashboard_name = "${local.prefix}-platform-health"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Lambda Scorer - Invocations & Errors"
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", "${local.prefix}-fraud-scorer", { stat = "Sum" }],
            ["AWS/Lambda", "Errors", "FunctionName", "${local.prefix}-fraud-scorer", { stat = "Sum", color = "#d62728" }],
          ]
          period = 300
          region = var.aws_region
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Lambda Scorer - Duration (P50/P95/P99)"
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-fraud-scorer", { stat = "p50" }],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-fraud-scorer", { stat = "p95", color = "#ff7f0e" }],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-fraud-scorer", { stat = "p99", color = "#d62728" }],
          ]
          period = 300
          region = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Kinesis - Incoming Records & Iterator Age"
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", "${local.prefix}-transactions", { stat = "Sum" }],
            ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", "${local.prefix}-transactions", { stat = "Maximum", yAxis = "right" }],
          ]
          period = 300
          region = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "DynamoDB - Read/Write Capacity"
          metrics = [
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", "${local.prefix}-fraud-decisions", { stat = "Sum" }],
            ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", "${local.prefix}-fraud-decisions", { stat = "Sum", color = "#ff7f0e" }],
          ]
          period = 300
          region = var.aws_region
        }
      },
    ]
  })
}
