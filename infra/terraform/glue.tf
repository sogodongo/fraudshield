# ==============================================================================
# AWS Glue Jobs
# ==============================================================================
# Two ETL jobs matching our batch pipeline.
# Scripts are uploaded to the artifacts S3 bucket during deployment.

resource "aws_glue_catalog_database" "fraudshield" {
  name = "${local.prefix}-catalog"
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "${local.prefix}-bronze-to-silver"
  role_arn = aws_iam_role.glue_etl.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/glue/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"               = "python"
    "--job-bookmark-option"        = "job-bookmark-enable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--input_path"                 = "s3://${aws_s3_bucket.data_lake.id}/bronze/transactions/"
    "--output_path"                = "s3://${aws_s3_bucket.data_lake.id}/silver/transactions_clean/"
    "--quarantine_path"            = "s3://${aws_s3_bucket.data_lake.id}/bronze/transactions_quarantine/"
  }

  glue_version      = "4.0"
  number_of_workers = 4
  worker_type       = "G.1X"
  timeout           = 60    # minutes
  max_retries       = 1

  execution_property {
    max_concurrent_runs = 1
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name     = "${local.prefix}-silver-to-gold"
  role_arn = aws_iam_role.glue_etl.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/glue/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"               = "python"
    "--job-bookmark-option"        = "job-bookmark-disable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--input_path"                 = "s3://${aws_s3_bucket.data_lake.id}/silver/transactions_clean/"
    "--output_path"                = "s3://${aws_s3_bucket.data_lake.id}/gold/"
  }

  glue_version      = "4.0"
  number_of_workers = 4
  worker_type       = "G.1X"
  timeout           = 60
  max_retries       = 1

  execution_property {
    max_concurrent_runs = 1
  }
}

# Crawler discovers schema changes in the bronze layer automatically.
# Runs after Firehose delivers new data, updates the Glue Data Catalog.
resource "aws_glue_crawler" "bronze_transactions" {
  name          = "${local.prefix}-bronze-transactions-crawler"
  role          = aws_iam_role.glue_etl.arn
  database_name = aws_glue_catalog_database.fraudshield.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/bronze/transactions/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  schedule = "cron(30 1 * * ? *)"  # 01:30 UTC daily, before the ETL DAG
}
