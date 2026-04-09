project_name           = "fraudshield"
environment            = "dev"
aws_region             = "us-east-1"
kinesis_shard_count    = 2
kinesis_retention_hours = 24
lambda_memory_mb       = 512
lambda_timeout_seconds = 30
redshift_base_rpu      = 8

tags = {
  Team  = "data-engineering"
  Owner = "sam.odongo"
}
