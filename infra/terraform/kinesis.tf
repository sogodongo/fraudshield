# ==============================================================================
# Kinesis Data Streams
# ==============================================================================
# Primary ingestion stream for payment transaction events.
# 2 shards at 2MB/s write capacity handles ~50K events/min at 2KB avg size.

resource "aws_kinesis_stream" "transactions" {
  name             = "${local.prefix}-transactions"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"
}

# Firehose delivery stream — buffers Kinesis records and writes
# Parquet files to the S3 bronze layer automatically.
resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "${local.prefix}-firehose-to-s3"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.transactions.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.data_lake.arn
    prefix              = "bronze/transactions/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "bronze/transactions_errors/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_size     = 128  # MB
    buffering_interval = 300  # seconds (5 minutes)
    compression_format = "UNCOMPRESSED"

    # TODO: Add Glue Data Catalog conversion to write Parquet directly
    # For now, Firehose writes JSON. The bronze_to_silver Glue job
    # handles Parquet conversion during the first ETL stage.
  }
}
