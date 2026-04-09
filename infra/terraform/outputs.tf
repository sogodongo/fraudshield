output "data_lake_bucket" {
  description = "S3 bucket for bronze/silver/gold data lake"
  value       = aws_s3_bucket.data_lake.id
}

output "artifacts_bucket" {
  description = "S3 bucket for Lambda packages, Glue scripts, models"
  value       = aws_s3_bucket.artifacts.id
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.transactions.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.transactions.arn
}

output "fraud_decisions_table" {
  value = aws_dynamodb_table.fraud_decisions.name
}

output "merchant_profiles_table" {
  value = aws_dynamodb_table.merchant_profiles.name
}

# These ARNs are needed when deploying Lambda and Glue jobs
output "lambda_scorer_role_arn" {
  value = aws_iam_role.lambda_scorer.arn
}

output "glue_etl_role_arn" {
  value = aws_iam_role.glue_etl.arn
}

output "redshift_copy_role_arn" {
  value = aws_iam_role.redshift_copy.arn
}
