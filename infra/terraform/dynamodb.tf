# ==============================================================================
# DynamoDB Tables
# ==============================================================================
# Two tables:
#   1. fraud_decisions — written by Lambda scorer, one row per scored transaction
#   2. merchant_profiles — read by Lambda for enrichment during scoring

resource "aws_dynamodb_table" "fraud_decisions" {
  name         = "${local.prefix}-fraud-decisions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "transaction_id"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  # TTL removes old decisions after 90 days.
  # Historical decisions are already in Redshift by then.
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.environment == "prod" ? true : false
  }
}

resource "aws_dynamodb_table" "merchant_profiles" {
  name         = "${local.prefix}-merchant-profiles"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "merchant_id"

  attribute {
    name = "merchant_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = var.environment == "prod" ? true : false
  }
}
