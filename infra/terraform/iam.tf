# ==============================================================================
# IAM Roles and Policies
# ==============================================================================
# Each service gets a dedicated role scoped to minimum required permissions.
# This follows the principle of least privilege.

# ── Lambda Scorer Role ────────────────────────────────────────────────────────
# Needs: read from Kinesis, read/write DynamoDB, read S3 (model files),
#         write CloudWatch logs

resource "aws_iam_role" "lambda_scorer" {
  name = "${local.prefix}-lambda-scorer"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_scorer_policy" {
  name = "${local.prefix}-lambda-scorer-policy"
  role = aws_iam_role.lambda_scorer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KinesisRead"
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:ListShards",
          "kinesis:ListStreams",
        ]
        Resource = aws_kinesis_stream.transactions.arn
      },
      {
        Sid    = "DynamoDBReadWrite"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
        ]
        Resource = [
          aws_dynamodb_table.fraud_decisions.arn,
          aws_dynamodb_table.merchant_profiles.arn,
        ]
      },
      {
        Sid    = "S3ReadModel"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
        ]
        Resource = "${aws_s3_bucket.artifacts.arn}/models/*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      },
    ]
  })
}

# ── Firehose Delivery Role ────────────────────────────────────────────────────
# Needs: read from Kinesis, write to S3 bronze layer

resource "aws_iam_role" "firehose" {
  name = "${local.prefix}-firehose"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "firehose.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "${local.prefix}-firehose-policy"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KinesisRead"
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListShards",
        ]
        Resource = aws_kinesis_stream.transactions.arn
      },
      {
        Sid    = "S3Write"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:AbortMultipartUpload",
        ]
        Resource = "${aws_s3_bucket.data_lake.arn}/bronze/*"
      },
    ]
  })
}

# ── Glue ETL Role ─────────────────────────────────────────────────────────────
# Needs: read/write S3 (all layers), Glue catalog access, CloudWatch logs

resource "aws_iam_role" "glue_etl" {
  name = "${local.prefix}-glue-etl"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "glue_etl_policy" {
  name = "${local.prefix}-glue-etl-policy"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadWriteDataLake"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      },
      {
        Sid    = "S3ReadArtifacts"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
        ]
        Resource = "${aws_s3_bucket.artifacts.arn}/glue/*"
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      },
    ]
  })
}

# ── Redshift COPY Role ────────────────────────────────────────────────────────
# Needs: read S3 gold layer only

resource "aws_iam_role" "redshift_copy" {
  name = "${local.prefix}-redshift-copy"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "redshift.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_copy_policy" {
  name = "${local.prefix}-redshift-copy-policy"
  role = aws_iam_role.redshift_copy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadGold"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/gold/*",
        ]
      },
    ]
  })
}
