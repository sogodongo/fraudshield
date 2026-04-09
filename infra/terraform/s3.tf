# ==============================================================================
# S3 Data Lake
# ==============================================================================
# Two buckets:
#   1. Data lake bucket — bronze/silver/gold layers
#   2. Artifacts bucket — Lambda code packages, Glue scripts, model files

resource "aws_s3_bucket" "data_lake" {
  bucket        = "${local.prefix}-lake"
  force_destroy = var.environment == "dev" ? true : false
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Bronze layer: raw data moves to Infrequent Access after 30 days,
  # then to Glacier after 90 days. Keeps costs low for data we rarely
  # re-read but need to retain for reprocessing and compliance.
  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # Silver layer: move to IA after 60 days.
  # Silver is re-read more frequently than bronze during investigations.
  rule {
    id     = "silver-lifecycle"
    status = "Enabled"
    filter {
      prefix = "silver/"
    }
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }

  # Gold layer: stays in Standard. Actively queried by Redshift COPY
  # and Power BI. Moving to IA would increase retrieval costs.
  rule {
    id     = "gold-lifecycle"
    status = "Enabled"
    filter {
      prefix = "gold/"
    }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Artifacts bucket — Lambda zips, Glue scripts, ML model files
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.prefix}-artifacts"
  force_destroy = var.environment == "dev" ? true : false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
