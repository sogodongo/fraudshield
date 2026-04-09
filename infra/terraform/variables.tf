variable "project_name" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "fraudshield"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "kinesis_shard_count" {
  description = "Number of shards for the Kinesis stream"
  type        = number
  default     = 2
}

variable "kinesis_retention_hours" {
  description = "Kinesis data retention in hours"
  type        = number
  default     = 24
}

variable "lambda_memory_mb" {
  description = "Memory allocation for the scoring Lambda"
  type        = number
  default     = 512
}

variable "lambda_timeout_seconds" {
  description = "Timeout for the scoring Lambda"
  type        = number
  default     = 30
}

variable "redshift_base_rpu" {
  description = "Base RPU capacity for Redshift Serverless"
  type        = number
  default     = 8
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}
