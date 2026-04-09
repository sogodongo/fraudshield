# Pull in the current AWS account ID and region.
# Some IAM policies need these for constructing full ARNs.
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
