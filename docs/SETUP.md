# Setup Guide

## Prerequisites

- AWS account with IAM permissions for Kinesis, Lambda, S3, Glue, Redshift, DynamoDB, MWAA, and CloudWatch
- Terraform >= 1.5
- Python 3.10+
- Docker and Docker Compose (for local testing)
- AWS CLI v2, configured with a named profile
- Power BI Desktop (for dashboard development)

## Local Development
```bash
git clone https://github.com/sogodongo/fraudshield.git
cd fraudshield
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Generate sample transaction data
python scripts/generate_transactions.py --count 100000 --output data/transactions.parquet

# Run tests
python -m pytest tests/ -v
```

## Environment Configuration

Create a `.env` file in the project root:
```
AWS_PROFILE=fraudshield-dev
AWS_REGION=us-east-1
S3_BUCKET=fraudshield-lake-dev
REDSHIFT_HOST=<redshift-endpoint>
REDSHIFT_DB=fraudshield
REDSHIFT_USER=admin
REDSHIFT_PORT=5439
```

This file is excluded from version control via `.gitignore`.

## Infrastructure Provisioning

Terraform creates resources in the following order:

1. S3 buckets (data lake, code artifacts)
2. Kinesis stream and Firehose delivery stream
3. DynamoDB tables
4. Lambda functions (packaged from src/processing/streaming/)
5. Glue jobs and crawlers
6. Redshift Serverless workgroup
7. MWAA environment
8. CloudWatch dashboards and alarms
9. IAM roles and policies
```bash
cd infra/terraform
cp example.tfvars dev.tfvars    # populate with account-specific values
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

Full provisioning takes approximately 20 minutes. MWAA alone accounts for roughly 15 minutes.

## End-to-End Pipeline Execution
```bash
# Start the transaction producer (simulates payment API traffic)
python src/ingestion/kinesis_producer.py --rate 100 --duration 300

# Trigger batch processing manually (or wait for Airflow schedule)
aws glue start-job-run --job-name fraudshield-bronze-to-silver

# Verify data in Redshift via psql or a SQL client
```

## Power BI Connection

1. Install the Amazon Redshift ODBC driver
2. In Power BI Desktop: Get Data > Amazon Redshift
3. Server: Redshift endpoint from Terraform output
4. Database: fraudshield
5. Use the `analytics` schema (contains materialized views optimized for BI)
6. Import mode is recommended for development; DirectQuery for production dashboards

## Troubleshooting

**Glue job fails with "No space left on device":** Increase the worker count in the Glue job configuration. The default is 2 DPUs; increase to 4 for larger datasets.

**Lambda cold starts exceeding 5 seconds:** Enable provisioned concurrency. The Terraform configuration includes this as a commented-out option (it incurs cost even at idle).

**Firehose not writing to S3:** Check the Firehose delivery stream error log in CloudWatch. This is typically caused by an IAM role permissions issue.

**Redshift COPY fails with "Access Denied":** Verify the IAM role ARN in the COPY command matches the role attached to the Redshift cluster, and that the role has s3:GetObject permission on the target prefix.
