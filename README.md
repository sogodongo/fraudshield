# FraudShield

Real-time and batch fraud detection platform for payment transactions on AWS.

## Overview

FraudShield ingests payment transaction events, scores them for fraud risk in near-real-time, and delivers curated analytics to a data warehouse for business intelligence. The platform supports approximately 50K transactions per minute at peak load, with a P95 scoring latency target of under 3 seconds.

The system serves a mid-size payment processor where fraud operations requires both immediate transaction decisioning and historical pattern analysis for rule tuning and model retraining.

## Business Context

Chargebacks from fraudulent transactions represent approximately 2.3% of gross transaction volume. The existing rule-based detection system captures roughly 61% of fraudulent activity with a false positive rate exceeding 15%.

### Stakeholders

| Stakeholder | Primary Need |
|---|---|
| Fraud Operations | Real-time alerts, investigation dashboards |
| Finance | Chargeback forecasting, loss provisioning |
| Product | Transaction approval rate optimization |
| Compliance | SAR filing support, audit trail |

### Target KPIs

| Metric | Baseline | Target |
|---|---|---|
| Fraud detection rate | 61% | 85%+ |
| False positive rate | 15.2% | < 5% |
| Mean time to detect | 4.2 hours | < 10 seconds |
| Chargeback rate | 2.3% | < 0.8% |

## System Architecture

The platform uses a dual-path architecture to satisfy two fundamentally different latency and throughput requirements:

- **Streaming path** handles real-time transaction scoring via Kinesis, Lambda, and DynamoDB.
- **Batch path** handles analytics, enrichment, and model retraining via S3, Glue (PySpark), and Redshift.

Both paths share a medallion-layered data lake on S3 (bronze, silver, gold).
```
Payment API --> Kinesis Data Streams --> Lambda (scoring) --> DynamoDB (decisions)
                     |                                            |
                     v                                            v
                S3 Bronze --> Glue ETL --> S3 Silver --> Glue --> S3 Gold
                                                                  |
                                                                  v
                                                              Redshift
                                                                  |
                                                                  v
                                                              Power BI
```

Refer to [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for Architecture Decision Records, failure handling strategy, security posture, and cost estimates.

### Technology Decisions

| Layer | Service | Rationale |
|---|---|---|
| Stream ingestion | Kinesis Data Streams | ~60% lower cost than MSK at current volume; native Lambda integration eliminates consumer group management |
| Batch ingestion | S3 + EventBridge | Partner banks deliver daily file drops; event-driven triggers avoid polling |
| Real-time processing | Lambda | Stateless scoring function (200-800ms); scales to zero during off-peak hours |
| Batch processing | AWS Glue (PySpark) | Serverless Spark; eliminates cluster lifecycle management; Glue Data Catalog doubles as Hive-compatible metastore |
| Data lake | S3 (medallion layers) | Decouples storage from compute; Parquet with Snappy compression; lifecycle policies for cost optimization |
| Data warehouse | Redshift Serverless | Materialized views for dashboard acceleration; COPY from S3 is well-optimized; concurrency scaling for Power BI parallel queries |
| Orchestration | MWAA (Airflow) | Existing team expertise; managed service removes infrastructure overhead |
| Monitoring | CloudWatch | Unified with AWS native tooling; custom metrics for pipeline-specific observability |
| Infrastructure | Terraform | State management, multi-environment support, module reuse |

## Data Model

Star schema in Redshift. Three fact tables (`fct_transactions`, `fct_chargebacks`, `fct_fraud_alerts`) and three dimension tables (`dim_merchants` with SCD Type 2, `dim_date`, `dim_card_bins`).

Refer to [docs/DATA_MODEL.md](docs/DATA_MODEL.md) for full DDL, distribution and sort key rationale, materialized view definitions, and incremental loading strategy.

## Repository Structure
```
fraudshield/
├── docs/                        # Architecture docs, ADRs, data model, setup guide
├── infra/
│   ├── terraform/               # Root Terraform configs per environment
│   └── modules/                 # Reusable Terraform modules
├── src/
│   ├── ingestion/               # Kinesis producer, S3 upload handlers
│   ├── processing/
│   │   ├── batch/               # Glue jobs (PySpark)
│   │   └── streaming/           # Lambda scoring function
│   ├── quality/                 # Data quality validation
│   └── warehouse/               # Redshift DDL, load scripts
├── pipelines/
│   ├── dags/                    # Airflow DAG definitions
│   └── step_functions/          # ASL definitions
├── dashboards/                  # Power BI templates, DAX measures
├── tests/                       # Unit and integration tests
├── scripts/                     # Data generators, utilities
└── .github/workflows/           # CI/CD pipelines
```

## Design Tradeoffs

**Kinesis over MSK (Kafka):** At the current volume of approximately 50K events per minute, Kinesis is significantly cheaper and operationally simpler. The 1MB record size limit and 7-day retention cap are not constraints for transaction events averaging 2KB. If throughput requirements exceed 500K events per minute or multi-region replication becomes necessary, MSK would be the appropriate choice.

**Glue over EMR:** Glue cold start adds 2-3 minutes per job. For daily batch runs this overhead is acceptable. For hourly micro-batch workloads, EMR Serverless would be more appropriate. The operational simplicity of Glue (no cluster management, built-in job bookmarks) justifies the cost premium at current scale.

**Parquet over Iceberg/Delta Lake:** Schema changes are infrequent enough that Parquet files with Glue Data Catalog versioning are sufficient. Iceberg would provide time travel, hidden partitioning, and better schema evolution support. This is the highest-priority migration for a future iteration.

**Simplified scoring model:** The current fraud scoring model is a logistic regression. It exists to validate the end-to-end pipeline. A production deployment would use gradient boosting (XGBoost or LightGBM) with features from a dedicated feature store.

**Inline feature computation:** Features are currently computed independently in both the streaming and batch paths. This creates potential feature drift between real-time and historical scoring. Unifying feature computation through a feature store (such as Feast backed by DynamoDB and Redshift) is the primary technical debt item.

## Roadmap

- Implement a unified feature store to eliminate online/offline feature drift
- Add a feedback loop where fraud analyst decisions trigger model retraining
- Introduce Glue Schema Registry for formal schema evolution management
- Migrate the data lake to Apache Iceberg for time travel and improved partition handling
- Replace raw SQL warehouse transformations with dbt for version-controlled, tested analytics models

## Setup

Refer to [docs/SETUP.md](docs/SETUP.md) for prerequisites, environment configuration, infrastructure provisioning, and end-to-end pipeline execution.
