# Power BI Dashboards

## Connection Setup

1. Install the Amazon Redshift ODBC driver from the AWS website
2. In Power BI Desktop: Get Data > Amazon Redshift
3. Server: Redshift endpoint (from Terraform output `redshift_endpoint`)
4. Database: fraudshield
5. Schema: analytics
6. Authentication: Database credentials or IAM-based

Use Import mode during development for faster iteration.
Switch to DirectQuery for production dashboards that need real-time data.

## Data Sources

Each dashboard maps to specific materialized views:

| Dashboard | Primary View | Refresh Frequency |
|---|---|---|
| Fraud Operations | mv_fraud_overview | After each daily batch load |
| Chargeback Forecasting | mv_chargeback_trends | After each daily batch load |
| Executive KPIs | mv_daily_kpis | After each daily batch load |

Materialized views are refreshed automatically by the Airflow DAG
after the Redshift COPY step completes.

## Import Tables

Import these tables/views into Power BI:
- analytics.mv_fraud_overview
- analytics.mv_chargeback_trends
- analytics.mv_daily_kpis
- analytics.dim_merchants (for slicer filters)
- analytics.dim_date (for date intelligence)

## Relationships

Configure these relationships in Power BI Model view:
- mv_fraud_overview[transaction_date] -> dim_date[date_key] (many to one)
- mv_fraud_overview[risk_tier] -> dim_merchants[risk_tier] (many to many, filter both)
- mv_chargeback_trends[transaction_date] -> dim_date[date_key] (many to one)
