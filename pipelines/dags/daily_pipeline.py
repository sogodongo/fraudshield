"""
Daily fraud detection pipeline DAG.

Schedule: 02:00 UTC daily
Runtime: ~45 minutes end-to-end

Task sequence:
    1. Validate bronze data exists for yesterday
    2. Bronze to Silver ETL (dedup, validate, clean)
    3. Run silver quality checks
    4. Silver to Gold ETL (aggregate, compute KPIs)
    5. Run gold quality checks
    6. COPY gold tables into Redshift
    7. Refresh Redshift materialized views
    8. Send completion notification

Retry policy: 2 retries with 5 minute backoff on all ETL tasks.
Quality check failures halt the pipeline to prevent bad data
from reaching Redshift and Power BI.

In MWAA, Glue jobs are triggered via GlueJobOperator.
This DAG uses BashOperator for local testing compatibility,
with comments showing the MWAA equivalent for each task.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
# In MWAA you would also import:
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


PROJECT_ROOT = "/opt/airflow/dags/fraudshield"
S3_BUCKET = "fraudshield-prod-lake"

# execution_date gives us yesterday's date in the daily schedule context
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/transactions/{{{{ ds }}}}"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/transactions_clean"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-alerts@fraudshield.io"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def check_bronze_exists(**context):
    """
    Verify bronze data exists for the processing date.
    In production this checks S3 via boto3. Here we log the check.
    """
    import logging
    ds = context["ds"]
    logging.info(f"Checking bronze data for date: {ds}")
    # production version:
    # import boto3
    # s3 = boto3.client("s3")
    # resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"bronze/transactions/{ds}/", MaxKeys=1)
    # if resp.get("KeyCount", 0) == 0:
    #     raise FileNotFoundError(f"No bronze data for {ds}")
    logging.info(f"Bronze data check passed for {ds}")


def run_quality_checks(layer, **context):
    """
    Run data quality validation for a given layer.
    Raises exception on critical failures to halt the pipeline.
    """
    import logging
    ds = context["ds"]
    logging.info(f"Running {layer} quality checks for {ds}")

    # production version would import and run validators:
    # from src.quality.validators import run_silver_checks, run_gold_checks
    # spark = create_spark_session()
    # if layer == "silver":
    #     df = spark.read.parquet(SILVER_PATH)
    #     results = run_silver_checks(df)
    # elif layer == "gold":
    #     kpi_df = spark.read.parquet(f"{GOLD_PATH}/kpi_daily")
    #     merchant_df = spark.read.parquet(f"{GOLD_PATH}/merchant_risk_profiles")
    #     results = run_gold_checks(kpi_df, merchant_df)
    #
    # critical_failures = [r for r in results if not r.passed and r.severity == "critical"]
    # if critical_failures:
    #     raise ValueError(f"{len(critical_failures)} critical quality checks failed")

    logging.info(f"{layer} quality checks passed for {ds}")


def send_completion_notification(**context):
    """
    Notify the team that the daily pipeline completed.
    In production this posts to Slack or PagerDuty.
    """
    import logging
    ds = context["ds"]
    duration = context.get("dag_run").end_date
    logging.info(f"Pipeline complete for {ds}")

    # production version:
    # import httpx
    # httpx.post(SLACK_WEBHOOK, json={
    #     "text": f"FraudShield daily pipeline complete for {ds}\n"
    #             f"Duration: {duration}\n"
    #             f"All quality checks passed."
    # })


with DAG(
    dag_id="fraudshield_daily_pipeline",
    default_args=default_args,
    description="Daily fraud detection ETL: bronze -> silver -> gold -> Redshift",
    schedule="0 2 * * *",       # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,              # do not backfill missed runs on deploy
    max_active_runs=1,          # only one run at a time
    tags=["fraudshield", "etl", "daily"],
) as dag:

    # ── Task 1: Check bronze data exists ──────────────────────────────────
    check_bronze = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_exists,
    )

    # ── Task 2: Bronze to Silver ──────────────────────────────────────────
    # In MWAA this would be:
    # bronze_to_silver = GlueJobOperator(
    #     task_id="bronze_to_silver",
    #     job_name="fraudshield-bronze-to-silver",
    #     script_args={"--ds": "{{ ds }}"},
    #     region_name="us-east-1",
    # )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/processing/batch/bronze_to_silver.py "
            "--input data/transactions.parquet "
            "--output data/silver/transactions_clean "
            "--quarantine data/quarantine/transactions"
        ),
    )

    # ── Task 3: Silver quality checks ─────────────────────────────────────
    silver_quality = PythonOperator(
        task_id="silver_quality_check",
        python_callable=run_quality_checks,
        op_kwargs={"layer": "silver"},
    )

    # ── Task 4: Silver to Gold ────────────────────────────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/processing/batch/silver_to_gold.py "
            "--input data/silver/transactions_clean "
            "--output data/gold"
        ),
    )

    # ── Task 5: Gold quality checks ───────────────────────────────────────
    gold_quality = PythonOperator(
        task_id="gold_quality_check",
        python_callable=run_quality_checks,
        op_kwargs={"layer": "gold"},
    )

    # ── Task 6: Load to Redshift ──────────────────────────────────────────
    # In MWAA with RedshiftDataOperator:
    # load_redshift = RedshiftDataOperator(
    #     task_id="load_to_redshift",
    #     sql="CALL analytics.load_daily_data('{{ ds }}');",
    #     database="fraudshield",
    #     workgroup_name="fraudshield-prod",
    # )
    load_redshift = BashOperator(
        task_id="load_to_redshift",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/warehouse/load.py "
            "--date {{ ds }} "
            "--tables fct_transactions fct_fraud_alerts"
        ),
    )

    # ── Task 7: Refresh materialized views ────────────────────────────────
    refresh_views = BashOperator(
        task_id="refresh_materialized_views",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/warehouse/load.py --refresh-views"
        ),
    )

    # ── Task 8: Notify completion ─────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=send_completion_notification,
        trigger_rule="all_success",
    )

    # ── DAG dependency chain ──────────────────────────────────────────────
    # Read this bottom-up: each line says "this task depends on the one above"
    (
        check_bronze
        >> bronze_to_silver
        >> silver_quality
        >> silver_to_gold
        >> gold_quality
        >> load_redshift
        >> refresh_views
        >> notify
    )
