"""
Redshift data loader.

Handles daily COPY from S3 gold layer into Redshift fact tables,
deduplication on load, chargeback flag updates, and materialized
view refresh.

Called by the Airflow DAG after gold layer ETL completes.

Usage:
    python src/warehouse/load.py --date 2024-01-15 --tables fct_transactions
    python src/warehouse/load.py --refresh-views
    python src/warehouse/load.py --dry-run --date 2024-01-15
"""

import os
import argparse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redshift_loader")


class RedshiftLoader:
    """
    Manages data loading from S3 into Redshift.

    Connection is lazy — only established when execute() is called.
    This lets us generate and inspect SQL in dry-run mode without
    needing a live Redshift cluster.
    """

    def __init__(self, host=None, database=None, user=None, port=None):
        self.host = host or os.environ.get("REDSHIFT_HOST", "localhost")
        self.database = database or os.environ.get("REDSHIFT_DB", "fraudshield")
        self.user = user or os.environ.get("REDSHIFT_USER", "admin")
        self.port = port or int(os.environ.get("REDSHIFT_PORT", "5439"))
        self._conn = None

    def _get_connection(self):
        if self._conn is None:
            import psycopg2
            self._conn = psycopg2.connect(
                host=self.host,
                dbname=self.database,
                user=self.user,
                port=self.port,
                # in production password comes from Secrets Manager
                password=os.environ.get("REDSHIFT_PASSWORD", ""),
            )
            self._conn.autocommit = False
            logger.info(f"Connected to Redshift: {self.host}:{self.port}/{self.database}")
        return self._conn

    def execute(self, sql, dry_run=False):
        """Execute SQL against Redshift. In dry-run mode just logs the SQL."""
        if dry_run:
            logger.info(f"[DRY RUN] Would execute:\n{sql[:500]}")
            return

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
            logger.info("SQL executed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"SQL execution failed: {e}")
            raise
        finally:
            cursor.close()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


def build_copy_sql(s3_path, iam_role, staging_table="analytics.fct_transactions_staging"):
    """
    Generate COPY command to load parquet from S3 into staging table.
    COPY is Redshift's bulk loader — much faster than row-by-row INSERT.
    """
    return f"""
COPY {staging_table}
FROM '{s3_path}'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET;
""".strip()


def build_dedup_insert_sql(
    staging_table="analytics.fct_transactions_staging",
    target_table="analytics.fct_transactions",
    key_column="transaction_id"
):
    """
    Generate INSERT that only adds new records (dedup on key_column).
    LEFT JOIN + WHERE NULL is the standard Redshift dedup pattern.
    This makes the load idempotent — running it twice on the same data
    inserts nothing the second time.
    """
    return f"""
INSERT INTO {target_table}
SELECT s.*
FROM {staging_table} s
LEFT JOIN {target_table} t
    ON s.{key_column} = t.{key_column}
WHERE t.{key_column} IS NULL;
""".strip()


def build_chargeback_update_sql():
    """
    Update chargeback flags on transactions that now have matching chargebacks.
    Chargebacks arrive T+3 to T+45, so previously loaded transactions need
    their flags updated when new chargeback data comes in.
    """
    return """
UPDATE analytics.fct_transactions
SET chargeback_flag = TRUE,
    is_fraudulent = TRUE
FROM analytics.fct_chargebacks cb
WHERE analytics.fct_transactions.transaction_id = cb.transaction_id
  AND analytics.fct_transactions.chargeback_flag = FALSE;
""".strip()


def build_refresh_views_sql():
    """Refresh all materialized views used by Power BI."""
    views = [
        "analytics.mv_fraud_overview",
        "analytics.mv_chargeback_trends",
        "analytics.mv_daily_kpis",
    ]
    statements = [f"REFRESH MATERIALIZED VIEW {v};" for v in views]
    return "\n".join(statements)


def build_full_load_sql(s3_path, iam_role, processing_date):
    """
    Generate the complete daily load transaction.

    Wrapped in BEGIN/END so either everything succeeds or nothing changes.
    The staging table is truncated at the end regardless of outcome
    to keep it clean for the next run.
    """
    return f"""
-- Daily load for {processing_date}
BEGIN TRANSACTION;

-- Step 1: Load from S3 into staging
{build_copy_sql(s3_path, iam_role)}

-- Step 2: Dedup insert into fact table
{build_dedup_insert_sql()}

-- Step 3: Update chargeback flags
{build_chargeback_update_sql()}

-- Step 4: Clean up staging
TRUNCATE analytics.fct_transactions_staging;

END TRANSACTION;

-- Step 5: Refresh materialized views (outside transaction)
{build_refresh_views_sql()}
""".strip()


def run(date_str, tables, refresh_only=False, dry_run=False):
    """Main entry point called by Airflow or CLI."""
    iam_role = os.environ.get(
        "REDSHIFT_COPY_ROLE",
        "arn:aws:iam::496845880271:role/fraudshield-prod-redshift-copy"
    )
    s3_bucket = os.environ.get("S3_BUCKET", "fraudshield-prod-lake")

    loader = RedshiftLoader()

    try:
        if refresh_only:
            logger.info("Refreshing materialized views only")
            sql = build_refresh_views_sql()
            loader.execute(sql, dry_run=dry_run)
            return

        s3_path = f"s3://{s3_bucket}/gold/fraud_summary_daily/dt={date_str}/"
        logger.info(f"Loading data for {date_str} from {s3_path}")

        sql = build_full_load_sql(s3_path, iam_role, date_str)
        loader.execute(sql, dry_run=dry_run)

        logger.info(f"Load complete for {date_str}")

    finally:
        loader.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load gold data into Redshift")
    parser.add_argument("--date", type=str, default=datetime.utcnow().strftime("%Y-%m-%d"),
                        help="Processing date (YYYY-MM-DD)")
    parser.add_argument("--tables", nargs="+", default=["fct_transactions"],
                        help="Tables to load")
    parser.add_argument("--refresh-views", action="store_true",
                        help="Only refresh materialized views")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print SQL without executing")
    args = parser.parse_args()

    run(args.date, args.tables, refresh_only=args.refresh_views, dry_run=args.dry_run)
