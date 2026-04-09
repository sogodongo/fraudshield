"""
Silver to Gold transformation.

Reads cleaned transaction data from the silver layer and produces
aggregated, analytics-ready tables in the gold layer. These tables
map directly to Redshift fact tables and Power BI dashboards.

Three outputs:
  1. fraud_summary_daily   — daily aggregates by category and channel
  2. merchant_risk_profiles — per-merchant fraud metrics
  3. kpi_daily             — platform-level KPIs matching README targets

Glue invocation:
    aws glue start-job-run --job-name fraudshield-silver-to-gold

Local invocation:
    python src/processing/batch/silver_to_gold.py \
        --input data/silver/transactions_clean \
        --output data/gold
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_to_gold")


def create_spark_session(app_name="silver_to_gold"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_silver(spark, input_path):
    logger.info(f"Reading silver data from: {input_path}")
    df = spark.read.parquet(input_path)
    count = df.count()
    logger.info(f"Silver rows: {count}")
    return df


def build_fraud_summary_daily(df):
    """
    Daily fraud metrics broken down by merchant category and channel.
    This powers the Fraud Operations Overview dashboard in Power BI.

    One row per (date, merchant_id, channel) combination.
    """
    logger.info("Building fraud_summary_daily")

    summary = (
        df
        .groupBy(
            F.col("transaction_date"),
            F.col("merchant_id"),
            F.col("channel"),
        )
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("total_volume"),
            F.avg("amount_usd").alias("avg_transaction_amount"),

            # fraud metrics
            F.sum(F.when(F.col("is_fraudulent") == True, 1).otherwise(0))
                .alias("fraud_count"),
            F.sum(F.when(F.col("is_fraudulent") == True, F.col("amount_usd")).otherwise(0))
                .alias("fraud_volume"),
            F.avg("fraud_score").alias("avg_fraud_score"),

            # decision breakdown
            F.sum(F.when(F.col("fraud_decision") == "approved", 1).otherwise(0))
                .alias("approved_count"),
            F.sum(F.when(F.col("fraud_decision") == "held", 1).otherwise(0))
                .alias("held_count"),
            F.sum(F.when(F.col("fraud_decision") == "declined", 1).otherwise(0))
                .alias("declined_count"),

            # high-risk transactions (score > 0.7)
            F.sum(F.when(F.col("fraud_score") > 0.7, 1).otherwise(0))
                .alias("high_risk_count"),

            # latency stats for monitoring
            F.avg("processing_latency_ms").alias("avg_latency_ms"),
            F.expr("percentile_approx(processing_latency_ms, 0.95)")
                .alias("p95_latency_ms"),
        )
        # derived rate columns are easier to compute after the aggregation
        .withColumn("fraud_rate",
                    F.col("fraud_count") / F.col("total_transactions"))
        .withColumn("decline_rate",
                    F.col("declined_count") / F.col("total_transactions"))
    )

    row_count = summary.count()
    logger.info(f"fraud_summary_daily: {row_count} rows")
    return summary


def build_merchant_risk_profiles(df):
    """
    Per-merchant fraud metrics across the full date range.

    In production this would use a rolling 30-day window.
    For the initial build we compute across all available data
    and add the window logic once we have enough history.
    """
    logger.info("Building merchant_risk_profiles")

    profiles = (
        df
        .groupBy("merchant_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("total_volume"),
            F.avg("amount_usd").alias("avg_amount"),

            F.sum(F.when(F.col("is_fraudulent") == True, 1).otherwise(0))
                .alias("fraud_count"),
            F.sum(F.when(F.col("is_fraudulent") == True, F.col("amount_usd")).otherwise(0))
                .alias("fraud_volume"),

            F.avg("fraud_score").alias("avg_fraud_score"),
            F.max("fraud_score").alias("max_fraud_score"),

            F.countDistinct("card_token").alias("unique_cards"),
            F.min("transaction_date").alias("first_transaction"),
            F.max("transaction_date").alias("last_transaction"),
        )
        .withColumn("fraud_rate",
                    F.col("fraud_count") / F.col("total_transactions"))
        # assign risk tiers based on observed fraud rate
        .withColumn("computed_risk_tier",
                    F.when(F.col("fraud_rate") >= 0.06, "critical")
                     .when(F.col("fraud_rate") >= 0.04, "high")
                     .when(F.col("fraud_rate") >= 0.02, "medium")
                     .otherwise("low"))
    )

    row_count = profiles.count()
    logger.info(f"merchant_risk_profiles: {row_count} rows")
    return profiles


def build_kpi_daily(df):
    """
    Platform-level KPIs computed per day.
    These match the target metrics in the README:
      - fraud detection rate
      - false positive rate
      - chargeback rate (simulated from is_fraudulent flag)

    In production, chargeback data arrives separately at T+3 to T+45.
    Here we approximate using the is_fraudulent flag from the generator.
    """
    logger.info("Building kpi_daily")

    kpis = (
        df
        .groupBy("transaction_date")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("total_volume"),

            # actual fraud count (ground truth from is_fraudulent)
            F.sum(F.when(F.col("is_fraudulent") == True, 1).otherwise(0))
                .alias("actual_fraud_count"),

            # detected: fraud score above threshold AND actually fraudulent
            F.sum(F.when(
                (F.col("fraud_score") > 0.45) & (F.col("is_fraudulent") == True), 1
            ).otherwise(0)).alias("detected_fraud_count"),

            # false positives: flagged but not actually fraud
            F.sum(F.when(
                (F.col("fraud_score") > 0.45) & (F.col("is_fraudulent") == False), 1
            ).otherwise(0)).alias("false_positive_count"),

            # total flagged (held + declined)
            F.sum(F.when(F.col("fraud_score") > 0.45, 1).otherwise(0))
                .alias("total_flagged"),

            F.avg("fraud_score").alias("avg_fraud_score"),
            F.avg("processing_latency_ms").alias("avg_latency_ms"),
        )
        .withColumn("fraud_detection_rate",
                    F.when(F.col("actual_fraud_count") > 0,
                           F.col("detected_fraud_count") / F.col("actual_fraud_count"))
                     .otherwise(0.0))
        .withColumn("false_positive_rate",
                    F.when(F.col("total_flagged") > 0,
                           F.col("false_positive_count") / F.col("total_flagged"))
                     .otherwise(0.0))
        .withColumn("chargeback_rate",
                    F.col("actual_fraud_count") / F.col("total_transactions"))
    )

    row_count = kpis.count()
    logger.info(f"kpi_daily: {row_count} rows")
    return kpis


def write_gold(df, output_path, table_name):
    """Write a gold table as partitioned parquet."""
    full_path = f"{output_path}/{table_name}"
    row_count = df.count()
    logger.info(f"Writing {row_count} rows to {full_path}")

    df.coalesce(2).write.mode("overwrite").parquet(full_path)
    logger.info(f"{table_name} write complete")


def run(input_path, output_path):
    """Full silver-to-gold pipeline."""
    spark = create_spark_session()

    try:
        silver_df = read_silver(spark, input_path)

        # build all three gold tables
        fraud_summary = build_fraud_summary_daily(silver_df)
        merchant_profiles = build_merchant_risk_profiles(silver_df)
        kpis = build_kpi_daily(silver_df)

        # write them out
        Path(output_path).mkdir(parents=True, exist_ok=True)
        write_gold(fraud_summary, output_path, "fraud_summary_daily")
        write_gold(merchant_profiles, output_path, "merchant_risk_profiles")
        write_gold(kpis, output_path, "kpi_daily")

        return {
            "fraud_summary_rows": fraud_summary.count(),
            "merchant_profile_rows": merchant_profiles.count(),
            "kpi_rows": kpis.count(),
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver to Gold ETL")
    parser.add_argument("--input", default="data/silver/transactions_clean",
                        help="Path to silver layer parquet")
    parser.add_argument("--output", default="data/gold",
                        help="Output path for gold layer tables")
    args = parser.parse_args()

    results = run(args.input, args.output)

    print("\n--- Silver to Gold Results ---")
    for key, val in results.items():
        print(f"  {key}: {val}")
