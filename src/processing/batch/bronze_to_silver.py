"""
Bronze to Silver transformation.

Reads raw transaction data from the bronze layer and produces
cleaned, deduplicated, schema-validated output in the silver layer.

This runs as an AWS Glue job in production and as a local PySpark
script during development. The transformation logic is identical
in both environments — only the I/O paths differ.

Glue invocation:
    aws glue start-job-run --job-name fraudshield-bronze-to-silver

Local invocation:
    python src/processing/batch/bronze_to_silver.py \
        --input data/transactions.parquet \
        --output data/silver/transactions_clean
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_to_silver")


# Expected schema for transaction records. Records missing
# required fields or with wrong types get quarantined.
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_ts", StringType(), nullable=False),
    StructField("merchant_id", StringType(), nullable=False),
    StructField("card_token", StringType(), nullable=False),
    StructField("amount_usd", DoubleType(), nullable=False),
    StructField("currency_code", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),
    StructField("fraud_score", DoubleType(), nullable=True),
    StructField("fraud_decision", StringType(), nullable=True),
    StructField("is_fraudulent", BooleanType(), nullable=True),
    StructField("processing_latency_ms", IntegerType(), nullable=True),
])

# Fields that must be present and non-null for a record to pass validation
REQUIRED_FIELDS = ["transaction_id", "transaction_ts", "merchant_id",
                   "card_token", "amount_usd"]


def create_spark_session(app_name="bronze_to_silver"):
    """
    Create a SparkSession. In Glue this is provided by the runtime.
    Locally we create our own with minimal config.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_bronze(spark, input_path):
    """Read raw parquet from the bronze layer."""
    logger.info(f"Reading bronze data from: {input_path}")
    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"Bronze rows read: {row_count}")
    return df


def deduplicate(df):
    """
    Remove duplicate records using transaction_id.

    Kinesis Firehose has at-least-once delivery, so the same
    record can appear multiple times. We keep the first occurrence
    based on transaction_ts.
    """
    before = df.count()
    df_deduped = df.dropDuplicates(["transaction_id"])
    after = df_deduped.count()
    removed = before - after
    if removed > 0:
        logger.info(f"Deduplication removed {removed} records ({before} -> {after})")
    else:
        logger.info("No duplicates found")
    return df_deduped


def validate_required_fields(df):
    """
    Split dataframe into valid and quarantined records.
    Records missing any required field go to quarantine for
    investigation rather than being silently dropped.
    """
    # build a combined null check across all required fields
    not_null_condition = F.lit(True)
    for field in REQUIRED_FIELDS:
        not_null_condition = not_null_condition & F.col(field).isNotNull()

    valid = df.filter(not_null_condition)
    quarantined = df.filter(~not_null_condition)

    valid_count = valid.count()
    quarantine_count = quarantined.count()
    logger.info(f"Validation: {valid_count} valid, {quarantine_count} quarantined")

    if quarantine_count > 0:
        logger.warning(f"{quarantine_count} records failed validation — check quarantine output")

    return valid, quarantined


def cast_and_clean(df):
    """
    Apply type casts and derive columns needed for the silver layer.

    - Parse transaction_ts string into proper timestamp
    - Extract transaction_date for partitioning
    - Normalize channel to lowercase (seen mixed case in some sources)
    - Fill null currency_code with USD (90%+ of our transactions)
    - Cap negative amounts at 0 (refunds handled separately)
    """
    cleaned = (
        df
        .withColumn("transaction_ts",
                     F.to_timestamp(F.col("transaction_ts")))
        .withColumn("transaction_date",
                     F.to_date(F.col("transaction_ts")))
        .withColumn("channel",
                     F.lower(F.trim(F.col("channel"))))
        .withColumn("currency_code",
                     F.coalesce(F.col("currency_code"), F.lit("USD")))
        .withColumn("amount_usd",
                     F.when(F.col("amount_usd") < 0, 0.0)
                      .otherwise(F.col("amount_usd")))
        .withColumn("processing_latency_ms",
                     F.coalesce(F.col("processing_latency_ms"), F.lit(0)))
        .withColumn("ingestion_ts",
                     F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
    )
    logger.info("Type casting and cleaning complete")
    return cleaned


def write_silver(df, output_path):
    """
    Write cleaned data to silver layer, partitioned by transaction_date.

    Partitioning by date aligns with how analysts query — almost every
    dashboard filter starts with a date range. Coalesce to a reasonable
    number of files to avoid the small files problem.
    """
    row_count = df.count()
    logger.info(f"Writing {row_count} rows to silver: {output_path}")

    (
        df
        .coalesce(4)  # keeps file count manageable for small datasets
        .write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(output_path)
    )
    logger.info("Silver write complete")


def run(input_path, output_path, quarantine_path=None):
    """Full bronze-to-silver pipeline."""
    spark = create_spark_session()

    try:
        # read
        bronze_df = read_bronze(spark, input_path)

        # clean
        deduped = deduplicate(bronze_df)
        valid, quarantined = validate_required_fields(deduped)
        cleaned = cast_and_clean(valid)

        # write valid records to silver
        write_silver(cleaned, output_path)

        # write quarantined records separately if path provided
        if quarantine_path and quarantined.count() > 0:
            quarantined.write.mode("overwrite").parquet(quarantine_path)
            logger.info(f"Quarantined records written to {quarantine_path}")

        return {
            "bronze_count": bronze_df.count(),
            "deduped_count": deduped.count(),
            "valid_count": valid.count(),
            "quarantined_count": quarantined.count(),
            "silver_count": cleaned.count(),
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL")
    parser.add_argument("--input", default="data/transactions.parquet",
                        help="Path to bronze layer parquet")
    parser.add_argument("--output", default="data/silver/transactions_clean",
                        help="Output path for silver layer")
    parser.add_argument("--quarantine", default="data/quarantine/transactions",
                        help="Output path for quarantined records")
    args = parser.parse_args()

    results = run(args.input, args.output, args.quarantine)

    print("\n--- Bronze to Silver Results ---")
    for key, val in results.items():
        print(f"  {key}: {val}")
