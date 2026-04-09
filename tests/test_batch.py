"""
Tests for batch ETL transformations.

Uses small in-memory DataFrames to validate transformation logic
without reading from disk or requiring generated data files.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.processing.batch.bronze_to_silver import (
    deduplicate, validate_required_fields, cast_and_clean
)
from src.processing.batch.silver_to_gold import (
    build_merchant_risk_profiles, build_kpi_daily
)


@pytest.fixture(scope="module")
def spark():
    """Shared Spark session for all tests. Created once, reused."""
    session = (
        SparkSession.builder
        .appName("test_batch")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestDeduplication:

    def test_removes_exact_duplicates(self, spark):
        data = [
            ("txn_001", "2024-01-01T10:00:00", "MRC00001", "tok_123", 50.0),
            ("txn_001", "2024-01-01T10:00:00", "MRC00001", "tok_123", 50.0),
            ("txn_002", "2024-01-01T11:00:00", "MRC00002", "tok_456", 75.0),
        ]
        cols = ["transaction_id", "transaction_ts", "merchant_id", "card_token", "amount_usd"]
        df = spark.createDataFrame(data, cols)

        result = deduplicate(df)
        assert result.count() == 2

    def test_keeps_unique_records(self, spark):
        data = [
            ("txn_001", "2024-01-01T10:00:00", "MRC00001", "tok_123", 50.0),
            ("txn_002", "2024-01-01T11:00:00", "MRC00002", "tok_456", 75.0),
            ("txn_003", "2024-01-01T12:00:00", "MRC00003", "tok_789", 100.0),
        ]
        cols = ["transaction_id", "transaction_ts", "merchant_id", "card_token", "amount_usd"]
        df = spark.createDataFrame(data, cols)

        result = deduplicate(df)
        assert result.count() == 3


class TestValidation:

    def test_rejects_null_transaction_id(self, spark):
        data = [
            (None, "2024-01-01T10:00:00", "MRC00001", "tok_123", 50.0),
            ("txn_002", "2024-01-01T11:00:00", "MRC00002", "tok_456", 75.0),
        ]
        cols = ["transaction_id", "transaction_ts", "merchant_id", "card_token", "amount_usd"]
        df = spark.createDataFrame(data, cols)

        valid, quarantined = validate_required_fields(df)
        assert valid.count() == 1
        assert quarantined.count() == 1

    def test_rejects_null_amount(self, spark):
        data = [
            ("txn_001", "2024-01-01T10:00:00", "MRC00001", "tok_123", None),
            ("txn_002", "2024-01-01T11:00:00", "MRC00002", "tok_456", 75.0),
        ]
        cols = ["transaction_id", "transaction_ts", "merchant_id", "card_token", "amount_usd"]
        df = spark.createDataFrame(data, cols)

        valid, quarantined = validate_required_fields(df)
        assert valid.count() == 1
        assert quarantined.count() == 1

    def test_passes_complete_records(self, spark):
        data = [
            ("txn_001", "2024-01-01T10:00:00", "MRC00001", "tok_123", 50.0),
            ("txn_002", "2024-01-01T11:00:00", "MRC00002", "tok_456", 75.0),
        ]
        cols = ["transaction_id", "transaction_ts", "merchant_id", "card_token", "amount_usd"]
        df = spark.createDataFrame(data, cols)

        valid, quarantined = validate_required_fields(df)
        assert valid.count() == 2
        assert quarantined.count() == 0


class TestCastAndClean:

    def _make_df(self, spark, overrides=None):
        """Build a minimal valid transaction row with explicit schema."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType,
            BooleanType, IntegerType
        )
        defaults = {
            "transaction_id": "txn_001",
            "transaction_ts": "2024-01-15T14:30:00",
            "merchant_id": "MRC00001",
            "card_token": "tok_123",
            "amount_usd": 50.0,
            "currency_code": None,
            "transaction_type": "purchase",
            "channel": " ONLINE ",
            "country_code": "US",
            "fraud_score": 0.15,
            "fraud_decision": "approved",
            "is_fraudulent": False,
            "processing_latency_ms": None,
        }
        if overrides:
            defaults.update(overrides)

        schema = StructType([
            StructField("transaction_id", StringType()),
            StructField("transaction_ts", StringType()),
            StructField("merchant_id", StringType()),
            StructField("card_token", StringType()),
            StructField("amount_usd", DoubleType()),
            StructField("currency_code", StringType()),
            StructField("transaction_type", StringType()),
            StructField("channel", StringType()),
            StructField("country_code", StringType()),
            StructField("fraud_score", DoubleType()),
            StructField("fraud_decision", StringType()),
            StructField("is_fraudulent", BooleanType()),
            StructField("processing_latency_ms", IntegerType()),
        ])
        return spark.createDataFrame([tuple(defaults.values())], schema)

    def test_normalizes_channel_lowercase(self, spark):
        df = self._make_df(spark, {"channel": " ONLINE "})
        result = cast_and_clean(df)
        channel = result.collect()[0]["channel"]
        assert channel == "online"

    def test_fills_null_currency_with_usd(self, spark):
        df = self._make_df(spark, {"currency_code": None})
        result = cast_and_clean(df)
        currency = result.collect()[0]["currency_code"]
        assert currency == "USD"

    def test_caps_negative_amount_at_zero(self, spark):
        df = self._make_df(spark, {"amount_usd": -25.0})
        result = cast_and_clean(df)
        amount = result.collect()[0]["amount_usd"]
        assert amount == 0.0

    def test_adds_transaction_date_column(self, spark):
        df = self._make_df(spark)
        result = cast_and_clean(df)
        assert "transaction_date" in result.columns

    def test_adds_ingestion_timestamp(self, spark):
        df = self._make_df(spark)
        result = cast_and_clean(df)
        assert "ingestion_ts" in result.columns


class TestGoldAggregations:

    def _make_silver_df(self, spark):
        """Small silver-like dataset for testing aggregations."""
        data = [
            ("txn_001", "2024-01-15", "MRC00001", "tok_001", 50.0, "online", 0.15, "approved", False, 200),
            ("txn_002", "2024-01-15", "MRC00001", "tok_002", 200.0, "online", 0.85, "declined", True, 350),
            ("txn_003", "2024-01-15", "MRC00002", "tok_003", 30.0, "pos", 0.05, "approved", False, 150),
            ("txn_004", "2024-01-16", "MRC00001", "tok_001", 75.0, "mobile", 0.60, "held", True, 500),
            ("txn_005", "2024-01-16", "MRC00002", "tok_004", 120.0, "online", 0.10, "approved", False, 180),
        ]
        cols = ["transaction_id", "transaction_date", "merchant_id", "card_token",
                "amount_usd", "channel", "fraud_score", "fraud_decision",
                "is_fraudulent", "processing_latency_ms"]
        df = spark.createDataFrame(data, cols)
        # cast transaction_date from string to date type
        return df.withColumn("transaction_date", F.to_date("transaction_date"))

    def test_merchant_profiles_one_row_per_merchant(self, spark):
        df = self._make_silver_df(spark)
        profiles = build_merchant_risk_profiles(df)
        assert profiles.count() == 2  # MRC00001 and MRC00002

    def test_merchant_profiles_correct_fraud_count(self, spark):
        df = self._make_silver_df(spark)
        profiles = build_merchant_risk_profiles(df)
        mrc1 = profiles.filter(F.col("merchant_id") == "MRC00001").collect()[0]
        # MRC00001 has txn_002 (fraud) and txn_004 (fraud) = 2 fraudulent
        assert mrc1["fraud_count"] == 2

    def test_kpi_daily_one_row_per_day(self, spark):
        df = self._make_silver_df(spark)
        kpis = build_kpi_daily(df)
        assert kpis.count() == 2  # Jan 15 and Jan 16

    def test_kpi_daily_detection_rate_bounded(self, spark):
        df = self._make_silver_df(spark)
        kpis = build_kpi_daily(df)
        rates = [row["fraud_detection_rate"] for row in kpis.collect()]
        for rate in rates:
            assert 0.0 <= rate <= 1.0
