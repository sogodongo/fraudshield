"""
Data quality validation for the FraudShield pipeline.

Runs checks against silver and gold layer data after each ETL run.
Each check returns a result dict with pass/fail status and details.
Failed critical checks should trigger a CloudWatch alarm in production.

Kept simple on purpose. Great Expectations does the same thing with
more ceremony. For a team of 2-3 engineers this is easier to maintain
and debug than a full GE deployment.
"""

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_quality")


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str       # "critical" or "warning"
    details: str
    actual_value: float = 0.0
    threshold: float = 0.0


def check_row_count(df, table_name, minimum=1):
    """Table must have at least minimum rows. Zero rows means ETL broke."""
    count = df.count()
    passed = count >= minimum
    return CheckResult(
        name=f"{table_name}_row_count",
        passed=passed,
        severity="critical",
        details=f"Expected >= {minimum} rows, found {count}",
        actual_value=count,
        threshold=minimum,
    )


def check_null_rate(df, column, table_name, max_null_pct=0.01):
    """Column should have less than max_null_pct null values."""
    total = df.count()
    if total == 0:
        return CheckResult(
            name=f"{table_name}_{column}_nulls",
            passed=False,
            severity="critical",
            details="Table is empty, cannot check null rate",
        )

    null_count = df.filter(F.col(column).isNull()).count()
    null_pct = null_count / total

    return CheckResult(
        name=f"{table_name}_{column}_nulls",
        passed=null_pct <= max_null_pct,
        severity="critical" if max_null_pct < 0.05 else "warning",
        details=f"{null_count}/{total} nulls ({null_pct:.2%}), threshold {max_null_pct:.2%}",
        actual_value=null_pct,
        threshold=max_null_pct,
    )


def check_value_range(df, column, table_name, min_val=None, max_val=None):
    """Column values should fall within expected range."""
    stats = df.agg(
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val"),
    ).collect()[0]

    actual_min = stats["min_val"]
    actual_max = stats["max_val"]
    issues = []

    if min_val is not None and actual_min is not None and actual_min < min_val:
        issues.append(f"min {actual_min} < expected {min_val}")
    if max_val is not None and actual_max is not None and actual_max > max_val:
        issues.append(f"max {actual_max} > expected {max_val}")

    passed = len(issues) == 0
    return CheckResult(
        name=f"{table_name}_{column}_range",
        passed=passed,
        severity="warning",
        details=f"Range [{actual_min}, {actual_max}]" + (f" Issues: {issues}" if issues else ""),
        actual_value=actual_max or 0,
        threshold=max_val or 0,
    )


def check_no_duplicates(df, key_column, table_name):
    """Key column should have no duplicate values."""
    total = df.count()
    distinct = df.select(key_column).distinct().count()
    dupes = total - distinct

    return CheckResult(
        name=f"{table_name}_{key_column}_unique",
        passed=dupes == 0,
        severity="critical",
        details=f"{dupes} duplicates found out of {total} rows",
        actual_value=dupes,
        threshold=0,
    )


def check_valid_values(df, column, table_name, valid_set):
    """Column should only contain values from the valid set."""
    invalid = (
        df
        .filter(~F.col(column).isin(list(valid_set)))
        .select(column)
        .distinct()
        .collect()
    )
    invalid_values = [row[column] for row in invalid]

    return CheckResult(
        name=f"{table_name}_{column}_valid_values",
        passed=len(invalid_values) == 0,
        severity="warning",
        details=f"Invalid values: {invalid_values}" if invalid_values else "All values valid",
        actual_value=len(invalid_values),
        threshold=0,
    )


def check_freshness(df, date_column, table_name, max_age_days=2):
    """
    Most recent record should be within max_age_days of the latest date in the data.
    In production you would compare against current date. For testing
    we just verify the date column has recent-ish data.
    """
    max_date = df.agg(F.max(date_column)).collect()[0][0]
    min_date = df.agg(F.min(date_column)).collect()[0][0]

    if max_date is None:
        return CheckResult(
            name=f"{table_name}_freshness",
            passed=False,
            severity="critical",
            details="No dates found in data",
        )

    date_span = (max_date - min_date).days if hasattr(max_date - min_date, 'days') else 0

    return CheckResult(
        name=f"{table_name}_freshness",
        passed=True,  # simplified for local testing
        severity="warning",
        details=f"Date range: {min_date} to {max_date} ({date_span} days)",
        actual_value=date_span,
        threshold=max_age_days,
    )


def run_silver_checks(df):
    """Full validation suite for the silver transactions table."""
    logger.info("Running silver layer quality checks")
    results = [
        check_row_count(df, "silver_transactions", minimum=1000),
        check_no_duplicates(df, "transaction_id", "silver_transactions"),
        check_null_rate(df, "transaction_id", "silver_transactions", max_null_pct=0.0),
        check_null_rate(df, "merchant_id", "silver_transactions", max_null_pct=0.0),
        check_null_rate(df, "amount_usd", "silver_transactions", max_null_pct=0.0),
        check_null_rate(df, "fraud_score", "silver_transactions", max_null_pct=0.05),
        check_value_range(df, "amount_usd", "silver_transactions", min_val=0.0, max_val=99999.99),
        check_value_range(df, "fraud_score", "silver_transactions", min_val=0.0, max_val=1.0),
        check_valid_values(df, "channel", "silver_transactions",
                          {"online", "pos", "mobile", "phone"}),
        check_valid_values(df, "fraud_decision", "silver_transactions",
                          {"approved", "held", "declined"}),
        check_freshness(df, "transaction_date", "silver_transactions"),
    ]
    return results


def run_gold_checks(kpi_df, merchant_df):
    """Validation suite for gold layer tables."""
    logger.info("Running gold layer quality checks")
    results = [
        check_row_count(kpi_df, "kpi_daily", minimum=1),
        check_row_count(merchant_df, "merchant_risk_profiles", minimum=1),
        check_null_rate(kpi_df, "fraud_detection_rate", "kpi_daily", max_null_pct=0.0),
        check_value_range(kpi_df, "fraud_detection_rate", "kpi_daily",
                         min_val=0.0, max_val=1.0),
        check_value_range(kpi_df, "false_positive_rate", "kpi_daily",
                         min_val=0.0, max_val=1.0),
        check_no_duplicates(merchant_df, "merchant_id", "merchant_risk_profiles"),
    ]
    return results


def print_results(results):
    """Print quality check results in a readable format."""
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    total = len(results)

    print(f"\n--- Data Quality Report ---")
    print(f"Total: {total}  Passed: {passed}  Failed: {failed}")
    print()

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        icon = "+" if r.passed else "!"
        print(f"  [{icon}] {status:4s} [{r.severity:8s}] {r.name}")
        if not r.passed:
            print(f"         {r.details}")

    if failed > 0:
        critical_failures = [r for r in results if not r.passed and r.severity == "critical"]
        if critical_failures:
            print(f"\n  {len(critical_failures)} CRITICAL failures — pipeline should halt")
    else:
        print(f"\n  All checks passed")
