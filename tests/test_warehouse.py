"""
Tests for Redshift SQL generation.

Validates that the loader produces correct SQL structure
without requiring a live Redshift connection.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.warehouse.load import (
    build_copy_sql,
    build_dedup_insert_sql,
    build_chargeback_update_sql,
    build_refresh_views_sql,
    build_full_load_sql,
)


class TestCopySQL:

    def test_contains_s3_path(self):
        sql = build_copy_sql("s3://my-bucket/gold/data/", "arn:aws:iam::123:role/myrole")
        assert "s3://my-bucket/gold/data/" in sql

    def test_contains_iam_role(self):
        sql = build_copy_sql("s3://bucket/path/", "arn:aws:iam::123:role/test-role")
        assert "arn:aws:iam::123:role/test-role" in sql

    def test_uses_parquet_format(self):
        sql = build_copy_sql("s3://bucket/path/", "arn:aws:iam::123:role/r")
        assert "FORMAT AS PARQUET" in sql

    def test_targets_staging_table(self):
        sql = build_copy_sql("s3://bucket/path/", "arn:aws:iam::123:role/r")
        assert "fct_transactions_staging" in sql


class TestDedupSQL:

    def test_uses_left_join_pattern(self):
        sql = build_dedup_insert_sql()
        assert "LEFT JOIN" in sql

    def test_filters_on_null_key(self):
        sql = build_dedup_insert_sql()
        assert "WHERE t.transaction_id IS NULL" in sql

    def test_inserts_into_target(self):
        sql = build_dedup_insert_sql()
        assert "INSERT INTO analytics.fct_transactions" in sql

    def test_custom_key_column(self):
        sql = build_dedup_insert_sql(key_column="chargeback_id")
        assert "chargeback_id IS NULL" in sql


class TestChargebackUpdate:

    def test_sets_chargeback_flag(self):
        sql = build_chargeback_update_sql()
        assert "chargeback_flag = TRUE" in sql

    def test_only_updates_unflagged(self):
        sql = build_chargeback_update_sql()
        assert "chargeback_flag = FALSE" in sql


class TestRefreshViews:

    def test_refreshes_all_three_views(self):
        sql = build_refresh_views_sql()
        assert "mv_fraud_overview" in sql
        assert "mv_chargeback_trends" in sql
        assert "mv_daily_kpis" in sql


class TestFullLoad:

    def test_wrapped_in_transaction(self):
        sql = build_full_load_sql("s3://b/p/", "arn:aws:iam::1:role/r", "2024-01-15")
        assert "BEGIN TRANSACTION" in sql
        assert "END TRANSACTION" in sql

    def test_truncates_staging(self):
        sql = build_full_load_sql("s3://b/p/", "arn:aws:iam::1:role/r", "2024-01-15")
        assert "TRUNCATE analytics.fct_transactions_staging" in sql

    def test_contains_all_steps(self):
        sql = build_full_load_sql("s3://b/p/", "arn:aws:iam::1:role/r", "2024-01-15")
        assert "COPY" in sql
        assert "INSERT INTO" in sql
        assert "UPDATE" in sql
        assert "TRUNCATE" in sql
        assert "REFRESH MATERIALIZED VIEW" in sql
