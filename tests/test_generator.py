"""
Tests for the synthetic data generator.

Validates that generated data has the expected statistical
properties rather than just checking that it runs without errors.
"""

import sys
from pathlib import Path

# Add project root to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.generate_transactions import (
    generate_merchants,
    generate_card_tokens,
    generate_transactions,
    assign_risk_tier,
)


class TestMerchantGeneration:

    def test_generates_correct_count(self):
        merchants = generate_merchants(50)
        assert len(merchants) == 50

    def test_merchant_has_required_fields(self):
        merchants = generate_merchants(1)
        m = merchants[0]
        required = ["merchant_id", "merchant_name", "category_code",
                     "category_name", "country", "base_fraud_rate", "risk_tier"]
        for field in required:
            assert field in m, f"Missing field: {field}"

    def test_merchant_id_format(self):
        merchants = generate_merchants(10)
        for m in merchants:
            assert m["merchant_id"].startswith("MRC")
            assert len(m["merchant_id"]) == 8

    def test_risk_tier_assignment(self):
        assert assign_risk_tier(0.08) == "critical"
        assert assign_risk_tier(0.05) == "high"
        assert assign_risk_tier(0.03) == "medium"
        assert assign_risk_tier(0.01) == "low"


class TestCardTokenGeneration:

    def test_generates_correct_count(self):
        tokens = generate_card_tokens(100)
        assert len(tokens) == 100

    def test_token_format(self):
        tokens = generate_card_tokens(10)
        for token in tokens:
            assert token.startswith("tok_")
            parts = token.split("_")
            assert len(parts) == 3
            assert len(parts[1]) == 6  # BIN prefix


class TestTransactionGeneration:

    def test_generates_correct_count(self):
        merchants = generate_merchants(10)
        tokens = generate_card_tokens(100)
        txns = generate_transactions(500, merchants, tokens, days_span=7)
        assert len(txns) == 500

    def test_transaction_has_required_fields(self):
        merchants = generate_merchants(10)
        tokens = generate_card_tokens(100)
        txns = generate_transactions(10, merchants, tokens)
        required = [
            "transaction_id", "transaction_ts", "merchant_id",
            "card_token", "amount_usd", "currency_code",
            "transaction_type", "channel", "country_code",
            "fraud_score", "fraud_decision", "is_fraudulent",
            "processing_latency_ms",
        ]
        for field in required:
            assert field in txns[0], f"Missing field: {field}"

    def test_amount_distribution_is_reasonable(self):
        """Median should be roughly $20-60 with log-normal distribution."""
        merchants = generate_merchants(50)
        tokens = generate_card_tokens(1000)
        txns = generate_transactions(5000, merchants, tokens)
        amounts = sorted([t["amount_usd"] for t in txns])
        median = amounts[len(amounts) // 2]
        assert 10 < median < 100, f"Median amount {median} outside expected range"

    def test_fraud_rate_within_range(self):
        """Fraud rate should be between 1% and 10% given the multipliers."""
        merchants = generate_merchants(50)
        tokens = generate_card_tokens(1000)
        txns = generate_transactions(10000, merchants, tokens, fraud_rate=0.023)
        fraud_count = sum(1 for t in txns if t["is_fraudulent"])
        fraud_rate = fraud_count / len(txns)
        assert 0.01 < fraud_rate < 0.10, f"Fraud rate {fraud_rate:.3f} outside expected range"

    def test_fraud_score_distribution(self):
        """Fraudulent txns should have higher scores than legitimate ones."""
        merchants = generate_merchants(50)
        tokens = generate_card_tokens(1000)
        txns = generate_transactions(5000, merchants, tokens)

        fraud_scores = [t["fraud_score"] for t in txns if t["is_fraudulent"]]
        legit_scores = [t["fraud_score"] for t in txns if not t["is_fraudulent"]]

        if fraud_scores and legit_scores:
            avg_fraud = sum(fraud_scores) / len(fraud_scores)
            avg_legit = sum(legit_scores) / len(legit_scores)
            assert avg_fraud > avg_legit, (
                f"Avg fraud score ({avg_fraud:.3f}) should exceed "
                f"avg legit score ({avg_legit:.3f})"
            )

    def test_decisions_are_valid(self):
        merchants = generate_merchants(10)
        tokens = generate_card_tokens(100)
        txns = generate_transactions(1000, merchants, tokens)
        valid_decisions = {"approved", "held", "declined"}
        for t in txns:
            assert t["fraud_decision"] in valid_decisions

    def test_channels_are_valid(self):
        merchants = generate_merchants(10)
        tokens = generate_card_tokens(100)
        txns = generate_transactions(1000, merchants, tokens)
        valid_channels = {"online", "pos", "mobile", "phone"}
        for t in txns:
            assert t["channel"] in valid_channels
