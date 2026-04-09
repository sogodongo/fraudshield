"""
Tests for the Lambda fraud scoring function.

Tests the pure scoring logic only — no DynamoDB, no Lambda handler.
Those are integration concerns tested separately.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.processing.streaming.scorer import compute_fraud_score, make_decision


class TestMakeDecision:

    def test_high_score_declined(self):
        assert make_decision(0.90) == "declined"
        assert make_decision(0.75) == "declined"

    def test_medium_score_held(self):
        assert make_decision(0.60) == "held"
        assert make_decision(0.45) == "held"

    def test_low_score_approved(self):
        assert make_decision(0.20) == "approved"
        assert make_decision(0.0) == "approved"

    def test_boundary_at_045(self):
        """0.45 is held, 0.4499 is approved."""
        assert make_decision(0.45) == "held"
        assert make_decision(0.44) == "approved"

    def test_boundary_at_075(self):
        """0.75 is declined, 0.7499 is held."""
        assert make_decision(0.75) == "declined"
        assert make_decision(0.74) == "held"


class TestComputeFraudScore:

    def _low_risk_profile(self):
        return {"risk_tier": "low", "historical_fraud_rate": 0.01}

    def _high_risk_profile(self):
        return {"risk_tier": "critical", "historical_fraud_rate": 0.08}

    def test_score_between_zero_and_one(self):
        """Score must always be in valid range regardless of inputs."""
        txn = {"amount_usd": 50, "channel": "pos"}
        score = compute_fraud_score(txn, self._low_risk_profile())
        assert 0.0 <= score <= 1.0

    def test_low_risk_scores_low(self):
        txn = {"amount_usd": 20, "channel": "pos"}
        score = compute_fraud_score(txn, self._low_risk_profile())
        assert score < 0.30, f"Low risk scenario scored {score}, expected under 0.30"

    def test_high_risk_scores_high(self):
        txn = {"amount_usd": 3000, "channel": "phone"}
        score = compute_fraud_score(txn, self._high_risk_profile())
        assert score > 0.60, f"High risk scenario scored {score}, expected above 0.60"

    def test_online_riskier_than_pos(self):
        """Same transaction, different channel — online should score higher."""
        profile = {"risk_tier": "medium", "historical_fraud_rate": 0.03}
        pos_score = compute_fraud_score({"amount_usd": 100, "channel": "pos"}, profile)
        online_score = compute_fraud_score({"amount_usd": 100, "channel": "online"}, profile)
        assert online_score > pos_score

    def test_large_amount_riskier(self):
        """Same merchant and channel, bigger amount should score higher."""
        profile = {"risk_tier": "medium", "historical_fraud_rate": 0.03}
        small = compute_fraud_score({"amount_usd": 30, "channel": "online"}, profile)
        large = compute_fraud_score({"amount_usd": 3000, "channel": "online"}, profile)
        assert large > small

    def test_critical_merchant_riskier_than_low(self):
        """Same transaction through different merchant tiers."""
        txn = {"amount_usd": 100, "channel": "online"}
        low_score = compute_fraud_score(txn, self._low_risk_profile())
        high_score = compute_fraud_score(txn, self._high_risk_profile())
        assert high_score > low_score

    def test_unknown_channel_gets_default(self):
        """Unknown channels should not crash, just get a default weight."""
        txn = {"amount_usd": 50, "channel": "carrier_pigeon"}
        profile = {"risk_tier": "medium", "historical_fraud_rate": 0.03}
        score = compute_fraud_score(txn, profile)
        assert 0.0 <= score <= 1.0

    def test_missing_amount_defaults_to_zero(self):
        """Missing amount should not crash."""
        txn = {"channel": "pos"}
        score = compute_fraud_score(txn, self._low_risk_profile())
        assert 0.0 <= score <= 1.0

    def test_extreme_amount_still_capped(self):
        """Even with all risk factors maxed, score should not exceed 1.0."""
        txn = {"amount_usd": 99999, "channel": "phone"}
        score = compute_fraud_score(txn, self._high_risk_profile())
        assert score <= 1.0
