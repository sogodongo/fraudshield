"""
Lambda fraud scoring function.

Receives batches of transaction records from Kinesis, scores each one,
and writes decisions to DynamoDB. This runs on every transaction in
near-real-time — latency budget is under 1 second per record.

The scoring logic is a simplified weighted-rules model. Production
would swap this for a deserialized XGBoost/LightGBM model loaded
from S3 on cold start and cached in /tmp across warm invocations.
"""

import os
import json
import base64
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialized once per container, reused across invocations
dynamodb = boto3.resource("dynamodb")
decisions_table = None
merchant_table = None


def get_tables():
    """
    Lazy-load table references. Doing this outside the handler
    means we only pay the setup cost on cold starts.
    """
    global decisions_table, merchant_table
    if decisions_table is None:
        decisions_table = dynamodb.Table(
            os.environ.get("DECISIONS_TABLE", "fraudshield-dev-fraud-decisions")
        )
    if merchant_table is None:
        merchant_table = dynamodb.Table(
            os.environ.get("MERCHANT_TABLE", "fraudshield-dev-merchant-profiles")
        )
    return decisions_table, merchant_table


def get_merchant_risk(merchant_id):
    """
    Look up merchant risk profile from DynamoDB.
    Returns risk_tier and historical fraud rate.
    Falls back to medium risk if merchant not found — this happens
    for new merchants that have not been loaded into the profile table yet.
    """
    _, merchants = get_tables()
    try:
        response = merchants.get_item(Key={"merchant_id": merchant_id})
        if "Item" in response:
            return response["Item"]
    except Exception as e:
        logger.warning(f"Merchant lookup failed for {merchant_id}: {e}")

    # sensible default for unknown merchants
    return {"risk_tier": "medium", "historical_fraud_rate": 0.03}


def compute_fraud_score(transaction, merchant_profile):
    """
    Score a transaction for fraud likelihood. Returns 0.0 to 1.0.

    This is the part you would replace with a real ML model. The
    weighted-rules approach here captures the same directional signals
    that a trained model would learn from data:
      - high-risk merchant categories score higher
      - online/phone channels score higher than POS
      - unusually large amounts score higher
      - known high-risk merchants score higher

    Weights were set by hand to produce a reasonable score distribution
    that matches the test data generator output. They are not calibrated
    to any real dataset.
    """
    score = 0.0
    amount = float(transaction.get("amount_usd", 0))
    channel = transaction.get("channel", "unknown")
    risk_tier = merchant_profile.get("risk_tier", "medium")

    # merchant risk contributes 0-0.35
    tier_weights = {"low": 0.05, "medium": 0.15, "high": 0.25, "critical": 0.35}
    score += tier_weights.get(risk_tier, 0.15)

    # channel risk contributes 0-0.25
    channel_weights = {"pos": 0.05, "mobile": 0.08, "online": 0.18, "phone": 0.25}
    score += channel_weights.get(channel, 0.10)

    # amount risk — escalating tiers
    if amount > 2000:
        score += 0.20
    elif amount > 500:
        score += 0.12
    elif amount > 200:
        score += 0.05

    # historical merchant fraud rate (if available)
    hist_rate = float(merchant_profile.get("historical_fraud_rate", 0.03))
    score += min(hist_rate * 3, 0.20)  # cap contribution at 0.20

    # clamp to valid range
    return round(min(max(score, 0.0), 1.0), 4)


def make_decision(score):
    """
    Turn a numeric score into an actionable decision.

    Thresholds set conservatively — we would rather hold a suspicious
    transaction for manual review than auto-decline a legitimate one.
    False declines cost us customers. False approvals cost us chargebacks.
    The held bucket lets analysts make the final call.
    """
    if score >= 0.75:
        return "declined"
    elif score >= 0.45:
        return "held"
    return "approved"


def write_decision(transaction_id, score, decision, transaction):
    """Persist the scoring decision to DynamoDB for downstream consumption."""
    decisions, _ = get_tables()
    try:
        item = {
            "transaction_id": transaction_id,
            "fraud_score": Decimal(str(score)),
            "decision": decision,
            "merchant_id": transaction.get("merchant_id", ""),
            "amount_usd": Decimal(str(transaction.get("amount_usd", 0))),
            "channel": transaction.get("channel", ""),
            "scored_at": datetime.utcnow().isoformat(),
            # auto-expire after 90 days
            "expires_at": int((datetime.utcnow() + timedelta(days=90)).timestamp()),
        }
        decisions.put_item(Item=item)
    except Exception as e:
        # log but do not raise — a failed write should not block scoring
        logger.error(f"Failed to write decision for {transaction_id}: {e}")


def process_record(raw_data):
    """
    Process a single Kinesis record end-to-end.
    Returns the decision dict for logging/metrics.
    """
    start = time.time()

    transaction = json.loads(raw_data)
    txn_id = transaction.get("transaction_id", "unknown")

    merchant_profile = get_merchant_risk(transaction.get("merchant_id", ""))
    score = compute_fraud_score(transaction, merchant_profile)
    decision = make_decision(score)

    write_decision(txn_id, score, decision, transaction)

    latency_ms = round((time.time() - start) * 1000, 1)
    logger.info(
        f"txn={txn_id} score={score} decision={decision} "
        f"amount={transaction.get('amount_usd')} latency={latency_ms}ms"
    )

    return {
        "transaction_id": txn_id,
        "score": score,
        "decision": decision,
        "latency_ms": latency_ms,
    }


def handler(event, context):
    """
    Lambda entry point. Receives a batch of Kinesis records.

    Kinesis-Lambda integration delivers records in batches (up to 10,000
    or 6MB, whichever is reached first). We process each record individually.
    Failed records are logged but do not fail the batch — the DLQ
    catches persistent failures via the event source mapping config.
    """
    records = event.get("Records", [])
    logger.info(f"Received batch of {len(records)} records")

    results = {"processed": 0, "errors": 0}

    for record in records:
        try:
            # Kinesis records are base64-encoded
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            process_record(payload)
            results["processed"] += 1

        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in record: {e}")
            results["errors"] += 1

        except Exception as e:
            logger.error(f"Unexpected error processing record: {e}")
            results["errors"] += 1

    logger.info(f"Batch complete: {results['processed']} processed, {results['errors']} errors")
    return results
