"""
Synthetic payment transaction generator with realistic fraud patterns.

Produces Parquet files with statistical properties matching real payment data:
- Log-normal amount distribution (most txns small, long tail of large ones)
- Diurnal traffic pattern (peak at midday and evening, low overnight)
- Fraud rates varying by merchant category, channel, and amount
- Tokenized card numbers (never raw PANs)

Usage:
    python scripts/generate_transactions.py --count 100000 --output data/transactions.parquet
    python scripts/generate_transactions.py --count 5000 --fraud-rate 0.05 --output data/high_fraud.parquet
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


# MCC code, category name, baseline fraud rate for that category
# These rates are roughly calibrated to real-world ranges.
# Electronics and gambling have high fraud. Grocery and medical are low.
MERCHANT_CATEGORIES = [
    ("5411", "Grocery Stores", 0.01),
    ("5812", "Restaurants", 0.015),
    ("5311", "Department Stores", 0.025),
    ("5732", "Electronics", 0.06),
    ("5944", "Jewelry Stores", 0.045),
    ("5999", "Misc Retail", 0.03),
    ("4816", "Online Services", 0.05),
    ("7995", "Gambling", 0.08),
    ("5047", "Medical Supplies", 0.02),
    ("6012", "Financial Institutions", 0.035),
]

CHANNELS = ["online", "pos", "mobile", "phone"]
CHANNEL_WEIGHTS = [0.45, 0.30, 0.20, 0.05]

COUNTRIES = ["US", "GB", "CA", "DE", "FR", "BR", "NG", "IN", "JP", "AU"]
COUNTRY_WEIGHTS = [0.50, 0.10, 0.08, 0.06, 0.05, 0.05, 0.04, 0.05, 0.04, 0.03]

CARD_BINS = [
    "411111", "424242", "531234", "601100",
    "371449", "350000", "621234", "400000",
]


def assign_risk_tier(fraud_rate):
    """Map a numeric fraud rate to a human-readable tier."""
    if fraud_rate >= 0.06:
        return "critical"
    elif fraud_rate >= 0.04:
        return "high"
    elif fraud_rate >= 0.02:
        return "medium"
    return "low"


def generate_merchants(n=200):
    """Create a pool of merchants with assigned categories and risk tiers."""
    merchants = []
    for i in range(n):
        cat_code, cat_name, base_rate = random.choice(MERCHANT_CATEGORIES)
        merchants.append({
            "merchant_id": f"MRC{i:05d}",
            "merchant_name": f"Merchant_{i}",
            "category_code": cat_code,
            "category_name": cat_name,
            "country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
            "base_fraud_rate": base_rate,
            "risk_tier": assign_risk_tier(base_rate),
        })
    return merchants


def generate_card_tokens(n=50000):
    """
    Pre-generate a pool of tokenized card numbers.
    In production, tokenization happens at the payment gateway.
    Here we simulate it with a BIN prefix + random hash.
    """
    tokens = []
    for _ in range(n):
        bin_prefix = random.choice(CARD_BINS)
        token = f"tok_{bin_prefix}_{uuid.uuid4().hex[:12]}"
        tokens.append(token)
    return tokens


def weighted_hour(rng):
    """
    Pick an hour of day weighted by realistic traffic volume.
    Two peaks: late morning (10-12) and evening (18-20).
    Very low traffic overnight (0-5).
    """
    weights = [
        1, 1, 1, 1, 2, 3,      # 00-05: overnight, minimal
        5, 7, 8, 10, 12, 12,   # 06-11: morning ramp-up
        11, 10, 9, 8, 8, 9,    # 12-17: afternoon plateau
        11, 12, 10, 7, 4, 2,   # 18-23: evening peak then drop
    ]
    weights = np.array(weights, dtype=float)
    weights /= weights.sum()
    return int(rng.choice(24, p=weights))


def determine_fraud(merchant, channel, amount, base_rate, rng):
    """
    Fraud probability depends on merchant category, channel, and amount.

    This is a simplified simulation. Real fraud detection considers
    velocity patterns, geo anomalies, device fingerprinting, and
    behavioral biometrics. But the statistical shape is similar:
    fraud concentrates around specific risk factor combinations.
    """
    rate = base_rate

    # Merchant category adjustment
    rate *= (merchant["base_fraud_rate"] / 0.03)

    # Channel risk multiplier
    if channel == "online":
        rate *= 1.8
    elif channel == "phone":
        rate *= 2.5

    # Higher amounts are slightly more likely to be fraud
    if amount > 500:
        rate *= 1.5
    if amount > 2000:
        rate *= 2.0

    # Cap to prevent unrealistic rates
    rate = min(rate, 0.25)

    return bool(rng.random() < rate)


def generate_transactions(count, merchants, card_tokens, fraud_rate=0.023,
                          start_date=None, days_span=30):
    """
    Generate a list of transaction records.

    Each record contains all fields needed for the bronze layer
    of the data lake, matching the schema defined in DATA_MODEL.md.
    """
    if start_date is None:
        start_date = datetime(2024, 1, 1)

    rng = np.random.default_rng(42)
    records = []

    for _ in range(count):
        merchant = random.choice(merchants)
        card = random.choice(card_tokens)
        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

        # Timestamp with diurnal pattern
        day_offset = random.randint(0, days_span - 1)
        hour = weighted_hour(rng)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        ts = start_date + timedelta(
            days=day_offset, hours=hour, minutes=minute, seconds=second
        )

        # Log-normal amount distribution
        # mean=3.5 and sigma=1.2 produces a median around $33
        # with a long tail reaching into the thousands
        amount = round(float(rng.lognormal(3.5, 1.2)), 2)
        amount = min(amount, 9999.99)

        is_fraud = determine_fraud(merchant, channel, amount, fraud_rate, rng)

        # Fraud score: noisy approximation of the true label.
        # In production the model computes this. Here we simulate it
        # so the pipeline has realistic score distributions to work with.
        if is_fraud:
            score = min(1.0, round(float(rng.beta(5, 2)), 4))
        else:
            score = round(float(rng.beta(1.5, 8)), 4)

        if score > 0.8:
            decision = "declined"
        elif score > 0.5:
            decision = "held"
        else:
            decision = "approved"

        records.append({
            "transaction_id": str(uuid.uuid4()),
            "transaction_ts": ts.isoformat(),
            "merchant_id": merchant["merchant_id"],
            "card_token": card,
            "amount_usd": amount,
            "currency_code": "USD",
            "transaction_type": random.choices(
                ["purchase", "auth_only", "refund"],
                weights=[0.85, 0.10, 0.05]
            )[0],
            "channel": channel,
            "country_code": merchant["country"],
            "fraud_score": score,
            "fraud_decision": decision,
            "is_fraudulent": is_fraud,
            "processing_latency_ms": random.randint(120, 2500),
        })

    return records


def write_parquet(records, output_path):
    """Write transaction records to a Parquet file with Snappy compression."""
    table = pa.Table.from_pylist(records)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table, output_path,
        compression="snappy",
        row_group_size=50000,
    )
    print(f"Wrote {len(records)} records to {output_path}")

    fraud_count = sum(1 for r in records if r["is_fraudulent"])
    total_amount = sum(r["amount_usd"] for r in records)
    print(f"Fraud rate: {fraud_count / len(records) * 100:.2f}%")
    print(f"Total volume: ${total_amount:,.2f}")


def write_merchants_parquet(merchants, output_path):
    """Write merchant reference data to a separate Parquet file."""
    table = pa.Table.from_pylist(merchants)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy")
    print(f"Wrote {len(merchants)} merchants to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic payment transaction data"
    )
    parser.add_argument("--count", type=int, default=100000,
                        help="Number of transactions to generate")
    parser.add_argument("--fraud-rate", type=float, default=0.023,
                        help="Base fraud rate (default: 0.023 = 2.3%%)")
    parser.add_argument("--output", type=str, default="data/transactions.parquet",
                        help="Output file path")
    parser.add_argument("--merchants-output", type=str,
                        default="data/merchants.parquet",
                        help="Merchant reference data output path")
    parser.add_argument("--days", type=int, default=30,
                        help="Number of days to spread transactions across")
    args = parser.parse_args()

    print(f"Generating {args.count} transactions over {args.days} days...")
    merchants = generate_merchants(200)
    card_tokens = generate_card_tokens(50000)

    records = generate_transactions(
        count=args.count,
        merchants=merchants,
        card_tokens=card_tokens,
        fraud_rate=args.fraud_rate,
        days_span=args.days,
    )

    write_parquet(records, args.output)
    write_merchants_parquet(merchants, args.merchants_output)


if __name__ == "__main__":
    main()
