"""
Seed DynamoDB merchant_profiles table from generated merchant data.

Reads the merchants.parquet file produced by generate_transactions.py
and writes each merchant as a DynamoDB item. This populates the lookup
table that the Lambda scorer reads during real-time scoring.

In production, merchant profiles come from the CRM system via a
weekly EventBridge-triggered sync. This script handles the initial
seed and development/testing scenarios.

Usage:
    python scripts/seed_merchant_profiles.py
    python scripts/seed_merchant_profiles.py --table fraudshield-dev-merchant-profiles
    python scripts/seed_merchant_profiles.py --dry-run
"""

import os
import argparse
import logging
from decimal import Decimal

import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seed_merchants")


def load_merchants(filepath="data/merchants.parquet"):
    """Read merchant data from parquet file."""
    table = pq.read_table(filepath)
    records = table.to_pydict()
    row_count = table.num_rows
    columns = list(records.keys())

    merchants = []
    for i in range(row_count):
        row = {col: records[col][i] for col in columns}
        merchants.append(row)

    logger.info(f"Loaded {len(merchants)} merchants from {filepath}")
    return merchants


def prepare_item(merchant):
    """
    Convert a merchant dict into a DynamoDB-compatible item.
    DynamoDB does not accept float — must use Decimal.
    Also converts any numpy types to native Python.
    """
    item = {}
    for key, value in merchant.items():
        if value is None:
            continue
        if isinstance(value, float):
            item[key] = Decimal(str(round(value, 6)))
        elif hasattr(value, "item"):
            # numpy scalar to Python native
            item[key] = value.item()
        else:
            item[key] = value
    return item


def seed_table(merchants, table_name, region=None, dry_run=False):
    """
    Write merchant profiles to DynamoDB.
    Uses batch_writer for efficiency — buffers up to 25 items
    per batch request automatically.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would write {len(merchants)} items to {table_name}")
        for m in merchants[:3]:
            item = prepare_item(m)
            logger.info(f"  Sample: {item['merchant_id']} — {item.get('category_name')} "
                        f"({item.get('risk_tier')})")
        return

    region = region or os.environ.get("AWS_REGION", "us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    written = 0
    errors = 0

    with table.batch_writer() as batch:
        for merchant in merchants:
            try:
                item = prepare_item(merchant)
                batch.put_item(Item=item)
                written += 1
            except Exception as e:
                logger.error(f"Failed to write {merchant.get('merchant_id')}: {e}")
                errors += 1

    logger.info(f"Seeding complete: {written} written, {errors} errors")


def main():
    parser = argparse.ArgumentParser(description="Seed DynamoDB merchant profiles")
    parser.add_argument("--table", type=str,
                        default="fraudshield-dev-merchant-profiles",
                        help="DynamoDB table name")
    parser.add_argument("--data", type=str,
                        default="data/merchants.parquet",
                        help="Path to merchants parquet file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview items without writing to DynamoDB")
    args = parser.parse_args()

    merchants = load_merchants(args.data)
    seed_table(merchants, args.table, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
