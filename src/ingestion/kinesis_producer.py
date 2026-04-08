"""
Kinesis producer for payment transaction events.

Reads transactions from a Parquet file and publishes them to a
Kinesis Data Stream at a configurable rate. Handles throttling
with exponential backoff.

In production, this would be replaced by direct integration with
the payment gateway. This script exists for development, testing,
and pipeline validation.

Usage:
    python src/ingestion/kinesis_producer.py --rate 100 --duration 60
    python src/ingestion/kinesis_producer.py --rate 500 --duration 300 --stream my-stream
"""

import os
import json
import time
import argparse
import random
from datetime import datetime

import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()


class TransactionProducer:
    """
    Publishes transaction records to a Kinesis Data Stream.

    Reads from a pre-generated Parquet file and sends records
    at a controlled rate to simulate real-time payment traffic.
    """

    def __init__(self, stream_name, region=None, data_path=None):
        self.stream_name = stream_name
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.data_path = data_path or "data/transactions.parquet"

        self._client = boto3.client("kinesis", region_name=self.region)
        self._records = self._load_records()
        self._sent_count = 0
        self._error_count = 0

        print(f"Producer initialized: stream={stream_name}, "
              f"region={self.region}, records_available={len(self._records)}")

    def _load_records(self):
        """Load transactions from Parquet into a list of dicts."""
        table = pq.read_table(self.data_path)
        records = table.to_pydict()

        # Convert columnar format to list of row dicts
        row_count = table.num_rows
        rows = []
        columns = list(records.keys())
        for i in range(row_count):
            row = {col: records[col][i] for col in columns}
            rows.append(row)

        print(f"Loaded {len(rows)} records from {self.data_path}")
        return rows

    def _prepare_record(self, transaction):
        """
        Prepare a single record for Kinesis.

        Overwrites the timestamp with current time so downstream
        consumers see fresh data, not historical timestamps.
        """
        record = dict(transaction)
        record["transaction_ts"] = datetime.utcnow().isoformat()
        record["ingestion_ts"] = datetime.utcnow().isoformat()

        # Convert numpy/pyarrow types to native Python for JSON serialization
        for key, value in record.items():
            if hasattr(value, "item"):
                record[key] = value.item()
            elif isinstance(value, bytes):
                record[key] = value.decode("utf-8")

        return record

    def send_record(self, transaction):
        """
        Send a single record to Kinesis with retry logic.

        Uses merchant_id as partition key so all transactions
        for a merchant land on the same shard. This preserves
        per-merchant ordering for velocity feature computation.
        """
        record = self._prepare_record(transaction)
        payload = json.dumps(record)
        partition_key = record.get("merchant_id", "unknown")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self._client.put_record(
                    StreamName=self.stream_name,
                    Data=payload.encode("utf-8"),
                    PartitionKey=partition_key,
                )
                self._sent_count += 1
                return response

            except self._client.exceptions.ProvisionedThroughputExceededException:
                # Exponential backoff: 0.5s, 1s, 2s
                wait = 0.5 * (2 ** attempt)
                print(f"Throttled. Retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)

            except Exception as e:
                self._error_count += 1
                print(f"Error sending record: {e}")
                return None

        self._error_count += 1
        print(f"Failed after {max_retries} retries")
        return None

    def send_batch(self, transactions):
        """
        Send up to 500 records in a single PutRecords call.

        Batch sending is more efficient than individual puts.
        Kinesis accepts up to 500 records per PutRecords call.
        """
        kinesis_records = []
        for txn in transactions[:500]:
            record = self._prepare_record(txn)
            kinesis_records.append({
                "Data": json.dumps(record).encode("utf-8"),
                "PartitionKey": record.get("merchant_id", "unknown"),
            })

        try:
            response = self._client.put_records(
                StreamName=self.stream_name,
                Records=kinesis_records,
            )

            failed = response.get("FailedRecordCount", 0)
            self._sent_count += len(kinesis_records) - failed
            self._error_count += failed

            if failed > 0:
                print(f"Batch: {len(kinesis_records) - failed} sent, {failed} failed")

            return response

        except Exception as e:
            self._error_count += len(kinesis_records)
            print(f"Batch send error: {e}")
            return None

    def run(self, rate_per_second=100, duration_seconds=60, use_batch=True):
        """
        Main loop. Sends records at the specified rate for the given duration.

        Args:
            rate_per_second: target throughput
            duration_seconds: how long to run
            use_batch: if True, use PutRecords (more efficient)
        """
        print(f"Starting producer: rate={rate_per_second}/s, "
              f"duration={duration_seconds}s, batch={use_batch}")

        start_time = time.time()
        record_index = 0
        batch_size = min(rate_per_second, 500)

        while (time.time() - start_time) < duration_seconds:
            loop_start = time.time()

            if use_batch:
                batch = []
                for _ in range(batch_size):
                    txn = self._records[record_index % len(self._records)]
                    batch.append(txn)
                    record_index += 1
                self.send_batch(batch)
            else:
                for _ in range(rate_per_second):
                    txn = self._records[record_index % len(self._records)]
                    self.send_record(txn)
                    record_index += 1

            # Pace the loop to maintain target rate
            elapsed = time.time() - loop_start
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Progress update every 10 seconds
            total_elapsed = time.time() - start_time
            if int(total_elapsed) % 10 == 0 and int(total_elapsed) > 0:
                actual_rate = self._sent_count / total_elapsed
                print(f"[{int(total_elapsed)}s] sent={self._sent_count}, "
                      f"errors={self._error_count}, "
                      f"rate={actual_rate:.0f}/s")

        total_time = time.time() - start_time
        print(f"\nProducer complete:")
        print(f"  Duration:   {total_time:.1f}s")
        print(f"  Sent:       {self._sent_count}")
        print(f"  Errors:     {self._error_count}")
        print(f"  Avg rate:   {self._sent_count / total_time:.0f}/s")


def main():
    parser = argparse.ArgumentParser(description="Kinesis transaction producer")
    parser.add_argument("--stream", type=str,
                        default="fraudshield-transactions",
                        help="Kinesis stream name")
    parser.add_argument("--rate", type=int, default=100,
                        help="Records per second")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration in seconds")
    parser.add_argument("--data", type=str,
                        default="data/transactions.parquet",
                        help="Path to transaction data file")
    parser.add_argument("--no-batch", action="store_true",
                        help="Send records individually instead of in batches")
    args = parser.parse_args()

    producer = TransactionProducer(
        stream_name=args.stream,
        data_path=args.data,
    )
    producer.run(
        rate_per_second=args.rate,
        duration_seconds=args.duration,
        use_batch=not args.no_batch,
    )


if __name__ == "__main__":
    main()
