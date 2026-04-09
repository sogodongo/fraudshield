# Engineering Decisions Log

This document captures key technical decisions, iterations, and tradeoffs
encountered during development. Each entry describes the problem, what was
tried, and what was ultimately implemented.

## DL-001: Lazy DynamoDB Initialization in Lambda Scorer

**Problem:** The initial scorer implementation called `boto3.resource("dynamodb")`
at module level. This meant importing the module for local testing required
valid AWS credentials and a network connection, making unit tests impossible
without mocking the entire boto3 layer.

**Resolution:** Moved all DynamoDB initialization behind lazy accessor functions
(`_get_dynamodb()`, `_get_decisions_table()`, `_get_merchant_table()`). The
pure scoring functions `compute_fraud_score()` and `make_decision()` have no
AWS dependencies and can be imported and tested anywhere.

**Impact:** 14 scorer tests run in under 1 second with no AWS configuration
required.

## DL-002: PySpark Null Type Inference in Tests

**Problem:** Test helper functions created DataFrames with `None` values for
nullable columns (currency_code, processing_latency_ms). PySpark 4.x raised
`CANNOT_DETERMINE_TYPE` because it could not infer the column type from a
null-only sample.

**Previous approach:** Relied on PySpark type inference from the data values.

**Resolution:** Added explicit `StructType` schemas to test DataFrame creation.
This is more verbose but eliminates version-dependent inference behavior.

**Lesson:** Always provide explicit schemas when creating DataFrames with
nullable columns. Do not rely on inference across PySpark versions.

## DL-003: Fraud Score Distribution Tuning

**Problem:** The initial data generator used uniform random fraud scores.
This produced an unrealistic distribution where legitimate and fraudulent
transactions had overlapping scores, making the scoring threshold meaningless.

**Resolution:** Switched to Beta distributions. Fraudulent transactions use
`Beta(5, 2)` (skewed toward 1.0), legitimate transactions use `Beta(1.5, 8)`
(skewed toward 0.0). This produces a clear separation between populations
with realistic overlap in the middle range where the model is uncertain.

**Result:** The gold layer KPIs show 93% detection rate, which validates that
the score distributions are separable but not perfectly so.

## DL-004: False Positive Rate Exceeds Target

**Problem:** The pipeline KPIs show a 33% false positive rate against a 5%
target. One in three flagged transactions is actually legitimate.

**Root cause:** The rule-based scoring model lacks the feature interactions
that a trained ML model would capture. It over-weights individual risk factors
(channel, amount, merchant tier) without considering how they combine. A $50
online purchase from a medium-risk merchant gets scored similarly to a $50
phone order from a low-risk merchant, even though the risk profiles are different.

**Current status:** Accepted as a known limitation. The pipeline infrastructure
is correct. The scoring threshold (0.45) could be raised to reduce false positives
at the cost of detection rate. In production, model retraining with actual fraud
labels would address this systematically.

## DL-005: Chargeback Rate Approximation

**Problem:** Real chargebacks arrive T+3 to T+45 after the original transaction.
The gold layer needs a chargeback rate metric, but we do not have a separate
chargeback feed in the synthetic data.

**Resolution:** Used the `is_fraudulent` flag from the generator as a proxy for
chargebacks. The daily pipeline computes chargeback_rate as fraud_count divided
by total_transactions. This approximation is documented in the silver_to_gold
code and the KPI dashboard DAX measures.

**Production difference:** The real system would join `fct_chargebacks` to
`fct_transactions` with a 45-day lookback window, and the Redshift load script
already includes the chargeback flag UPDATE statement for this purpose.

## DL-006: Docker Build and Network Constraints

**Problem:** PySpark package is 455MB. Docker builds timed out or produced
corrupted downloads on bandwidth-constrained connections.

**Resolution:** The Dockerfile and docker-compose.yml are committed as correct,
tested configurations. Local development uses the native Python virtual
environment instead. The CI pipeline (GitHub Actions) builds on faster
infrastructure where the download completes reliably.

**Alternative considered:** Pre-building a base image with PySpark and pushing
it to ECR. This would eliminate the download during builds but adds an image
maintenance burden. Not justified for the current team size.

## DL-007: Medallion Architecture over Iceberg

**Problem:** Needed to choose between plain Parquet with Hive-style partitions
and Apache Iceberg for the data lake format.

**Decision:** Plain Parquet for the initial implementation.

**Reasoning:** Iceberg provides time travel, schema evolution, and hidden
partitioning, all of which are valuable. However, Glue Iceberg support was
still stabilizing at the time of development, the team had no Iceberg
operational experience, and schema changes in this system are infrequent
enough that Glue Data Catalog versioning is sufficient.

**Migration path:** Documented as the highest-priority item in the roadmap.
The transformation code writes standard Parquet and would require minimal
changes to write Iceberg tables instead.

## DL-008: Quality Framework vs Great Expectations

**Problem:** Needed data quality validation between pipeline stages. Great
Expectations is the industry standard but adds significant dependency weight
and configuration complexity.

**Decision:** Built a lightweight validation framework with simple check
functions returning `CheckResult` dataclass objects.

**Reasoning:** The custom framework has 6 check types (row count, null rate,
value range, uniqueness, valid values, freshness) covering all current
validation needs in approximately 150 lines of code. Great Expectations would
provide expectation suites, data docs, and checkpoint orchestration, but those
features are not needed at the current scale.

**When to switch:** If the number of data sources exceeds 10, or if
non-engineers need to define quality rules through a UI, Great Expectations
becomes worth the complexity.
