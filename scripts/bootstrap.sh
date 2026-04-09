#!/bin/bash
set -e

echo "============================================"
echo "  FraudShield — Bootstrap Setup"
echo "============================================"
echo ""

# check prerequisites
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo "ERROR: $1 is required but not installed."
        exit 1
    fi
    echo "  [ok] $1 found"
}

echo "Checking prerequisites..."
check_command python3
check_command java
check_command aws

JAVA_VERSION=$(java -version 2>&1 | head -n1)
echo "  Java: $JAVA_VERSION"
echo ""

# set JAVA_HOME if not already set
if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    echo "Set JAVA_HOME=$JAVA_HOME"
fi

# create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi
source venv/bin/activate
echo "Virtual environment activated"
echo ""

# install dependencies
echo "Installing Python dependencies..."
pip install -q -r requirements.txt
pip install -q pandas psycopg2-binary
echo "Dependencies installed"
echo ""

# generate sample data
echo "Generating sample transaction data (100K records)..."
python scripts/generate_transactions.py \
    --count 100000 \
    --output data/transactions.parquet \
    --merchants-output data/merchants.parquet \
    --days 30
echo ""

# run bronze to silver
echo "Running Bronze to Silver ETL..."
python src/processing/batch/bronze_to_silver.py \
    --input data/transactions.parquet \
    --output data/silver/transactions_clean \
    --quarantine data/quarantine/transactions
echo ""

# run silver to gold
echo "Running Silver to Gold ETL..."
python src/processing/batch/silver_to_gold.py \
    --input data/silver/transactions_clean \
    --output data/gold
echo ""

# show KPI summary
echo "============================================"
echo "  Pipeline KPIs"
echo "============================================"
python -c "
import pandas as pd
kpis = pd.read_parquet('data/gold/kpi_daily')
print(f'  Days processed:        {len(kpis)}')
print(f'  Total transactions:    {kpis[\"total_transactions\"].sum():,}')
print(f'  Total volume:          \${kpis[\"total_volume\"].sum():,.2f}')
print(f'  Avg detection rate:    {kpis[\"fraud_detection_rate\"].mean():.1%}')
print(f'  Avg false positive:    {kpis[\"false_positive_rate\"].mean():.1%}')
print(f'  Avg chargeback rate:   {kpis[\"chargeback_rate\"].mean():.1%}')
"
echo ""

# run tests
echo "Running test suite..."
python -m pytest tests/ -v --tb=short
echo ""

echo "============================================"
echo "  Bootstrap complete"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Review data in data/gold/"
echo "  2. Configure .env with AWS credentials"
echo "  3. Run: cd infra/terraform && terraform init"
echo "  4. See docs/SETUP.md for full deployment guide"
