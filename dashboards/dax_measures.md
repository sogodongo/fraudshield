# DAX Measures

## Dashboard 1: Fraud Operations Overview

### Core Metrics
```dax
// Total transactions in the selected period
Total Transactions =
SUM(mv_fraud_overview[total_transactions])

// Total transaction volume in USD
Total Volume =
SUM(mv_fraud_overview[total_volume])

// Number of transactions flagged as high risk (score > 0.7)
High Risk Count =
SUM(mv_fraud_overview[high_risk_count])

// Percentage of transactions flagged as high risk
High Risk Rate =
DIVIDE(
    [High Risk Count],
    [Total Transactions],
    0
)

// Average fraud score across all transactions
Avg Fraud Score =
DIVIDE(
    SUMX(mv_fraud_overview,
        mv_fraud_overview[avg_fraud_score] * mv_fraud_overview[total_transactions]),
    [Total Transactions],
    0
)

// Chargeback rate as percentage of total volume
Chargeback Rate =
DIVIDE(
    SUM(mv_fraud_overview[chargeback_volume]),
    [Total Volume],
    0
)
```

### Conditional Formatting
```dax
// Color code for fraud rate — used in conditional formatting
// Green below 2%, yellow 2-5%, red above 5%
Fraud Rate Color =
VAR rate = [Chargeback Rate]
RETURN
    SWITCH(TRUE(),
        rate >= 0.05, "#E24B4A",
        rate >= 0.02, "#EF9F27",
        "#639922"
    )

// Trend indicator: compare current week to previous week
Fraud Trend =
VAR current_week =
    CALCULATE([High Risk Count],
        DATESINPERIOD(dim_date[date_key], MAX(dim_date[date_key]), -7, DAY))
VAR previous_week =
    CALCULATE([High Risk Count],
        DATESINPERIOD(dim_date[date_key], MAX(dim_date[date_key]) - 7, -7, DAY))
RETURN
    DIVIDE(current_week - previous_week, previous_week, 0)
```

### Suggested Visuals

- **KPI cards** at the top: Total Transactions, Total Volume, High Risk Rate, Chargeback Rate
- **Line chart**: High Risk Count by transaction_date, with trend line
- **Bar chart**: Total Volume by channel, colored by avg_fraud_score
- **Matrix table**: risk_tier (rows) x channel (columns), values = High Risk Rate
- **Slicer**: transaction_date range, risk_tier, channel

---

## Dashboard 2: Chargeback Forecasting

### Core Metrics
```dax
// Total chargebacks in selected period
Total Chargebacks =
SUM(mv_chargeback_trends[chargeback_count])

// Total chargeback dollar amount
Chargeback Amount =
SUM(mv_chargeback_trends[chargeback_amount])

// Average days between transaction and chargeback filing
Avg Days to File =
DIVIDE(
    SUMX(mv_chargeback_trends,
        mv_chargeback_trends[avg_days_to_file] * mv_chargeback_trends[chargeback_count]),
    [Total Chargebacks],
    0
)

// Rolling 30-day chargeback total — used for trend analysis
Rolling 30d Chargebacks =
CALCULATE(
    [Total Chargebacks],
    DATESINPERIOD(dim_date[date_key], MAX(dim_date[date_key]), -30, DAY)
)

// Month-over-month chargeback growth rate
MoM Chargeback Growth =
VAR current_month = [Chargeback Amount]
VAR previous_month =
    CALCULATE([Chargeback Amount], DATEADD(dim_date[date_key], -1, MONTH))
RETURN
    DIVIDE(current_month - previous_month, previous_month, 0)

// Simple linear forecast for next month chargebacks
// Uses last 3 months average growth rate
Forecast Next Month =
VAR avg_monthly =
    CALCULATE(
        [Chargeback Amount],
        DATESINPERIOD(dim_date[date_key], MAX(dim_date[date_key]), -90, DAY)
    ) / 3
VAR growth = [MoM Chargeback Growth]
RETURN
    avg_monthly * (1 + growth)
```

### Suggested Visuals

- **KPI cards**: Total Chargebacks, Chargeback Amount, Avg Days to File
- **Area chart**: Rolling 30d Chargebacks by date, with forecast line
- **Stacked bar**: Chargeback Amount by reason_code and month
- **Histogram**: Distribution of days_to_file (helps predict when chargebacks arrive)
- **Table**: country_code, chargeback_count, chargeback_amount, avg_days_to_file
- **Slicer**: date range, country_code, reason_code

---

## Dashboard 3: Executive KPIs

### Core Metrics
```dax
// Overall fraud detection rate — the headline number
Detection Rate =
DIVIDE(
    SUM(mv_daily_kpis[fraud_count]),
    SUM(mv_daily_kpis[total_transactions]),
    0
)

// Platform-wide fraud rate
Platform Fraud Rate =
DIVIDE(
    SUM(mv_daily_kpis[fraud_count]),
    SUM(mv_daily_kpis[total_transactions]),
    0
)

// Average processing latency in milliseconds
Avg Latency =
DIVIDE(
    SUMX(mv_daily_kpis,
        mv_daily_kpis[avg_latency_ms] * mv_daily_kpis[total_transactions]),
    SUM(mv_daily_kpis[total_transactions]),
    0
)

// Daily transaction volume for sparkline charts
Daily Volume =
SUM(mv_daily_kpis[total_volume])

// KPI target comparison — returns the gap to target
Detection Rate vs Target =
[Detection Rate] - 0.85

Fraud Rate vs Target =
0.008 - [Platform Fraud Rate]
```

### Target Reference Lines
```dax
// These are used as constant lines on charts
// to show where targets sit relative to actuals
Detection Rate Target = 0.85
False Positive Target = 0.05
Chargeback Rate Target = 0.008
Latency Target = 3000
```

### Suggested Visuals

- **Gauge charts**: Detection Rate (target 85%), Fraud Rate (target <0.8%), Latency (target <3s)
- **Line chart**: Detection Rate over time with target reference line at 0.85
- **Combo chart**: Daily Volume (bars) + Fraud Rate (line) over time
- **Card with sparkline**: Total Volume, Total Transactions, Avg Latency (each with 30-day trend)
- **Slicer**: date range only (executive view should be simple)

---

## Row-Level Security

For production deployment, configure RLS so that:
- Fraud analysts see all data
- Regional managers see only their country_code
- Finance sees chargeback data but not individual transaction details
```dax
// RLS filter expression for regional managers
// Applied to the mv_fraud_overview table
[country_code] = USERPRINCIPALNAME()
// Note: actual implementation maps user email to country in a security table
```
