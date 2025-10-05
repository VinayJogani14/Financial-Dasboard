# Financial Health Dashboard

> End-to-end financial analytics system featuring PostgreSQL, Tableau, Python automation, and advanced SQL

![Python](https://img.shields.io/badge/Python-3.13-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue)
![Tableau](https://img.shields.io/badge/Tableau-Public-orange)

---

## Project Overview

A comprehensive personal finance analytics solution that transforms transaction data into actionable insights through interactive dashboards, automated reporting, and advanced data analysis.

**Live Dashboard:** [View on Tableau Public](https://public.tableau.com/app/profile/vinay.jogani/viz/Book1_17596201871960/Dashboard5) ← Add your link here

---

## Key Features

### 1. PostgreSQL Database Design
- 7 normalized tables with proper relationships
- 1,754 transactions across 12 months
- Hierarchical category structure
- Indexed for query performance

### 2. Advanced SQL Analytics
- **Window Functions:** RANK, LAG, LEAD, moving averages
- **CTEs & Recursive Queries:** Category hierarchy, debt projections
- **Statistical Analysis:** Z-score anomaly detection, percentiles
- **Time Intelligence:** Month-over-month changes, fiscal calendars
- 12+ complex queries demonstrating enterprise-level SQL

### 3. Interactive Tableau Dashboard (5 Pages)
- **Executive Summary:** KPIs, trends, budget health
- **Spending Analysis:** Category breakdown, merchant analysis, heatmaps
- **Budget Performance:** Variance analysis, forecasts, alerts
- **Financial Goals:** Progress tracking, debt payoff timelines
- **Insights & Alerts:** Anomaly detection, recommendations

### 4. Python Automation
- ETL pipeline with pandas
- Automated CSV/Excel exports
- Monthly PDF report generation
- Data validation and quality checks

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL 18 |
| Languages | Python 3.13, SQL |
| Visualization | Tableau Public |
| Libraries | pandas, psycopg2, matplotlib, reportlab |
| Tools | pgAdmin, GitHub |

---

## Project Structure

```
financial-dashboard/
├── 01_database/          # Database schema and data generation
├── 02_data_prep/         # ETL pipeline and exports
├── 03_dashboards/        # Tableau data sources and screenshots
├── 04_excel_models/      # Financial modeling (optional)
└── 05_reports/           # Automated reporting scripts
```

---

## Quick Start

### Prerequisites
- PostgreSQL 18+
- Python 3.8+
- pip packages: `psycopg2-binary pandas faker`

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/VinayJogani14/financial-dashboard.git
cd financial-dashboard
```

2. **Set up database**
```bash
createdb finance_dashboard
psql -d finance_dashboard -f 01_database/schema.sql
```

3. **Generate sample data**
```bash
cd 01_database
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary faker numpy
python sample_data_generator.py
```

4. **Run analytics queries**
```bash
psql -d finance_dashboard -f 01_database/analytics_queries.sql
```

5. **Export data for Tableau**
```bash
cd ../02_data_prep
python export_csv.py
```

---

## Sample Insights

From analyzing 1,754 transactions over 12 months:

- **Spending Patterns:** Groceries ($41,904/year) largest category
- **Budget Performance:** 7/10 categories under budget
- **Savings Rate:** 22.3% average monthly savings
- **Anomalies Detected:** 23 unusual transactions flagged
- **Subscriptions:** $487/month in recurring charges identified

---

## Skills Demonstrated

**Data Analysis**
- Complex SQL with window functions and CTEs
- Statistical analysis and anomaly detection
- Time-series analysis and forecasting
- KPI development and tracking

**Engineering**
- Database design and normalization
- ETL pipeline development
- Python automation and scripting
- Data quality and validation

**Visualization**
- Interactive dashboard design
- Data storytelling
- Chart selection and formatting
- Business intelligence reporting

---

## Database Schema

**7 Core Tables:**
- `transactions` - All financial transactions
- `categories` - Hierarchical category structure
- `accounts` - Financial accounts (checking, savings, credit)
- `budgets` - Monthly budget allocations
- `financial_goals` - Savings and debt payoff goals
- `debts` - Debt tracking with interest calculations
- `recurring_transactions` - Subscription management

---

## Advanced SQL Examples

**Moving Average with Window Functions:**
```sql
SELECT 
    month,
    category_name,
    monthly_spend,
    AVG(monthly_spend) OVER (
        PARTITION BY category_name
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as three_month_avg
FROM monthly_spending;
```

**Anomaly Detection with Z-Scores:**
```sql
WITH stats AS (
    SELECT category_id, AVG(amount) as avg, STDDEV(amount) as stddev
    FROM transactions GROUP BY category_id
)
SELECT t.*, (t.amount - s.avg) / s.stddev as z_score
FROM transactions t JOIN stats s USING (category_id)
WHERE ABS((t.amount - s.avg) / s.stddev) > 2;
```


