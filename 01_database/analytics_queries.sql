-- FINANCIAL HEALTH DASHBOARD - SQL ANALYTICS QUERIES
-- Collection of 12 advanced SQL queries demonstrating window functions, CTEs, and complex analysis

-- 1. MONTHLY SPENDING TRENDS WITH MOVING AVERAGES
WITH monthly_spending AS (
    SELECT
        DATE_TRUNC('month', transaction_date) as month,
        c.category_name,
        SUM(t.amount) as monthly_spend,
        COUNT(*) as transaction_count
    FROM transactions t
    JOIN categories c ON t.category_id = c.category_id
    WHERE t.transaction_type = 'debit'
    GROUP BY 1, 2
)
SELECT
    month,
    category_name,
    monthly_spend,
    transaction_count,
    AVG(monthly_spend) OVER (
        PARTITION BY category_name
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as three_month_avg,
    monthly_spend - LAG(monthly_spend) OVER (
        PARTITION BY category_name
        ORDER BY month
    ) as mom_change
FROM monthly_spending
ORDER BY month DESC, monthly_spend DESC;

-- 2. BUDGET VS ACTUAL WITH VARIANCE ANALYSIS
WITH current_month_spending AS (
    SELECT
        c.category_id,
        c.category_name,
        COALESCE(SUM(t.amount), 0) as actual_spend
    FROM categories c
    LEFT JOIN transactions t ON c.category_id = t.category_id
        AND DATE_TRUNC('month', t.transaction_date) = DATE_TRUNC('month', CURRENT_DATE)
        AND t.transaction_type = 'debit'
    WHERE c.parent_category_id IS NOT NULL
    GROUP BY c.category_id, c.category_name
)
SELECT
    cms.category_name,
    b.budget_amount,
    cms.actual_spend,
    b.budget_amount - cms.actual_spend as variance,
    ROUND(((cms.actual_spend / NULLIF(b.budget_amount, 0)) * 100)::numeric, 1) as pct_used
FROM budgets b
JOIN current_month_spending cms ON b.category_id = cms.category_id
WHERE b.is_active = TRUE
ORDER BY pct_used DESC;

-- 3. TOP MERCHANTS BY SPENDING
SELECT
    merchant,
    COUNT(*) as transaction_count,
    SUM(amount) as total_spent,
    ROUND(AVG(amount)::numeric, 2) as avg_transaction
FROM transactions
WHERE transaction_type = 'debit'
    AND merchant IS NOT NULL
GROUP BY merchant
HAVING COUNT(*) >= 3
ORDER BY total_spent DESC
LIMIT 20;

-- 4. ANOMALY DETECTION (Z-SCORE ANALYSIS)
WITH category_stats AS (
    SELECT
        category_id,
        AVG(amount) as avg_amount,
        STDDEV(amount) as stddev_amount
    FROM transactions
    WHERE transaction_type = 'debit'
    GROUP BY category_id
)
SELECT 
    t.transaction_id,
    t.transaction_date,
    c.category_name,
    t.amount,
    t.merchant,
    cs.avg_amount as category_avg,
    ROUND(((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0))::numeric, 2) as z_score
FROM transactions t
JOIN categories c ON t.category_id = c.category_id
JOIN category_stats cs ON t.category_id = cs.category_id
WHERE t.transaction_type = 'debit'
    AND ABS((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0)) > 2
ORDER BY ABS((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0)) DESC
LIMIT 50;

-- 5. SAVINGS RATE CALCULATION
WITH monthly_summary AS (
    SELECT
        DATE_TRUNC('month', t.transaction_date) as month,
        SUM(CASE WHEN c.category_type = 'income' THEN t.amount ELSE 0 END) as total_income,
        SUM(CASE WHEN c.category_type = 'expense' AND t.transaction_type = 'debit' THEN t.amount ELSE 0 END) as total_expenses
    FROM transactions t
    JOIN categories c ON t.category_id = c.category_id
    GROUP BY 1
)
SELECT
    month,
    total_income,
    total_expenses,
    total_income - total_expenses as net_savings,
    ROUND(((total_income - total_expenses) / NULLIF(total_income, 0) * 100)::numeric, 2) as savings_rate_pct
FROM monthly_summary
ORDER BY month DESC;
