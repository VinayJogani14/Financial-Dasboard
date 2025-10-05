-- Financial Health Dashboard - PostgreSQL Schema
-- Database: finance_dashboard

-- Drop existing tables if they exist
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS budgets CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS financial_goals CASCADE;
DROP TABLE IF EXISTS debts CASCADE;
DROP TABLE IF EXISTS recurring_transactions CASCADE;

-- Categories table (hierarchical structure)
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER REFERENCES categories(category_id),
    category_type VARCHAR(20) CHECK (category_type IN ('income', 'expense', 'transfer')),
    is_essential BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Accounts table
CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    account_name VARCHAR(100) NOT NULL,
    account_type VARCHAR(50) CHECK (account_type IN ('checking', 'savings', 'credit_card', 'investment', 'loan')),
    current_balance DECIMAL(12, 2) DEFAULT 0,
    currency VARCHAR(3) DEFAULT 'USD',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table (main fact table)
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    category_id INTEGER REFERENCES categories(category_id),
    transaction_date DATE NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    description TEXT,
    merchant VARCHAR(200),
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('debit', 'credit', 'transfer')),
    is_recurring BOOLEAN DEFAULT FALSE,
    tags TEXT[],
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Budgets table
CREATE TABLE budgets (
    budget_id SERIAL PRIMARY KEY,
    category_id INTEGER REFERENCES categories(category_id),
    budget_period VARCHAR(20) CHECK (budget_period IN ('monthly', 'quarterly', 'yearly')),
    budget_amount DECIMAL(12, 2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Financial goals table
CREATE TABLE financial_goals (
    goal_id SERIAL PRIMARY KEY,
    goal_name VARCHAR(200) NOT NULL,
    goal_type VARCHAR(50) CHECK (goal_type IN ('savings', 'debt_payoff', 'investment', 'emergency_fund', 'other')),
    target_amount DECIMAL(12, 2) NOT NULL,
    current_amount DECIMAL(12, 2) DEFAULT 0,
    target_date DATE,
    priority INTEGER CHECK (priority BETWEEN 1 AND 5),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Debts table
CREATE TABLE debts (
    debt_id SERIAL PRIMARY KEY,
    debt_name VARCHAR(200) NOT NULL,
    debt_type VARCHAR(50) CHECK (debt_type IN ('credit_card', 'student_loan', 'mortgage', 'personal_loan', 'auto_loan', 'other')),
    principal_amount DECIMAL(12, 2) NOT NULL,
    current_balance DECIMAL(12, 2) NOT NULL,
    interest_rate DECIMAL(5, 4) NOT NULL,
    minimum_payment DECIMAL(12, 2) NOT NULL,
    payment_due_day INTEGER CHECK (payment_due_day BETWEEN 1 AND 31),
    start_date DATE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recurring transactions table
CREATE TABLE recurring_transactions (
    recurring_id SERIAL PRIMARY KEY,
    category_id INTEGER REFERENCES categories(category_id),
    description VARCHAR(200) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    frequency VARCHAR(20) CHECK (frequency IN ('daily', 'weekly', 'biweekly', 'monthly', 'quarterly', 'yearly')),
    start_date DATE NOT NULL,
    end_date DATE,
    next_occurrence DATE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_category ON transactions(category_id);
CREATE INDEX idx_transactions_account ON transactions(account_id);
CREATE INDEX idx_transactions_merchant ON transactions(merchant);
CREATE INDEX idx_categories_parent ON categories(parent_category_id);
CREATE INDEX idx_budgets_category ON budgets(category_id);
CREATE INDEX idx_budgets_dates ON budgets(start_date, end_date);

-- Create a view for flattened category hierarchy
CREATE OR REPLACE VIEW category_hierarchy AS
WITH RECURSIVE cat_tree AS (
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        category_type,
        category_name::TEXT as full_path,
        0 as level
    FROM categories
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        c.category_type,
        (ct.full_path || ' > ' || c.category_name)::TEXT as full_path,
        ct.level + 1 as level
    FROM categories c
    INNER JOIN cat_tree ct ON c.parent_category_id = ct.category_id
)
SELECT * FROM cat_tree;

-- Create a view for account balances with transaction history
CREATE OR REPLACE VIEW account_balances_with_history AS
SELECT 
    a.account_id,
    a.account_name,
    a.account_type,
    a.current_balance,
    COALESCE(SUM(CASE WHEN t.transaction_type = 'credit' THEN t.amount ELSE -t.amount END), 0) as total_transactions,
    COUNT(t.transaction_id) as transaction_count,
    MAX(t.transaction_date) as last_transaction_date
FROM accounts a
LEFT JOIN transactions t ON a.account_id = t.account_id
GROUP BY a.account_id, a.account_name, a.account_type, a.current_balance;
