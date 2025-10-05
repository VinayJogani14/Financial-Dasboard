import random
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
import numpy as np

fake = Faker()

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="finance_dashboard",
        user="postgres",
        password="jogani123"
    )

def insert_categories(conn):
    """Insert hierarchical categories"""
    cursor = conn.cursor()
    
    categories = [
        # Income categories
        ('Salary', None, 'income', False),
        ('Freelance', None, 'income', False),
        ('Investment Income', None, 'income', False),
        ('Other Income', None, 'income', False),
        
        # Expense categories - Housing
        ('Housing', None, 'expense', True),
        ('Rent/Mortgage', 5, 'expense', True),
        ('Property Tax', 5, 'expense', True),
        ('Home Insurance', 5, 'expense', True),
        ('Utilities', 5, 'expense', True),
        ('Home Maintenance', 5, 'expense', False),
        
        # Transportation
        ('Transportation', None, 'expense', True),
        ('Car Payment', 11, 'expense', True),
        ('Gas/Fuel', 11, 'expense', True),
        ('Car Insurance', 11, 'expense', True),
        ('Public Transit', 11, 'expense', True),
        ('Car Maintenance', 11, 'expense', False),
        
        # Food
        ('Food', None, 'expense', True),
        ('Groceries', 17, 'expense', True),
        ('Restaurants', 17, 'expense', False),
        ('Coffee Shops', 17, 'expense', False),
        
        # Healthcare
        ('Healthcare', None, 'expense', True),
        ('Health Insurance', 21, 'expense', True),
        ('Medical', 21, 'expense', True),
        ('Pharmacy', 21, 'expense', True),
        ('Dental', 21, 'expense', False),
        
        # Entertainment
        ('Entertainment', None, 'expense', False),
        ('Streaming Services', 26, 'expense', False),
        ('Movies/Events', 26, 'expense', False),
        ('Hobbies', 26, 'expense', False),
        
        # Shopping
        ('Shopping', None, 'expense', False),
        ('Clothing', 30, 'expense', False),
        ('Electronics', 30, 'expense', False),
        ('Home Goods', 30, 'expense', False),
        
        # Personal
        ('Personal', None, 'expense', False),
        ('Gym/Fitness', 34, 'expense', False),
        ('Personal Care', 34, 'expense', False),
        ('Education', 34, 'expense', False),
        
        # Financial
        ('Financial', None, 'expense', True),
        ('Debt Payment', 38, 'expense', True),
        ('Savings', 38, 'expense', True),
        ('Investment', 38, 'expense', False),
    ]
    
    cursor.executemany(
        "INSERT INTO categories (category_name, parent_category_id, category_type, is_essential) VALUES (%s, %s, %s, %s)",
        categories
    )
    conn.commit()
    print(f"✓ Inserted {len(categories)} categories")
    cursor.close()

def insert_accounts(conn):
    """Insert various account types"""
    cursor = conn.cursor()
    
    accounts = [
        ('Primary Checking', 'checking', 5420.75),
        ('Savings Account', 'savings', 15230.50),
        ('Emergency Fund', 'savings', 8500.00),
        ('Chase Credit Card', 'credit_card', -2341.20),
        ('Amex Credit Card', 'credit_card', -876.45),
        ('Investment Account', 'investment', 45230.80),
        ('401k', 'investment', 87650.30),
    ]
    
    cursor.executemany(
        "INSERT INTO accounts (account_name, account_type, current_balance) VALUES (%s, %s, %s)",
        accounts
    )
    conn.commit()
    print(f"✓ Inserted {len(accounts)} accounts")
    cursor.close()

def insert_budgets(conn):
    """Insert monthly budgets"""
    cursor = conn.cursor()
    
    # Get category IDs
    cursor.execute("SELECT category_id, category_name FROM categories WHERE parent_category_id IS NOT NULL")
    categories = cursor.fetchall()
    
    budgets = []
    start_date = datetime.now().replace(day=1)
    
    # Budget amounts by category
    budget_amounts = {
        'Groceries': 600,
        'Restaurants': 300,
        'Coffee Shops': 100,
        'Gas/Fuel': 200,
        'Streaming Services': 50,
        'Gym/Fitness': 80,
        'Utilities': 150,
        'Car Insurance': 150,
        'Health Insurance': 400,
        'Rent/Mortgage': 2000,
    }
    
    for cat_id, cat_name in categories:
        if cat_name in budget_amounts:
            budgets.append((cat_id, 'monthly', budget_amounts[cat_name], start_date))
    
    cursor.executemany(
        "INSERT INTO budgets (category_id, budget_period, budget_amount, start_date) VALUES (%s, %s, %s, %s)",
        budgets
    )
    conn.commit()
    print(f"✓ Inserted {len(budgets)} budgets")
    cursor.close()

def insert_transactions(conn, num_months=12):
    """Generate realistic transaction history"""
    cursor = conn.cursor()
    
    # Get categories and accounts
    cursor.execute("SELECT category_id, category_name, category_type FROM categories")
    categories = {name: (cat_id, cat_type) for cat_id, name, cat_type in cursor.fetchall()}
    
    cursor.execute("SELECT account_id, account_type FROM accounts")
    accounts = cursor.fetchall()
    checking_accounts = [acc[0] for acc in accounts if acc[1] == 'checking']
    credit_accounts = [acc[0] for acc in accounts if acc[1] == 'credit_card']
    
    transactions = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_months * 30)
    
    # Merchants by category
    merchants = {
        'Groceries': ['Whole Foods', 'Safeway', 'Trader Joes', 'Costco', 'Target'],
        'Restaurants': ['Chipotle', 'Olive Garden', 'Local Bistro', 'Sushi Palace', 'Pizza Hut'],
        'Coffee Shops': ['Starbucks', 'Peets Coffee', 'Local Cafe', 'Dunkin'],
        'Gas/Fuel': ['Shell', 'Chevron', '76 Station', 'Arco'],
        'Streaming Services': ['Netflix', 'Spotify', 'Disney+', 'HBO Max'],
        'Utilities': ['PG&E', 'Water District', 'Internet Provider'],
        'Gym/Fitness': ['LA Fitness', 'Planet Fitness', 'Yoga Studio'],
    }
    
    # Generate monthly recurring income
    current_date = start_date
    while current_date <= end_date:
        # Salary (monthly)
        if current_date.day == 1:
            cat_id = categories['Salary'][0]
            account_id = random.choice(checking_accounts)
            amount = round(random.uniform(4800, 5200), 2)
            transactions.append((
                account_id, cat_id, current_date, amount, 
                'Monthly Salary Deposit', 'Employer Inc', 'credit', False
            ))
        
        current_date += timedelta(days=1)
    
    # Generate expenses
    current_date = start_date
    while current_date <= end_date:
        num_transactions = random.randint(2, 8)  # 2-8 transactions per day
        
        for _ in range(num_transactions):
            # Select category weighted by frequency
            expense_categories = [
                ('Groceries', 0.20),
                ('Restaurants', 0.15),
                ('Coffee Shops', 0.10),
                ('Gas/Fuel', 0.10),
                ('Shopping', 0.08),
                ('Entertainment', 0.07),
                ('Utilities', 0.05),
                ('Home Maintenance', 0.05),
                ('Personal Care', 0.05),
                ('Medical', 0.03),
                ('Car Maintenance', 0.02),
            ]
            
            cat_name = random.choices(
                [c[0] for c in expense_categories],
                weights=[c[1] for c in expense_categories]
            )[0]
            
            if cat_name not in categories:
                continue
            
            cat_id, cat_type = categories[cat_name]
            
            # Amount varies by category
            amount_ranges = {
                'Groceries': (40, 180),
                'Restaurants': (15, 85),
                'Coffee Shops': (4, 12),
                'Gas/Fuel': (35, 75),
                'Utilities': (80, 200),
                'Shopping': (25, 250),
                'Entertainment': (10, 60),
                'Medical': (20, 300),
            }
            
            amount_range = amount_ranges.get(cat_name, (10, 100))
            amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
            
            # Randomly assign to checking or credit card
            account_id = random.choice(checking_accounts + credit_accounts)
            
            merchant = random.choice(merchants.get(cat_name, ['Generic Merchant']))
            
            transactions.append((
                account_id, cat_id, current_date, amount,
                f'Purchase at {merchant}', merchant, 'debit', False
            ))
        
        current_date += timedelta(days=1)
    
    # Insert in batches
    batch_size = 1000
    for i in range(0, len(transactions), batch_size):
        batch = transactions[i:i+batch_size]
        cursor.executemany(
            """INSERT INTO transactions 
            (account_id, category_id, transaction_date, amount, description, merchant, transaction_type, is_recurring) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            batch
        )
        conn.commit()
        print(f"✓ Inserted batch {i//batch_size + 1} ({len(batch)} transactions)")
    
    print(f"✓ Total transactions inserted: {len(transactions)}")
    cursor.close()

def insert_financial_goals(conn):
    """Insert financial goals"""
    cursor = conn.cursor()
    
    goals = [
        ('Emergency Fund', 'emergency_fund', 15000, 8500, '2025-12-31', 1),
        ('House Down Payment', 'savings', 50000, 12300, '2027-06-30', 2),
        ('Vacation Fund', 'savings', 5000, 1200, '2025-08-01', 3),
        ('Pay Off Credit Card', 'debt_payoff', 3217.65, 875.45, '2026-03-31', 1),
        ('Investment Portfolio', 'investment', 100000, 45230, '2030-12-31', 2),
    ]
    
    cursor.executemany(
        """INSERT INTO financial_goals 
        (goal_name, goal_type, target_amount, current_amount, target_date, priority) 
        VALUES (%s, %s, %s, %s, %s, %s)""",
        goals
    )
    conn.commit()
    print(f"✓ Inserted {len(goals)} financial goals")
    cursor.close()

def insert_debts(conn):
    """Insert debt accounts"""
    cursor = conn.cursor()
    
    debts = [
        ('Chase Credit Card', 'credit_card', 5000, 2341.20, 0.1899, 75, 15, '2023-01-15'),
        ('Amex Credit Card', 'credit_card', 3000, 876.45, 0.1649, 35, 20, '2023-06-01'),
        ('Student Loan', 'student_loan', 35000, 28450.00, 0.0465, 325, 1, '2018-09-01'),
        ('Car Loan', 'auto_loan', 28000, 18230.50, 0.0499, 485, 10, '2022-03-15'),
    ]
    
    cursor.executemany(
        """INSERT INTO debts 
        (debt_name, debt_type, principal_amount, current_balance, interest_rate, 
        minimum_payment, payment_due_day, start_date) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        debts
    )
    conn.commit()
    print(f"✓ Inserted {len(debts)} debt accounts")
    cursor.close()

def main():
    """Main execution function"""
    print("🚀 Starting data generation...\n")
    
    conn = get_db_connection()
    
    try:
        insert_categories(conn)
        insert_accounts(conn)
        insert_budgets(conn)
        insert_financial_goals(conn)
        insert_debts(conn)
        insert_transactions(conn, num_months=12)  # 12 months of data
        
        print("\n✅ Data generation completed successfully!")
        
        # Print summary
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions")
        txn_count = cursor.fetchone()[0]
        cursor.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM transactions")
        date_range = cursor.fetchone()
        
        print(f"\n📊 Summary:")
        print(f"   • Transactions: {txn_count:,}")
        print(f"   • Date Range: {date_range[0]} to {date_range[1]}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()