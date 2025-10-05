import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class FinancialDataETL:
    """ETL Pipeline for Financial Dashboard Data"""
    
    def __init__(self, db_config):
        """Initialize with database configuration"""
        self.db_config = db_config
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("✓ Database connection established")
        except Exception as e:
            print(f"❌ Connection error: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("✓ Database connection closed")
    
    def extract_transactions(self, start_date=None, end_date=None):
        """Extract transaction data with category hierarchy"""
        query = """
        SELECT 
            t.transaction_id,
            t.transaction_date,
            t.amount,
            t.description,
            t.merchant,
            t.transaction_type,
            c.category_name,
            c.category_type,
            c.is_essential,
            parent_c.category_name as parent_category,
            a.account_name,
            a.account_type,
            EXTRACT(YEAR FROM t.transaction_date) as year,
            EXTRACT(MONTH FROM t.transaction_date) as month,
            EXTRACT(DOW FROM t.transaction_date) as day_of_week,
            TO_CHAR(t.transaction_date, 'Day') as day_name,
            TO_CHAR(t.transaction_date, 'Month') as month_name
        FROM transactions t
        JOIN categories c ON t.category_id = c.category_id
        LEFT JOIN categories parent_c ON c.parent_category_id = parent_c.category_id
        JOIN accounts a ON t.account_id = a.account_id
        WHERE 1=1
        """
        
        params = []
        if start_date:
            query += " AND t.transaction_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND t.transaction_date <= %s"
            params.append(end_date)
        
        query += " ORDER BY t.transaction_date DESC"
        
        df = pd.read_sql(query, self.conn, params=params)
        print(f"✓ Extracted {len(df):,} transactions")
        return df
    
    def extract_budget_performance(self):
        """Extract budget vs actual performance"""
        query = """
        WITH current_month_spending AS (
            SELECT
                c.category_id,
                c.category_name,
                COALESCE(SUM(t.amount), 0) as actual_spend,
                COUNT(t.transaction_id) as num_transactions
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
            ROUND(((cms.actual_spend / NULLIF(b.budget_amount, 0)) * 100)::numeric, 1) as pct_used,
            cms.num_transactions
        FROM budgets b
        JOIN current_month_spending cms ON b.category_id = cms.category_id
        WHERE b.is_active = TRUE
            AND b.budget_period = 'monthly'
        ORDER BY cms.actual_spend DESC
        """
        
        df = pd.read_sql(query, self.conn)
        print(f"✓ Extracted budget data for {len(df)} categories")
        return df
    
    def extract_monthly_summary(self):
        """Extract monthly income/expense summary"""
        query = """
        SELECT
            DATE_TRUNC('month', t.transaction_date) as month,
            c.category_type,
            SUM(t.amount) as total_amount,
            COUNT(*) as transaction_count,
            AVG(t.amount) as avg_amount
        FROM transactions t
        JOIN categories c ON t.category_id = c.category_id
        GROUP BY 1, 2
        ORDER BY month DESC, category_type
        """
        
        df = pd.read_sql(query, self.conn)
        
        # Pivot to get income and expenses in separate columns
        df_pivot = df.pivot_table(
            index='month',
            columns='category_type',
            values='total_amount',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Calculate savings
        if 'income' in df_pivot.columns and 'expense' in df_pivot.columns:
            df_pivot['savings'] = df_pivot['income'] - df_pivot['expense']
            df_pivot['savings_rate'] = (df_pivot['savings'] / df_pivot['income'] * 100).round(2)
        
        print(f"✓ Extracted {len(df_pivot)} months of summary data")
        return df_pivot
    
    def extract_category_spending(self):
        """Extract spending by category with time series"""
        query = """
        SELECT
            DATE_TRUNC('month', t.transaction_date) as month,
            c.category_name,
            parent_c.category_name as parent_category,
            SUM(t.amount) as total_spend,
            COUNT(*) as transaction_count,
            AVG(t.amount) as avg_transaction
        FROM transactions t
        JOIN categories c ON t.category_id = c.category_id
        LEFT JOIN categories parent_c ON c.parent_category_id = parent_c.category_id
        WHERE t.transaction_type = 'debit'
            AND c.parent_category_id IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY month DESC, total_spend DESC
        """
        
        df = pd.read_sql(query, self.conn)
        print(f"✓ Extracted category spending data ({len(df)} records)")
        return df
    
    def extract_merchant_analysis(self):
        """Extract merchant spending patterns"""
        query = """
        SELECT
            merchant,
            COUNT(*) as transaction_count,
            SUM(amount) as total_spent,
            AVG(amount) as avg_transaction,
            MIN(transaction_date) as first_transaction,
            MAX(transaction_date) as last_transaction,
            STDDEV(amount) as amount_stddev
        FROM transactions
        WHERE transaction_type = 'debit'
            AND merchant IS NOT NULL
        GROUP BY merchant
        HAVING COUNT(*) >= 2
        ORDER BY total_spent DESC
        """
        
        df = pd.read_sql(query, self.conn)
        
        # Calculate days between first and last transaction
        df['days_active'] = (df['last_transaction'] - df['first_transaction']).dt.days
        df['avg_days_between'] = df['days_active'] / (df['transaction_count'] - 1)
        
        print(f"✓ Extracted data for {len(df)} merchants")
        return df
    
    def extract_financial_goals(self):
        """Extract financial goals with progress"""
        query = """
        SELECT
            goal_name,
            goal_type,
            target_amount,
            current_amount,
            target_date,
            priority,
            target_amount - current_amount as amount_remaining,
            ROUND(((current_amount / NULLIF(target_amount, 0)) * 100)::numeric, 2) as pct_complete,
            target_date - CURRENT_DATE as days_remaining
        FROM financial_goals
        WHERE is_active = TRUE
        ORDER BY priority, pct_complete DESC
        """
        
        df = pd.read_sql(query, self.conn)
        
        # Calculate monthly savings needed
        if len(df) > 0:
            df['monthly_savings_needed'] = np.where(
                df['days_remaining'].dt.days > 0,
                df['amount_remaining'] / (df['days_remaining'].dt.days / 30),
                0
            ).round(2)
        
        print(f"✓ Extracted {len(df)} financial goals")
        return df
    
    def transform_for_visualization(self, df):
        """Transform data for dashboard visualization - FIXED VERSION"""
        # Ensure datetime columns are properly formatted
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = pd.to_datetime(df[col])
        
        # Also check for object columns that might be dates
        for col in df.columns:
            if 'date' in col.lower() and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass
        
        # Add calculated fields for better visualization
        if 'transaction_date' in df.columns:
            # Make sure transaction_date is datetime
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            
            # Now safely use .dt accessor
            df['year_month'] = df['transaction_date'].dt.to_period('M').astype(str)
            df['quarter'] = df['transaction_date'].dt.to_period('Q').astype(str)
            df['week'] = df['transaction_date'].dt.isocalendar().week
        
        # Round numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if col not in ['transaction_id', 'year', 'month', 'day_of_week', 'week']:
                df[col] = df[col].round(2)
        
        print("✓ Data transformation complete")
        return df
    
    def export_to_csv(self, df, filename, output_dir='./data_exports'):
        """Export dataframe to CSV"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False)
        print(f"✓ Exported to {filepath}")
    
    def export_to_excel(self, output_file='financial_dashboard_data.xlsx'):
        """Export all datasets to Excel with multiple sheets - FIXED VERSION"""
        print("\n📊 Exporting data to Excel...")
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Transactions
                try:
                    df_transactions = self.extract_transactions()
                    df_transactions = self.transform_for_visualization(df_transactions)
                    df_transactions.to_excel(writer, sheet_name='Transactions', index=False)
                except Exception as e:
                    print(f"⚠️ Warning: Could not export Transactions sheet: {e}")
                
                # Budget Performance
                try:
                    df_budget = self.extract_budget_performance()
                    df_budget.to_excel(writer, sheet_name='Budget Performance', index=False)
                except Exception as e:
                    print(f"⚠️ Warning: Could not export Budget Performance sheet: {e}")
                
                # Monthly Summary
                try:
                    df_monthly = self.extract_monthly_summary()
                    df_monthly.to_excel(writer, sheet_name='Monthly Summary', index=False)
                except Exception as e:
                    print(f"⚠️ Warning: Could not export Monthly Summary sheet: {e}")
                
                # Category Spending
                try:
                    df_category = self.extract_category_spending()
                    df_category.to_excel(writer, sheet_name='Category Spending', index=False)
                except Exception as e:
                    print(f"⚠️ Warning: Could not export Category Spending sheet: {e}")
                
                # Merchant Analysis
                try:
                    df_merchant = self.extract_merchant_analysis()
                    df_merchant.to_excel(writer, sheet_name='Merchant Analysis', index=False)
                except Exception as e:
                    print(f"⚠️ Warning: Could not export Merchant Analysis sheet: {e}")
                
                # Financial Goals
                try:
                    df_goals = self.extract_financial_goals()
                    df_goals.to_excel(writer, sheet_name='Financial Goals', index=False)
                except Exception as e:
                    print(f"⚠️ Warning: Could not export Financial Goals sheet: {e}")
            
            print(f"✅ Data exported to {output_file}")
            
        except Exception as e:
            print(f"❌ Error creating Excel file: {e}")
            print("Falling back to CSV exports...")
            
            # Export as CSVs instead
            import os
            os.makedirs('./data_exports', exist_ok=True)
            
            try:
                df_transactions = self.extract_transactions()
                df_transactions.to_csv('./data_exports/transactions.csv', index=False)
                print("✓ Exported transactions.csv")
            except Exception as e:
                print(f"⚠️ Could not export transactions: {e}")
            
            try:
                df_budget = self.extract_budget_performance()
                df_budget.to_csv('./data_exports/budget_performance.csv', index=False)
                print("✓ Exported budget_performance.csv")
            except Exception as e:
                print(f"⚠️ Could not export budget: {e}")
            
            try:
                df_monthly = self.extract_monthly_summary()
                df_monthly.to_csv('./data_exports/monthly_summary.csv', index=False)
                print("✓ Exported monthly_summary.csv")
            except Exception as e:
                print(f"⚠️ Could not export monthly summary: {e}")

def main():
    """Main execution"""
    # Database configuration - UPDATE YOUR PASSWORD!
    db_config = {
        'host': 'localhost',
        'database': 'finance_dashboard',
        'user': 'postgres',
        'password': 'jogani123'  # ← YOUR PASSWORD HERE
    }
    
    # Initialize ETL
    etl = FinancialDataETL(db_config)
    
    try:
        # Connect to database
        etl.connect()
        
        # Extract and export data
        etl.export_to_excel('financial_dashboard_data.xlsx')
        
        print("\n✅ ETL process completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        etl.disconnect()

if __name__ == "__main__":
    main()