import psycopg2
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # For Mac compatibility
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
import warnings
warnings.filterwarnings('ignore')

# Set style for charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)

class FinancialReportGenerator:
    """Generate automated financial reports"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.styles = getSampleStyleSheet()
        self.create_custom_styles()
        
    def create_custom_styles(self):
        """Create custom paragraph styles"""
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#0066cc'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        
        self.heading_style = ParagraphStyle(
            'CustomHeading',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#333333'),
            spaceAfter=12,
            spaceBefore=12
        )
        
        self.alert_style = ParagraphStyle(
            'Alert',
            parent=self.styles['Normal'],
            fontSize=12,
            textColor=colors.red,
            spaceAfter=10
        )
        
    def connect(self):
        """Connect to database"""
        self.conn = psycopg2.connect(**self.db_config)
    
    def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            self.conn.close()
    
    def get_monthly_summary(self):
        """Get current month financial summary"""
        query = """
        WITH current_month AS (
            SELECT
                SUM(CASE WHEN c.category_type = 'income' THEN t.amount ELSE 0 END) as income,
                SUM(CASE WHEN c.category_type = 'expense' AND t.transaction_type = 'debit' 
                    THEN t.amount ELSE 0 END) as expenses
            FROM transactions t
            JOIN categories c ON t.category_id = c.category_id
            WHERE DATE_TRUNC('month', t.transaction_date) = DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT 
            income,
            expenses,
            income - expenses as savings,
            CASE WHEN income > 0 THEN
                ROUND(((income - expenses) / income * 100)::numeric, 2)
            ELSE 0 END as savings_rate
        FROM current_month
        """
        df = pd.read_sql(query, self.conn)
        return df.iloc[0] if len(df) > 0 else pd.Series({'income': 0, 'expenses': 0, 'savings': 0, 'savings_rate': 0})
    
    def get_budget_performance(self):
        """Get budget vs actual data"""
        query = """
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
            COALESCE(b.budget_amount, 0) as budget,
            cms.actual_spend,
            COALESCE(b.budget_amount, 0) - cms.actual_spend as variance,
            CASE WHEN b.budget_amount > 0 THEN
                ROUND((cms.actual_spend / b.budget_amount * 100)::numeric, 1)
            ELSE 0 END as pct_used
        FROM current_month_spending cms
        LEFT JOIN budgets b ON cms.category_id = b.category_id AND b.is_active = TRUE
        WHERE b.budget_amount IS NOT NULL
        ORDER BY pct_used DESC
        """
        return pd.read_sql(query, self.conn)
    
    def get_top_expenses(self, limit=10):
        """Get top expenses for the month"""
        query = f"""
        SELECT
            transaction_date,
            merchant,
            c.category_name,
            amount
        FROM transactions t
        JOIN categories c ON t.category_id = c.category_id
        WHERE DATE_TRUNC('month', t.transaction_date) = DATE_TRUNC('month', CURRENT_DATE)
            AND t.transaction_type = 'debit'
        ORDER BY amount DESC
        LIMIT {limit}
        """
        return pd.read_sql(query, self.conn)
    
    def get_spending_trends(self):
        """Get last 6 months spending trends"""
        query = """
        SELECT
            DATE_TRUNC('month', transaction_date)::date as month,
            SUM(amount) as total_spent
        FROM transactions t
        JOIN categories c ON t.category_id = c.category_id
        WHERE c.category_type = 'expense'
            AND t.transaction_type = 'debit'
            AND transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 months'
        GROUP BY 1
        ORDER BY 1
        """
        return pd.read_sql(query, self.conn)
    
    def get_alerts(self):
        """Generate financial alerts"""
        alerts = []
        
        # Budget alerts
        budget_df = self.get_budget_performance()
        over_budget = budget_df[budget_df['pct_used'] > 100]
        
        if not over_budget.empty:
            for _, row in over_budget.iterrows():
                alerts.append({
                    'type': 'budget_exceeded',
                    'severity': 'high',
                    'message': f"🔴 {row['category_name']}: ${row['actual_spend']:.2f} spent (${abs(row['variance']):.2f} over budget)"
                })
        
        near_budget = budget_df[(budget_df['pct_used'] >= 90) & (budget_df['pct_used'] <= 100)]
        if not near_budget.empty:
            for _, row in near_budget.iterrows():
                alerts.append({
                    'type': 'budget_warning',
                    'severity': 'medium',
                    'message': f"🟡 {row['category_name']}: {row['pct_used']:.1f}% of budget used"
                })
        
        return alerts
    
    def create_chart(self, data, title, filename):
        """Create and save chart"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if 'month' in data.columns and 'total_spent' in data.columns:
            data.plot(kind='line', x='month', y='total_spent', ax=ax, marker='o', color='#0066cc', linewidth=2)
            ax.set_xlabel('Month', fontsize=12)
            ax.set_ylabel('Amount ($)', fontsize=12)
        
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def generate_monthly_report(self, output_file='monthly_financial_report.pdf'):
        """Generate comprehensive monthly report"""
        print("📊 Generating monthly financial report...")
        
        # Connect to database
        self.connect()
        
        try:
            # Fetch data
            summary = self.get_monthly_summary()
            budget_df = self.get_budget_performance()
            top_expenses = self.get_top_expenses()
            trends = self.get_spending_trends()
            alerts = self.get_alerts()
            
            # Create PDF document
            doc = SimpleDocTemplate(output_file, pagesize=letter)
            story = []
            
            # Title
            title = Paragraph(
                f"Monthly Financial Report<br/>{datetime.now().strftime('%B %Y')}",
                self.title_style
            )
            story.append(title)
            story.append(Spacer(1, 0.3*inch))
            
            # Executive Summary Section
            story.append(Paragraph("Executive Summary", self.heading_style))
            
            summary_data = [
                ['Metric', 'Amount', 'Status'],
                ['Total Income', f"${summary['income']:,.2f}", '💰'],
                ['Total Expenses', f"${summary['expenses']:,.2f}", '💸'],
                ['Net Savings', f"${summary['savings']:,.2f}", '✅' if summary['savings'] > 0 else '⚠️'],
                ['Savings Rate', f"{summary['savings_rate']:.1f}%", 
                 '🟢' if summary['savings_rate'] >= 20 else '🟡' if summary['savings_rate'] >= 10 else '🔴']
            ]
            
            summary_table = Table(summary_data, colWidths=[2*inch, 1.5*inch, 1*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(summary_table)
            story.append(Spacer(1, 0.3*inch))
            
            # Alerts Section
            if alerts:
                story.append(Paragraph("⚠️ Alerts & Notifications", self.heading_style))
                for alert in alerts[:5]:  # Limit to 5 alerts
                    story.append(Paragraph(alert['message'], self.alert_style))
                story.append(Spacer(1, 0.2*inch))
            
            # Budget Performance Section
            story.append(Paragraph("Budget Performance", self.heading_style))
            
            if not budget_df.empty:
                budget_data = [['Category', 'Budget', 'Actual', 'Variance', '% Used']]
                for _, row in budget_df.head(10).iterrows():
                    budget_data.append([
                        row['category_name'],
                        f"${row['budget']:,.2f}",
                        f"${row['actual_spend']:,.2f}",
                        f"${row['variance']:,.2f}",
                        f"{row['pct_used']:.1f}%"
                    ])
                
                budget_table = Table(budget_data, colWidths=[1.8*inch, 1*inch, 1*inch, 1*inch, 0.8*inch])
                budget_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
                ]))
                
                story.append(budget_table)
                story.append(Spacer(1, 0.3*inch))
            
            # Create spending trend chart
            if not trends.empty:
                trends_chart = self.create_chart(
                    trends,
                    '6-Month Spending Trend',
                    'spending_trend.png'
                )
                story.append(Image(trends_chart, width=6*inch, height=3.6*inch))
                story.append(Spacer(1, 0.3*inch))
            
            # Top Expenses Section
            story.append(Paragraph("Top 10 Expenses This Month", self.heading_style))
            
            if not top_expenses.empty:
                expenses_data = [['Date', 'Merchant', 'Category', 'Amount']]
                for _, row in top_expenses.iterrows():
                    expenses_data.append([
                        row['transaction_date'].strftime('%m/%d') if pd.notnull(row['transaction_date']) else 'N/A',
                        str(row['merchant'])[:25] if pd.notnull(row['merchant']) else 'N/A',
                        str(row['category_name']),
                        f"${row['amount']:,.2f}"
                    ])
                
                expenses_table = Table(expenses_data, colWidths=[0.8*inch, 2*inch, 1.5*inch, 1*inch])
                expenses_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('ALIGN', (-1, 0), (-1, -1), 'RIGHT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
                ]))
                
                story.append(expenses_table)
                story.append(Spacer(1, 0.3*inch))
            
            # Recommendations Section
            story.append(Paragraph("💡 Recommendations", self.heading_style))
            
            recommendations = []
            if summary['savings_rate'] < 20:
                recommendations.append("• Consider increasing your savings rate to at least 20%")
            if len(alerts) > 0:
                recommendations.append("• Review budget categories that exceeded limits")
            
            if not budget_df.empty:
                over_budget_categories = budget_df[budget_df['pct_used'] > 100]
                if not over_budget_categories.empty:
                    top_overspend = over_budget_categories.iloc[0]
                    recommendations.append(f"• Focus on reducing {top_overspend['category_name']} spending")
            
            if not recommendations:
                recommendations.append("• Great job! You're managing your budget well.")
                recommendations.append("• Keep up the consistent saving habits.")
            
            for rec in recommendations:
                story.append(Paragraph(rec, self.styles['Normal']))
            
            # Build PDF
            doc.build(story)
            
            print(f"✅ Report generated: {output_file}")
            
            return output_file
            
        except Exception as e:
            print(f"❌ Error generating report: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.disconnect()

def main():
    """Main execution"""
    # Database configuration - UPDATE YOUR PASSWORD!
    db_config = {
        'host': 'localhost',
        'database': 'finance_dashboard',
        'user': 'postgres',
        'password': 'jogani123'  # ← YOUR PASSWORD HERE
    }
    
    # Generate reports
    generator = FinancialReportGenerator(db_config)
    
    # Generate monthly report
    monthly_report = generator.generate_monthly_report()
    print(f"✅ Monthly report: {monthly_report}")
    
    print("\n🎉 Report generation complete!")

if __name__ == "__main__":
    main()