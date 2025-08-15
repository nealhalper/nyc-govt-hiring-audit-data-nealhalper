#!/bin/bash
# NYC Hiring Audit Pipeline - Full Development Workflow

set -e

echo "🚀 Starting NYC Hiring Audit Data Pipeline"
echo "=========================================="

# Step 1: Run Python data acquisition and processing
echo "📥 Step 1: Data Acquisition and Processing"
cd /workspace
python run_data_pipeline.py

# Step 2: Load raw data to PostgreSQL (if not done by pipeline)
echo "🗄️  Step 2: Loading Raw Data to PostgreSQL"
# This would typically be done by your Python pipeline
# But we can also load directly if needed

# Step 3: Initialize/Update DBT dependencies
echo "📦 Step 3: Installing DBT Dependencies"
cd /workspace/dbt_project
dbt deps || echo "No packages to install"

# Step 4: Test DBT connection
echo "🔧 Step 4: Testing DBT Connection"
dbt debug

# Step 5: Load seed data (reference tables, lookups, etc.)
echo "🌱 Step 5: Loading Seed Data"
dbt seed --show

# Step 6: Run DBT transformations
echo "🔄 Step 6: Running DBT Transformations"
dbt run --show

# Step 7: Test data quality
echo "🧪 Step 7: Running Data Quality Tests"
dbt test --show

# Step 8: Generate documentation
echo "📚 Step 8: Generating Documentation"
dbt docs generate

echo ""
echo "✅ Pipeline completed successfully!"
echo ""
echo "🎯 Next steps:"
echo "- View DBT docs: dbt docs serve --port 8000"
echo "- Connect to database: psql -h postgres -U airflow_user -d nyc_hiring_audit"
echo "- Access Jupyter: jupyter lab --port 8888 --no-browser --allow-root"
echo ""
echo "🔗 Key tables created:"
echo "- dbt_dev.stg_job_postings (cleaned job postings)"
echo "- dbt_dev.stg_payroll_data (cleaned payroll data)" 
echo "- dbt_dev.int_salary_benchmarks (salary statistics by role)"
echo "- dbt_dev.mart_job_posting_analysis (final analysis table)"
