#!/bin/bash
# Post-create setup script for NYC Hiring Audit Dev Container

set -e

echo "🚀 Setting up NYC Hiring Audit Development Environment..."

# Create necessary directories
mkdir -p /workspace/logs
mkdir -p /workspace/data/raw
mkdir -p /workspace/data/processed
mkdir -p /workspace/data/metadata
mkdir -p /workspace/.dbt

# Set up DBT profiles
echo "📊 Configuring DBT profiles..."
cat > /workspace/.dbt/profiles.yml << EOF
nyc_hiring_audit:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: airflow_user
      password: airflow_password
      port: 5432
      dbname: nyc_hiring_audit
      schema: dbt_dev
      threads: 4
      keepalives_idle: 0
    prod:
      type: postgres
      host: postgres
      user: airflow_user
      password: airflow_password
      port: 5432
      dbname: nyc_hiring_audit
      schema: dbt_prod
      threads: 4
      keepalives_idle: 0
EOF

# Initialize DBT project if it doesn't exist
if [ ! -d "/workspace/dbt_project" ]; then
    echo "🏗️  Initializing DBT project..."
    cd /workspace
    dbt init dbt_project --skip-profile-setup
    
    # Update dbt_project.yml
    cat > /workspace/dbt_project/dbt_project.yml << EOF
name: 'nyc_hiring_audit'
version: '1.0.0'
config-version: 2

profile: 'nyc_hiring_audit'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  nyc_hiring_audit:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view  
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts

seeds:
  nyc_hiring_audit:
    +schema: seeds

vars:
  # DBT variables
  start_date: '2024-01-01'
  end_date: '2025-12-31'
EOF
fi

# Set up git hooks for code quality
echo "🔧 Setting up development tools..."

# Configure git (if not already configured)
if ! git config user.name > /dev/null; then
    echo "⚠️  Please configure git:"
    echo "git config --global user.name 'Your Name'"
    echo "git config --global user.email 'your.email@example.com'"
fi

# Set permissions
chmod +x /workspace/scripts/*.sh 2>/dev/null || true

echo "✅ Development environment setup complete!"
echo ""
echo "🎯 Next steps:"
echo "1. Wait for PostgreSQL to be ready"
echo "2. Run: dbt debug (to test connection)"
echo "3. Run: dbt seed (to load reference data)"
echo "4. Run: dbt run (to build models)"
echo "5. Run: dbt test (to run data tests)"
echo ""
echo "🔗 Available services:"
echo "- PostgreSQL: localhost:5432"
echo "- Jupyter Lab: jupyter lab --port 8888 --no-browser --allow-root"
echo "- DBT docs: dbt docs generate && dbt docs serve --port 8000"
