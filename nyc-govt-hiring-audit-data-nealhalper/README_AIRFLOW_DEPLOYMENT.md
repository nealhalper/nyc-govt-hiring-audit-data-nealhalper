# NYC Government Hiring Audit Data Pipeline

A comprehensive data engineering pipeline using Apache Airflow to orchestrate the extraction, transformation, and loading of NYC government hiring data with advanced fuzzy matching and duration analysis capabilities.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Airflow DAG   │    │   PostgreSQL    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ NYC Payroll API │───▶│ Data Extraction │───▶│ fact_job_       │
│ NYC Jobs API    │    │                 │    │   postings      │
│ Lighthouse Data │    │ Data Processing │    │                 │
└─────────────────┘    │                 │    │ fact_posting_   │
                       │ Fuzzy Matching  │    │   duration      │
┌─────────────────┐    │                 │    └─────────────────┘
│   Docker Stack │    │ Validation &    │    
├─────────────────┤    │ Reporting       │    ┌─────────────────┐
│ Airflow Web UI  │    └─────────────────┘    │   DataGrip      │
│ PostgreSQL      │                           │   Connection    │
│ Redis Cache     │                           └─────────────────┘
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux)
- **Docker Compose** (included with Docker Desktop)
- **Git**
- **Python 3.11+** (for local development)

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd nyc-govt-hiring-audit-data-nealhalper

# Copy environment template
cp .env.example .env

# Edit .env file with your API credentials
```

### 2. Deploy Infrastructure

**Windows:**
```cmd
deploy.bat
```

**Linux/Mac:**
```bash
chmod +x deploy.sh
./deploy.sh
```

### 3. Access Services

- **Airflow Web UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`

- **PostgreSQL Database**: `localhost:5432`
  - Database: `nyc_hiring_audit`
  - Username: `airflow_user`
  - Password: `airflow_password`

## 📊 Pipeline Tasks

### Task 1: Setup Database Tables
- **Task ID**: `setup_db_tables`
- **Type**: PostgreSQL Operator
- **Description**: Creates fact tables and indexes from `audit_schema.sql`

### Task 2: Download Raw Data
- **Task ID**: `download_raw_data`
- **Type**: Python Operator
- **Description**: Downloads data from NYC APIs and Google Sheets
- **Retries**: 3 with exponential backoff

### Task 3: Process and Match Payroll Jobs
- **Task ID**: `process_and_match_payroll_jobs`
- **Type**: Python Operator
- **Description**: 
  - Processes payroll data with salary standardization
  - Processes job postings with fuzzy matching (≥85% threshold)
  - Loads results to `fact_2024_2025_job_postings`

### Task 4: Process Lighthouse Data
- **Task ID**: `process_lighthouse_data`
- **Type**: Python Operator
- **Description**: Cleans and standardizes Lighthouse benchmark data

### Task 5: Generate Posting Duration Dataset
- **Task ID**: `generate_posting_duration_dataset`
- **Type**: Python Operator
- **Description**: 
  - Matches job postings with Lighthouse benchmarks
  - Calculates duration variance analysis
  - Loads results to `fact_2024_2025_posting_duration`

### Task 6: Validation and Reporting
- **Task ID**: `basic_validation_and_reporting`
- **Type**: Python Operator
- **Description**: Runs comprehensive data quality checks and generates reports

## 🗃️ Database Schema

### fact_2024_2025_job_postings
```sql
-- Core job posting data with payroll matching
job_id (VARCHAR) PRIMARY KEY
agency, business_title, civil_service_title
salary_range_from, salary_range_to, salary_frequency
posting_date, posting_duration_days
best_payroll_match, best_match_score
payroll_salary_min, payroll_salary_max  -- Only for matches ≥85%
```

### fact_2024_2025_posting_duration
```sql
-- Duration analysis with Lighthouse benchmarks
duration_analysis_id (SERIAL) PRIMARY KEY
job_id (FOREIGN KEY)
actual_posting_duration_days, lighthouse_median_posting_duration
duration_variance, duration_variance_pct
lighthouse_match_title, lighthouse_match_score
lighthouse_total_postings, lighthouse_median_salary
```

## 🔧 Configuration

### Environment Variables (.env)
```env
NYC_PAYROLL_API_URL=https://data.cityofnewyork.us/resource/k397-673e.json
NYC_JOBS_POSTING_API_URL=https://data.cityofnewyork.us/resource/kpav-sd4t.json
LIGHTHOUSE_CREDENTIALS=./lighthouse-data-469114-a9319e12d65e.json
PIPELINE_ROW_LIMIT=50000
```

### Airflow Variables
Set in Airflow Web UI → Admin → Variables:
- `pipeline_row_limit`: Maximum rows to process (default: 50000)

### Docker Configuration
- **Airflow**: LocalExecutor with PostgreSQL backend
- **PostgreSQL**: Version 15 with persistent volumes
- **Redis**: For Airflow caching and task queuing

## 📈 Monitoring and Logging

### Airflow Web UI Features
- **DAG View**: Visual pipeline representation
- **Task Logs**: Detailed execution logs with error handling
- **Task Duration**: Performance monitoring
- **Retry Logic**: Automatic retry with exponential backoff

### Logging Levels
- **INFO**: Standard operation logs
- **WARNING**: Data quality issues (non-critical)
- **ERROR**: Task failures and critical issues
- **DEBUG**: Detailed processing information

### Key Metrics Tracked
- Record counts at each stage
- Fuzzy matching quality scores
- Data completeness percentages
- Processing duration times
- Error rates and retry attempts

## 🎯 Data Quality Features

### Fuzzy Matching Thresholds
- **Payroll Matching**: ≥85% similarity score
- **Lighthouse Matching**: ≥70% similarity score (configurable)

### Validation Checks
- **Completeness**: Agency, title, date field coverage
- **Integrity**: Foreign key relationships
- **Business Logic**: Salary ranges, duration limits
- **Anomaly Detection**: Outlier identification

### Error Handling
- **Retry Logic**: 3 attempts with exponential backoff
- **Graceful Degradation**: Continue processing on non-critical errors
- **Data Quality Alerts**: Automatic notifications for critical issues

## 🔄 Development Workflow

### Local Development
```bash
# Install Python dependencies
pip install -r requirements.txt

# Run individual processors
python data_processing/citywide_payroll_processor.py
python data_processing/jobs_nyc_posting_processor.py
python data_processing/lighthouse_processor.py

# Run full pipeline locally
python run_data_pipeline.py
```

### Testing
```bash
# Run validation scripts
python airflow/scripts/validation_scripts.py

# Check data quality
python -c "from airflow.scripts.validation_scripts import validate_pipeline_execution; validate_pipeline_execution()"
```

## 🔗 DataGrip Integration

### Connection Settings
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `nyc_hiring_audit`
- **Username**: `airflow_user`
- **Password**: `airflow_password`
- **JDBC URL**: `jdbc:postgresql://localhost:5432/nyc_hiring_audit`

### Useful Queries
```sql
-- High-quality payroll matches
SELECT * FROM vw_high_quality_payroll_matches LIMIT 100;

-- Duration variance analysis
SELECT * FROM vw_posting_duration_analysis LIMIT 100;

-- Pipeline summary statistics
SELECT * FROM vw_audit_summary_stats;
```

## 🛠️ Maintenance

### Daily Operations
- Monitor DAG execution in Airflow Web UI
- Check logs for any warnings or errors
- Verify data quality metrics

### Weekly Tasks
- Review summary statistics
- Check disk usage for Docker volumes
- Update row limits if needed

### Troubleshooting
```bash
# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres

# Restart services
docker-compose restart airflow-scheduler
docker-compose restart airflow-webserver

# Complete reset
docker-compose down -v
./deploy.sh  # or deploy.bat
```

### Backup and Recovery
```bash
# Backup database
docker-compose exec postgres pg_dump -U airflow_user nyc_hiring_audit > backup.sql

# Restore database
docker-compose exec -T postgres psql -U airflow_user nyc_hiring_audit < backup.sql
```

## 📋 Pipeline Schedule

- **Default Schedule**: Daily at midnight
- **Catchup**: Disabled (only run current date)
- **Max Active Runs**: 1 (prevents overlapping executions)
- **Timeout**: 6 hours per DAG run

## 🎛️ Advanced Configuration

### Scaling Options
- Switch to CeleryExecutor for distributed processing
- Add worker nodes for parallel task execution
- Configure Redis Cluster for high availability

### Security Enhancements
- Enable RBAC in Airflow
- Use Kubernetes Secrets for credentials
- Configure SSL for database connections
- Set up VPN for production deployments

### Performance Tuning
- Adjust batch sizes in configuration
- Configure connection pools
- Enable query optimization in PostgreSQL
- Use SSD storage for Docker volumes

## 📚 Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [NYC Open Data API](https://dev.socrata.com/foundry/data.cityofnewyork.us/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with the validation scripts
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
