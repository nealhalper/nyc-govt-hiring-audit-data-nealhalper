"""
NYC Government Hiring Audit Data Pipeline
Airflow DAG for orchestrating data acquisition, processing, and database loading

Author: Data Engineering Team
Date: 2025-08-15
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import logging
import sys
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import polars as pl
import pandas as pd

# Add project root to Python path
PROJECT_ROOT = Path('/opt/airflow/workspace')
sys.path.append(str(PROJECT_ROOT))

# Import our pipeline modules
from data_acquisition.citywide_payroll_data import NYCPayrollDataFetcher
from data_acquisition.jobs_nyc_posting import NYCJobsPostingDataFetcher
from data_acquisition.lighthouse_data import LighthouseDataProcessor
from data_processing.citywide_payroll_processor import PayrollDataProcessor
from data_processing.jobs_nyc_posting_processor import JobsPostingProcessor
from data_processing.lighthouse_processor import LighthouseProcessor
from config import Config

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_retry_delay': timedelta(minutes=30),
    'retry_exponential_backoff': True,
}

# DAG definition
dag = DAG(
    'nyc_hiring_audit_pipeline',
    default_args=default_args,
    description='NYC Government Hiring Audit Data Pipeline',
    schedule_interval=timedelta(days=1),  # Run daily
    max_active_runs=1,
    catchup=False,
    tags=['nyc', 'hiring', 'audit', 'etl']
)

def setup_error_handling(func):
    """Decorator for consistent error handling and logging"""
    def wrapper(*args, **kwargs):
        task_id = kwargs.get('task_id', func.__name__)
        logger.info(f"Starting task: {task_id}")
        
        try:
            result = func(*args, **kwargs)
            logger.info(f"Successfully completed task: {task_id}")
            return result
            
        except Exception as e:
            error_msg = f"Task {task_id} failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AirflowException(error_msg)
    
    return wrapper

def _load_jobs_to_database(df: pl.DataFrame):
    """Helper function to load jobs data to PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Convert to pandas for easier database insertion
    pandas_df = df.to_pandas()
    
    # Clear existing data (for development - modify for production)
    postgres_hook.run("DELETE FROM fact_2024_2025_job_postings")
    
    # Insert data in batches
    batch_size = 1000
    total_rows = len(pandas_df)
    
    for i in range(0, total_rows, batch_size):
        batch = pandas_df.iloc[i:i+batch_size]
        
        # Prepare insert statement
        columns = ', '.join(batch.columns)
        values = []
        
        for _, row in batch.iterrows():
            row_values = []
            for value in row:
                if pd.isna(value):
                    row_values.append('NULL')
                elif isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    row_values.append(f"'{escaped_value}'")
                else:
                    row_values.append(str(value))
            values.append(f"({', '.join(row_values)})")
        
        insert_sql = f"""
            INSERT INTO fact_2024_2025_job_postings ({columns})
            VALUES {', '.join(values)}
        """
        
        postgres_hook.run(insert_sql)
        logger.info(f"Inserted batch {i//batch_size + 1}/{(total_rows//batch_size) + 1}")

def _load_duration_to_database(df: pl.DataFrame):
    """Helper function to load duration data to PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Convert to pandas for easier database insertion
    pandas_df = df.to_pandas()
    
    # Clear existing data (for development - modify for production)
    postgres_hook.run("DELETE FROM fact_2024_2025_posting_duration")
    
    # Insert data in batches
    batch_size = 1000
    total_rows = len(pandas_df)
    
    for i in range(0, total_rows, batch_size):
        batch = pandas_df.iloc[i:i+batch_size]
        
        # Prepare insert statement (excluding auto-generated ID)
        columns = [col for col in batch.columns if col != 'duration_analysis_id']
        column_names = ', '.join(columns)
        values = []
        
        for _, row in batch.iterrows():
            row_values = []
            for col in columns:
                value = row[col]
                if pd.isna(value):
                    row_values.append('NULL')
                elif isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    row_values.append(f"'{escaped_value}'")
                else:
                    row_values.append(str(value))
            values.append(f"({', '.join(row_values)})")
        
        insert_sql = f"""
            INSERT INTO fact_2024_2025_posting_duration ({column_names})
            VALUES {', '.join(values)}
        """
        
        postgres_hook.run(insert_sql)
        logger.info(f"Inserted duration batch {i//batch_size + 1}/{(total_rows//batch_size) + 1}")

@setup_error_handling
def download_raw_data(**context):
    """
    Task 2: Download all raw data sources
    """
    logger.info("Starting raw data download...")
    
    results = {}
    
    # Download NYC Payroll Data
    logger.info("Downloading NYC Payroll data...")
    payroll_fetcher = NYCPayrollDataFetcher()
    payroll_df = payroll_fetcher.fetch_data(
        max_rows=Variable.get("pipeline_row_limit", default_var=50000),
        use_cache=True
    )
    results['payroll_rows'] = payroll_df.shape[0] if not payroll_df.is_empty() else 0
    
    # Download NYC Jobs Posting Data
    logger.info("Downloading NYC Jobs Posting data...")
    jobs_fetcher = NYCJobsPostingDataFetcher()
    jobs_df = jobs_fetcher.fetch_data(
        max_rows=Variable.get("pipeline_row_limit", default_var=50000),
        use_cache=True
    )
    results['jobs_rows'] = jobs_df.shape[0] if not jobs_df.is_empty() else 0
    
    # Download Lighthouse Data
    logger.info("Downloading Lighthouse data...")
    lighthouse_fetcher = LighthouseDataProcessor()
    lighthouse_fetcher.process_excel_file(use_cache=True)
    results['lighthouse_processed'] = True
    
    logger.info(f"Raw data download complete: {results}")
    return results

@setup_error_handling
def process_and_match_payroll_jobs(**context):
    """
    Task 3: Process payroll and jobs data, perform matching, load to DB
    """
    logger.info("Starting payroll and jobs processing with fuzzy matching...")
    
    # Process Payroll Data
    logger.info("Processing payroll data...")
    payroll_processor = PayrollDataProcessor()
    payroll_df = payroll_processor.process_payroll_data(use_cache=True)
    
    if payroll_df.is_empty():
        raise AirflowException("Payroll processing returned empty dataset")
    
    # Process Jobs Data
    logger.info("Processing jobs posting data...")
    jobs_processor = JobsPostingProcessor()
    jobs_df = jobs_processor.process_jobs_posting_data(use_cache=True)
    
    if jobs_df.is_empty():
        raise AirflowException("Jobs processing returned empty dataset")
    
    # Load to PostgreSQL
    logger.info("Loading job postings to database...")
    _load_jobs_to_database(jobs_df)
    
    results = {
        'payroll_processed_rows': payroll_df.shape[0],
        'jobs_processed_rows': jobs_df.shape[0],
        'high_quality_matches': jobs_df.filter(
            pl.col('best_match_score') >= 0.85
        ).shape[0] if 'best_match_score' in jobs_df.columns else 0
    }
    
    logger.info(f"Payroll and jobs processing complete: {results}")
    return results

@setup_error_handling
def process_lighthouse_data(**context):
    """
    Task 4: Process lighthouse data
    """
    logger.info("Processing Lighthouse data...")
    
    lighthouse_processor = LighthouseProcessor()
    lighthouse_df = lighthouse_processor.process_lighthouse_data(use_cache=True)
    
    if lighthouse_df.is_empty():
        raise AirflowException("Lighthouse processing returned empty dataset")
    
    results = {
        'lighthouse_processed_rows': lighthouse_df.shape[0],
        'unique_job_titles': lighthouse_df.select('lighthouse_job_title').n_unique()
    }
    
    logger.info(f"Lighthouse processing complete: {results}")
    return results

@setup_error_handling
def generate_posting_duration_dataset(**context):
    """
    Task 5: Generate posting duration dataset and load to DB
    """
    logger.info("Generating posting duration dataset...")
    
    # Load processed data
    jobs_files = list(Config.PROCESSED_DATA_DIRECTORY.glob("nyc_jobs_posting_processed_*.parquet"))
    lighthouse_files = list(Config.PROCESSED_DATA_DIRECTORY.glob("lighthouse_processed_*.parquet"))
    
    if not jobs_files:
        raise AirflowException("No processed jobs data found")
    if not lighthouse_files:
        raise AirflowException("No processed lighthouse data found")
    
    # Load most recent files
    jobs_df = pl.read_parquet(sorted(jobs_files)[-1])
    lighthouse_df = pl.read_parquet(sorted(lighthouse_files)[-1])
    
    # Generate combined dataset
    lighthouse_processor = LighthouseProcessor()
    combined_df = lighthouse_processor.generate_posting_duration_dataset(
        jobs_df, lighthouse_df
    )
    
    if combined_df.is_empty():
        raise AirflowException("Combined dataset generation failed")
    
    # Load to PostgreSQL
    logger.info("Loading posting duration data to database...")
    _load_duration_to_database(combined_df)
    
    results = {
        'combined_rows': combined_df.shape[0],
        'high_quality_lighthouse_matches': combined_df.filter(
            pl.col('lighthouse_match_score') >= 0.8
        ).shape[0] if 'lighthouse_match_score' in combined_df.columns else 0
    }
    
    logger.info(f"Posting duration dataset generation complete: {results}")
    return results

@setup_error_handling
def validate_and_report(**context):
    """
    Task 6: Run validation queries and generate summary report
    """
    logger.info("Running validation and generating report...")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Run validation queries
    validation_results = {}
    
    # Check record counts
    job_count = postgres_hook.get_first(
        "SELECT COUNT(*) FROM fact_2024_2025_job_postings"
    )[0]
    duration_count = postgres_hook.get_first(
        "SELECT COUNT(*) FROM fact_2024_2025_posting_duration"
    )[0]
    
    validation_results['total_job_postings'] = job_count
    validation_results['total_duration_records'] = duration_count
    
    # Check data quality
    high_quality_payroll = postgres_hook.get_first("""
        SELECT COUNT(*) FROM fact_2024_2025_job_postings 
        WHERE best_match_score >= 0.85 AND payroll_salary_min IS NOT NULL
    """)[0]
    
    good_lighthouse_matches = postgres_hook.get_first("""
        SELECT COUNT(*) FROM fact_2024_2025_posting_duration 
        WHERE lighthouse_match_score >= 0.7
    """)[0]
    
    validation_results['high_quality_payroll_matches'] = high_quality_payroll
    validation_results['good_lighthouse_matches'] = good_lighthouse_matches
    
    # Check for data anomalies
    avg_duration_variance = postgres_hook.get_first("""
        SELECT AVG(duration_variance_pct) FROM fact_2024_2025_posting_duration 
        WHERE duration_variance_pct IS NOT NULL
    """)[0]
    
    validation_results['avg_duration_variance_pct'] = float(avg_duration_variance) if avg_duration_variance else None
    
    # Validate referential integrity
    orphaned_duration_records = postgres_hook.get_first("""
        SELECT COUNT(*) FROM fact_2024_2025_posting_duration pd
        LEFT JOIN fact_2024_2025_job_postings jp ON pd.job_id = jp.job_id
        WHERE jp.job_id IS NULL
    """)[0]
    
    validation_results['orphaned_duration_records'] = orphaned_duration_records
    
    # Log summary statistics
    logger.info("=== PIPELINE VALIDATION SUMMARY ===")
    for key, value in validation_results.items():
        logger.info(f"{key}: {value}")
    
    # Raise alerts for critical issues
    if job_count == 0:
        raise AirflowException("No job postings found in database")
    if duration_count == 0:
        raise AirflowException("No duration records found in database")
    if orphaned_duration_records > 0:
        logger.warning(f"Found {orphaned_duration_records} orphaned duration records")
    
    logger.info("Validation and reporting complete")
    return validation_results

# Define tasks
setup_db_tables = PostgresOperator(
    task_id='setup_db_tables',
    postgres_conn_id='postgres_default',
    sql='sql/audit_schema.sql',
    dag=dag
)

download_raw_data_task = PythonOperator(
    task_id='download_raw_data',
    python_callable=download_raw_data,
    dag=dag
)

process_and_match_task = PythonOperator(
    task_id='process_and_match_payroll_jobs',
    python_callable=process_and_match_payroll_jobs,
    dag=dag
)

process_lighthouse_task = PythonOperator(
    task_id='process_lighthouse_data',
    python_callable=process_lighthouse_data,
    dag=dag
)

generate_duration_task = PythonOperator(
    task_id='generate_posting_duration_dataset',
    python_callable=generate_posting_duration_dataset,
    dag=dag
)

validate_and_report_task = PythonOperator(
    task_id='basic_validation_and_reporting',
    python_callable=validate_and_report,
    dag=dag
)

# Define task dependencies
setup_db_tables >> download_raw_data_task
download_raw_data_task >> [process_and_match_task, process_lighthouse_task]
[process_and_match_task, process_lighthouse_task] >> generate_duration_task
generate_duration_task >> validate_and_report_task
