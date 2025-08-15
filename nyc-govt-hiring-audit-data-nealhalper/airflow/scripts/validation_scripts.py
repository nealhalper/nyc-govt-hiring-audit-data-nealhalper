"""
Validation and Reporting Scripts for NYC Hiring Audit Pipeline
"""

import logging
import pandas as pd
from sqlalchemy import create_engine
from typing import Dict, Any

logger = logging.getLogger(__name__)

def get_database_connection():
    """Get database connection for validation queries"""
    connection_string = "postgresql://airflow_user:airflow_password@postgres:5432/nyc_hiring_audit"
    return create_engine(connection_string)

def run_data_quality_checks() -> Dict[str, Any]:
    """Run comprehensive data quality validation checks"""
    
    engine = get_database_connection()
    validation_results = {}
    
    logger.info("Starting data quality validation checks...")
    
    # Basic record counts
    job_postings_count = pd.read_sql(
        "SELECT COUNT(*) as count FROM fact_2024_2025_job_postings", engine
    ).iloc[0]['count']
    
    duration_records_count = pd.read_sql(
        "SELECT COUNT(*) as count FROM fact_2024_2025_posting_duration", engine
    ).iloc[0]['count']
    
    validation_results.update({
        'total_job_postings': job_postings_count,
        'total_duration_records': duration_records_count
    })
    
    # Data completeness checks
    completeness_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN agency IS NOT NULL THEN 1 END) as agency_complete,
        COUNT(CASE WHEN business_title IS NOT NULL THEN 1 END) as title_complete,
        COUNT(CASE WHEN posting_date IS NOT NULL THEN 1 END) as date_complete,
        COUNT(CASE WHEN salary_range_from IS NOT NULL THEN 1 END) as salary_from_complete,
        COUNT(CASE WHEN salary_range_to IS NOT NULL THEN 1 END) as salary_to_complete
    FROM fact_2024_2025_job_postings
    """
    
    completeness_results = pd.read_sql(completeness_query, engine).iloc[0]
    
    # Calculate completeness percentages
    total_records = completeness_results['total_records']
    if total_records > 0:
        validation_results.update({
            'agency_completeness_pct': (completeness_results['agency_complete'] / total_records) * 100,
            'title_completeness_pct': (completeness_results['title_complete'] / total_records) * 100,
            'date_completeness_pct': (completeness_results['date_complete'] / total_records) * 100,
            'salary_from_completeness_pct': (completeness_results['salary_from_complete'] / total_records) * 100,
            'salary_to_completeness_pct': (completeness_results['salary_to_complete'] / total_records) * 100
        })
    
    # Matching quality checks
    matching_quality_query = """
    SELECT 
        COUNT(*) as total_jobs,
        COUNT(CASE WHEN best_match_score >= 0.85 THEN 1 END) as high_quality_payroll_matches,
        COUNT(CASE WHEN best_match_score >= 0.70 THEN 1 END) as good_payroll_matches,
        COUNT(CASE WHEN payroll_salary_min IS NOT NULL THEN 1 END) as jobs_with_payroll_salary,
        AVG(best_match_score) as avg_match_score
    FROM fact_2024_2025_job_postings
    WHERE best_match_score IS NOT NULL
    """
    
    matching_results = pd.read_sql(matching_quality_query, engine).iloc[0]
    validation_results.update({
        'high_quality_payroll_matches': matching_results['high_quality_payroll_matches'],
        'good_payroll_matches': matching_results['good_payroll_matches'],
        'jobs_with_payroll_salary': matching_results['jobs_with_payroll_salary'],
        'avg_payroll_match_score': float(matching_results['avg_match_score']) if matching_results['avg_match_score'] else 0
    })
    
    # Duration analysis quality checks
    duration_quality_query = """
    SELECT 
        COUNT(*) as total_duration_records,
        COUNT(CASE WHEN lighthouse_match_score >= 0.80 THEN 1 END) as high_quality_lighthouse_matches,
        COUNT(CASE WHEN lighthouse_match_score >= 0.70 THEN 1 END) as good_lighthouse_matches,
        AVG(lighthouse_match_score) as avg_lighthouse_match_score,
        AVG(duration_variance_pct) as avg_duration_variance_pct,
        MIN(duration_variance_pct) as min_duration_variance_pct,
        MAX(duration_variance_pct) as max_duration_variance_pct
    FROM fact_2024_2025_posting_duration
    WHERE lighthouse_match_score IS NOT NULL
    """
    
    duration_results = pd.read_sql(duration_quality_query, engine).iloc[0]
    validation_results.update({
        'high_quality_lighthouse_matches': duration_results['high_quality_lighthouse_matches'],
        'good_lighthouse_matches': duration_results['good_lighthouse_matches'],
        'avg_lighthouse_match_score': float(duration_results['avg_lighthouse_match_score']) if duration_results['avg_lighthouse_match_score'] else 0,
        'avg_duration_variance_pct': float(duration_results['avg_duration_variance_pct']) if duration_results['avg_duration_variance_pct'] else 0
    })
    
    # Referential integrity checks
    integrity_query = """
    SELECT 
        COUNT(*) as orphaned_duration_records
    FROM fact_2024_2025_posting_duration pd
    LEFT JOIN fact_2024_2025_job_postings jp ON pd.job_id = jp.job_id
    WHERE jp.job_id IS NULL
    """
    
    integrity_results = pd.read_sql(integrity_query, engine).iloc[0]
    validation_results['orphaned_duration_records'] = integrity_results['orphaned_duration_records']
    
    # Business logic validation
    business_validation_query = """
    SELECT 
        COUNT(*) as invalid_salary_ranges,
        COUNT(CASE WHEN posting_duration_days < 0 THEN 1 END) as negative_durations,
        COUNT(CASE WHEN posting_duration_days > 365 THEN 1 END) as excessive_durations
    FROM fact_2024_2025_job_postings
    WHERE salary_range_from > salary_range_to 
       OR posting_duration_days < 0 
       OR posting_duration_days > 365
    """
    
    business_results = pd.read_sql(business_validation_query, engine).iloc[0]
    validation_results.update({
        'invalid_salary_ranges': business_results['invalid_salary_ranges'],
        'negative_durations': business_results['negative_durations'],
        'excessive_durations': business_results['excessive_durations']
    })
    
    engine.dispose()
    
    logger.info("Data quality validation checks completed")
    return validation_results

def generate_summary_report() -> Dict[str, Any]:
    """Generate comprehensive summary report"""
    
    engine = get_database_connection()
    
    logger.info("Generating summary report...")
    
    # Agency breakdown
    agency_breakdown = pd.read_sql("""
        SELECT 
            agency,
            COUNT(*) as job_count,
            COUNT(CASE WHEN best_match_score >= 0.85 THEN 1 END) as high_quality_matches,
            AVG(posting_duration_days) as avg_posting_duration,
            AVG(salary_range_from) as avg_min_salary,
            AVG(salary_range_to) as avg_max_salary
        FROM fact_2024_2025_job_postings
        GROUP BY agency
        ORDER BY job_count DESC
        LIMIT 10
    """, engine)
    
    # Top job titles
    top_titles = pd.read_sql("""
        SELECT 
            business_title,
            COUNT(*) as posting_count,
            AVG(best_match_score) as avg_match_score,
            AVG(posting_duration_days) as avg_duration
        FROM fact_2024_2025_job_postings
        GROUP BY business_title
        ORDER BY posting_count DESC
        LIMIT 10
    """, engine)
    
    # Salary analysis
    salary_stats = pd.read_sql("""
        SELECT 
            AVG(salary_range_from) as avg_min_salary,
            AVG(salary_range_to) as avg_max_salary,
            AVG(payroll_salary_min) as avg_payroll_min,
            AVG(payroll_salary_max) as avg_payroll_max,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_range_from) as median_min_salary,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_range_to) as median_max_salary
        FROM fact_2024_2025_job_postings
        WHERE salary_range_from IS NOT NULL AND salary_range_to IS NOT NULL
    """, engine)
    
    # Duration variance analysis
    duration_variance_stats = pd.read_sql("""
        SELECT 
            AVG(duration_variance_pct) as avg_variance_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_variance_pct) as median_variance_pct,
            COUNT(CASE WHEN ABS(duration_variance_pct) > 50 THEN 1 END) as significant_variances,
            COUNT(CASE WHEN duration_variance_pct > 0 THEN 1 END) as longer_than_expected,
            COUNT(CASE WHEN duration_variance_pct < 0 THEN 1 END) as shorter_than_expected
        FROM fact_2024_2025_posting_duration
        WHERE duration_variance_pct IS NOT NULL
    """, engine)
    
    engine.dispose()
    
    summary_report = {
        'agency_breakdown': agency_breakdown.to_dict('records'),
        'top_job_titles': top_titles.to_dict('records'),
        'salary_statistics': salary_stats.iloc[0].to_dict(),
        'duration_variance_statistics': duration_variance_stats.iloc[0].to_dict()
    }
    
    logger.info("Summary report generation completed")
    return summary_report

def validate_pipeline_execution():
    """Main validation function to be called by Airflow"""
    
    logger.info("=== STARTING PIPELINE VALIDATION ===")
    
    try:
        # Run data quality checks
        quality_results = run_data_quality_checks()
        
        # Generate summary report
        summary_report = generate_summary_report()
        
        # Log key metrics
        logger.info("=== DATA QUALITY RESULTS ===")
        for key, value in quality_results.items():
            logger.info(f"{key}: {value}")
        
        logger.info("=== SUMMARY STATISTICS ===")
        logger.info(f"Top agencies by job count: {len(summary_report['agency_breakdown'])}")
        logger.info(f"Top job titles analyzed: {len(summary_report['top_job_titles'])}")
        
        # Validate critical thresholds
        critical_issues = []
        
        if quality_results['total_job_postings'] == 0:
            critical_issues.append("No job postings found in database")
        
        if quality_results['agency_completeness_pct'] < 95:
            critical_issues.append(f"Agency data completeness below threshold: {quality_results['agency_completeness_pct']:.1f}%")
        
        if quality_results['title_completeness_pct'] < 95:
            critical_issues.append(f"Job title data completeness below threshold: {quality_results['title_completeness_pct']:.1f}%")
        
        if quality_results['orphaned_duration_records'] > 0:
            critical_issues.append(f"Found {quality_results['orphaned_duration_records']} orphaned duration records")
        
        if critical_issues:
            logger.error("=== CRITICAL ISSUES FOUND ===")
            for issue in critical_issues:
                logger.error(issue)
            raise Exception(f"Pipeline validation failed: {'; '.join(critical_issues)}")
        
        logger.info("=== PIPELINE VALIDATION COMPLETED SUCCESSFULLY ===")
        return {
            'status': 'success',
            'quality_results': quality_results,
            'summary_report': summary_report
        }
    
    except Exception as e:
        logger.error(f"Pipeline validation failed: {str(e)}")
        raise

if __name__ == "__main__":
    # For standalone testing
    logging.basicConfig(level=logging.INFO)
    validate_pipeline_execution()
