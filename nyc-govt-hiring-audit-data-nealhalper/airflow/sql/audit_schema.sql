-- NYC Government Hiring Audit Database Schema
-- Creates tables for job postings and posting duration analysis

-- Drop existing tables if they exist (for development/testing)
DROP TABLE IF EXISTS fact_2024_2025_posting_duration CASCADE;
DROP TABLE IF EXISTS fact_2024_2025_job_postings CASCADE;

-- Create fact table for job postings with payroll matching
CREATE TABLE fact_2024_2025_job_postings (
    -- Primary identifiers
    job_id VARCHAR(50) PRIMARY KEY,
    
    -- Job posting details
    agency VARCHAR(200),
    posting_type VARCHAR(100),
    number_of_positions INTEGER,
    business_title VARCHAR(500),
    civil_service_title VARCHAR(500),
    job_category VARCHAR(200),
    full_time_part_time_indicator VARCHAR(50),
    career_level VARCHAR(100),
    
    -- Salary information from job posting
    salary_range_from DECIMAL(12,2),
    salary_range_to DECIMAL(12,2),
    salary_frequency VARCHAR(50),
    
    -- Location and timing
    work_location VARCHAR(500),
    posting_date TIMESTAMP,
    posting_duration_days INTEGER,
    
    -- Fuzzy matching with payroll
    fuzzy_match_string VARCHAR(500),
    best_payroll_match VARCHAR(500),
    best_match_score DECIMAL(5,4),
    
    -- Payroll salary data (only for high-quality matches >= 85%)
    payroll_salary_min DECIMAL(12,2),
    payroll_salary_max DECIMAL(12,2),
    
    -- Audit timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create fact table for posting duration analysis with lighthouse matching
CREATE TABLE fact_2024_2025_posting_duration (
    -- Primary key
    duration_analysis_id SERIAL PRIMARY KEY,
    
    -- Job posting reference
    job_id VARCHAR(50),
    agency VARCHAR(200),
    business_title VARCHAR(500),
    
    -- Duration metrics
    actual_posting_duration_days INTEGER,
    lighthouse_median_posting_duration DECIMAL(8,2),
    duration_variance INTEGER,
    duration_variance_pct DECIMAL(8,2),
    
    -- Lighthouse matching
    lighthouse_match_title VARCHAR(500),
    lighthouse_match_score DECIMAL(5,4),
    lighthouse_total_postings INTEGER,
    lighthouse_median_salary DECIMAL(12,2),
    lighthouse_top_skills TEXT,
    lighthouse_education_requirements TEXT,
    lighthouse_experience_level VARCHAR(200),
    lighthouse_salary_category VARCHAR(50),
    lighthouse_posting_intensity DECIMAL(8,4),
    
    -- Audit timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key reference
    FOREIGN KEY (job_id) REFERENCES fact_2024_2025_job_postings(job_id)
);

-- Create indexes for performance
CREATE INDEX idx_job_postings_agency ON fact_2024_2025_job_postings(agency);
CREATE INDEX idx_job_postings_posting_date ON fact_2024_2025_job_postings(posting_date);
CREATE INDEX idx_job_postings_match_score ON fact_2024_2025_job_postings(best_match_score);
CREATE INDEX idx_job_postings_business_title ON fact_2024_2025_job_postings(business_title);

CREATE INDEX idx_duration_agency ON fact_2024_2025_posting_duration(agency);
CREATE INDEX idx_duration_variance ON fact_2024_2025_posting_duration(duration_variance_pct);
CREATE INDEX idx_duration_lighthouse_score ON fact_2024_2025_posting_duration(lighthouse_match_score);

-- Create views for analysis
CREATE VIEW vw_high_quality_payroll_matches AS
SELECT 
    job_id,
    agency,
    business_title,
    salary_range_from,
    salary_range_to,
    payroll_salary_min,
    payroll_salary_max,
    best_match_score,
    posting_duration_days
FROM fact_2024_2025_job_postings
WHERE best_match_score >= 0.85
    AND payroll_salary_min IS NOT NULL
    AND payroll_salary_max IS NOT NULL;

CREATE VIEW vw_posting_duration_analysis AS
SELECT 
    pd.job_id,
    pd.agency,
    pd.business_title,
    pd.actual_posting_duration_days,
    pd.lighthouse_median_posting_duration,
    pd.duration_variance_pct,
    pd.lighthouse_match_score,
    jp.salary_range_from,
    jp.salary_range_to,
    jp.number_of_positions
FROM fact_2024_2025_posting_duration pd
JOIN fact_2024_2025_job_postings jp ON pd.job_id = jp.job_id
WHERE pd.lighthouse_match_score >= 0.7;

-- Create summary statistics view
CREATE VIEW vw_audit_summary_stats AS
SELECT 
    -- Job postings summary
    (SELECT COUNT(*) FROM fact_2024_2025_job_postings) as total_job_postings,
    (SELECT COUNT(*) FROM fact_2024_2025_job_postings WHERE best_match_score >= 0.85) as high_quality_payroll_matches,
    (SELECT AVG(best_match_score) FROM fact_2024_2025_job_postings WHERE best_match_score IS NOT NULL) as avg_payroll_match_score,
    
    -- Duration analysis summary
    (SELECT COUNT(*) FROM fact_2024_2025_posting_duration) as total_duration_records,
    (SELECT COUNT(*) FROM fact_2024_2025_posting_duration WHERE lighthouse_match_score >= 0.7) as good_lighthouse_matches,
    (SELECT AVG(duration_variance_pct) FROM fact_2024_2025_posting_duration WHERE duration_variance_pct IS NOT NULL) as avg_duration_variance_pct,
    
    -- Data quality metrics
    (SELECT COUNT(DISTINCT agency) FROM fact_2024_2025_job_postings) as unique_agencies,
    (SELECT COUNT(DISTINCT business_title) FROM fact_2024_2025_job_postings) as unique_job_titles;

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO airflow_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO airflow_user;

-- Log schema creation
INSERT INTO public.schema_audit_log (schema_version, created_at, description)
VALUES ('1.0', CURRENT_TIMESTAMP, 'Initial schema creation for NYC hiring audit')
ON CONFLICT DO NOTHING;

-- Create audit log table if it doesn't exist
CREATE TABLE IF NOT EXISTS public.schema_audit_log (
    id SERIAL PRIMARY KEY,
    schema_version VARCHAR(10),
    created_at TIMESTAMP,
    description TEXT
);

COMMENT ON TABLE fact_2024_2025_job_postings IS 'NYC job postings for 2024-2025 with payroll matching data';
COMMENT ON TABLE fact_2024_2025_posting_duration IS 'Posting duration analysis with Lighthouse benchmark data';
