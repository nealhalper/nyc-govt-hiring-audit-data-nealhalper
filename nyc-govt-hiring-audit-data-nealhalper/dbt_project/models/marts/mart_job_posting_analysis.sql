-- Final mart: Job postings with salary analysis and payroll matching
-- Business-ready model for hiring audit analysis

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['agency_name'], 'type': 'btree'},
        {'columns': ['posting_date'], 'type': 'btree'},
        {'columns': ['salary_match_ratio'], 'type': 'btree'}
    ]
) }}

with job_postings as (
    select *
    from {{ ref('stg_job_postings') }}
    where posting_date >= '{{ var("start_date") }}'
      and posting_date <= '{{ var("end_date") }}'
),

salary_benchmarks as (
    select *
    from {{ ref('int_salary_benchmarks') }}
    where fiscal_year = extract(year from current_date)  -- Current year benchmarks
),

-- Fuzzy matching logic (simplified for DBT)
matched_postings as (
    select 
        jp.*,
        sb.min_base_salary as payroll_min_salary,
        sb.max_base_salary as payroll_max_salary,
        sb.median_base_salary as payroll_median_salary,
        sb.avg_base_salary as payroll_avg_salary,
        sb.employee_count as payroll_employee_count,
        sb.salary_band as payroll_salary_band,
        sb.is_high_variance_position,
        
        -- Calculate match ratio (simplified string similarity)
        case 
            when jp.agency_name_standardized = sb.agency_name 
                 and jp.title_description = sb.title_description
            then 1.0
            when jp.agency_name_standardized = sb.agency_name
                 and similarity(jp.title_description, sb.title_description) > 0.8
            then similarity(jp.title_description, sb.title_description)
            else 0.0
        end as salary_match_ratio
        
    from job_postings jp
    left join salary_benchmarks sb
        on jp.agency_name_standardized = sb.agency_name
        and (jp.title_description = sb.title_description 
             or similarity(jp.title_description, sb.title_description) > {{ var('fuzzy_match_threshold') }})
),

final_analysis as (
    select 
        -- Job posting information
        job_id,
        agency_name,
        agency_name_standardized,
        business_title,
        title_description,
        posting_date,
        application_deadline,
        posting_duration_days,
        work_location,
        employment_type,
        career_level,
        
        -- Posted salary information
        salary_range_from as posted_salary_min,
        salary_range_to as posted_salary_max,
        salary_midpoint as posted_salary_midpoint,
        
        -- Payroll benchmark information (only for high-confidence matches)
        case 
            when salary_match_ratio >= {{ var('fuzzy_match_threshold') }}
            then payroll_min_salary 
            else null 
        end as benchmark_salary_min,
        
        case 
            when salary_match_ratio >= {{ var('fuzzy_match_threshold') }}
            then payroll_max_salary 
            else null 
        end as benchmark_salary_max,
        
        case 
            when salary_match_ratio >= {{ var('fuzzy_match_threshold') }}
            then payroll_median_salary 
            else null 
        end as benchmark_salary_median,
        
        case 
            when salary_match_ratio >= {{ var('fuzzy_match_threshold') }}
            then payroll_employee_count 
            else null 
        end as benchmark_employee_count,
        
        salary_match_ratio,
        
        -- Salary comparison analysis
        case 
            when salary_match_ratio >= {{ var('fuzzy_match_threshold') }}
                 and salary_midpoint is not null 
                 and payroll_median_salary is not null
            then (salary_midpoint - payroll_median_salary) / payroll_median_salary
            else null
        end as salary_variance_pct,
        
        -- Data quality flags
        case when salary_match_ratio >= {{ var('fuzzy_match_threshold') }} then true else false end as has_salary_benchmark,
        case when salary_range_from is not null and salary_range_to is not null then true else false end as has_posted_salary,
        case when posting_duration_days is not null and posting_duration_days > 0 then true else false end as has_valid_posting_period,
        
        -- Business flags
        case 
            when salary_match_ratio >= {{ var('fuzzy_match_threshold') }}
                 and abs((salary_midpoint - payroll_median_salary) / payroll_median_salary) > 0.20
            then true 
            else false 
        end as is_salary_outlier,
        
        payroll_salary_band,
        is_high_variance_position,
        
        -- Metadata
        current_timestamp as analysis_timestamp
        
    from matched_postings
)

select * from final_analysis
