-- Intermediate model: Salary benchmarking by agency and title
-- Calculates statistical salary ranges for each agency-title combination

{{ config(materialized='view') }}

with payroll_active_employees as (
    select *
    from {{ ref('stg_payroll_data') }}
    where is_active_employee = true
      and base_salary is not null
      and base_salary > 0
),

salary_stats_by_agency_title as (
    select
        agency_name_standardized as agency_name,
        title_description,
        fiscal_year,
        
        -- Salary statistics
        count(*) as employee_count,
        min(base_salary) as min_base_salary,
        max(base_salary) as max_base_salary,
        avg(base_salary)::numeric(10,2) as avg_base_salary,
        percentile_cont(0.5) within group (order by base_salary)::numeric(10,2) as median_base_salary,
        percentile_cont(0.25) within group (order by base_salary)::numeric(10,2) as q1_base_salary,
        percentile_cont(0.75) within group (order by base_salary)::numeric(10,2) as q3_base_salary,
        
        -- Total compensation statistics
        avg(total_compensation)::numeric(10,2) as avg_total_compensation,
        min(total_compensation) as min_total_compensation,
        max(total_compensation) as max_total_compensation,
        
        -- Variability measures
        stddev(base_salary)::numeric(10,2) as salary_std_dev,
        (stddev(base_salary) / avg(base_salary))::numeric(4,3) as salary_coefficient_variation,
        
        -- Working hours analysis
        avg(regular_hours)::numeric(8,2) as avg_regular_hours,
        avg(estimated_hourly_rate)::numeric(8,2) as avg_hourly_rate
        
    from payroll_active_employees
    group by 
        agency_name_standardized,
        title_description,
        fiscal_year
    having count(*) >= 3  -- Only include titles with sufficient sample size
),

add_salary_bands as (
    select 
        *,
        
        -- Create salary bands based on quartiles
        case 
            when avg_base_salary < 40000 then 'Entry Level'
            when avg_base_salary < 60000 then 'Mid Level'
            when avg_base_salary < 80000 then 'Senior Level'
            when avg_base_salary < 120000 then 'Manager Level'
            else 'Executive Level'
        end as salary_band,
        
        -- Calculate acceptable salary range (±20% of median)
        (median_base_salary * 0.8)::numeric(10,2) as salary_range_lower,
        (median_base_salary * 1.2)::numeric(10,2) as salary_range_upper,
        
        -- Flag high-variance positions
        case 
            when salary_coefficient_variation > {{ var('salary_variance_threshold') }}
            then true 
            else false 
        end as is_high_variance_position
        
    from salary_stats_by_agency_title
)

select * from add_salary_bands
