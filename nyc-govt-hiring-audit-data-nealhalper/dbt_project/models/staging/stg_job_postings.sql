-- Staging model for NYC job postings data
-- Cleans and standardizes raw job posting data from jobs.nyc.gov

{{ config(materialized='view') }}

with source_data as (
    select * from {{ ref('raw_job_postings') }}
),

cleaned_data as (
    select
        -- Identifiers
        job_id::varchar as job_id,
        agency::varchar as agency_name,
        
        -- Job details
        {{ dbt.safe_cast("business_title", dbt.type_string()) }} as business_title,
        {{ dbt.safe_cast("title_description", dbt.type_string()) }} as title_description,
        {{ dbt.safe_cast("job_description", dbt.type_string()) }} as job_description,
        
        -- Salary information
        case 
            when salary_range_from is not null and salary_range_from > 0 
            then salary_range_from::numeric 
            else null 
        end as salary_range_from,
        
        case 
            when salary_range_to is not null and salary_range_to > 0 
            then salary_range_to::numeric 
            else null 
        end as salary_range_to,
        
        -- Dates
        {{ dbt.safe_cast("posting_date", dbt.type_timestamp()) }} as posting_date,
        {{ dbt.safe_cast("application_deadline", dbt.type_timestamp()) }} as application_deadline,
        
        -- Location and requirements
        {{ dbt.safe_cast("work_location", dbt.type_string()) }} as work_location,
        {{ dbt.safe_cast("minimum_qual_requirements", dbt.type_string()) }} as minimum_qual_requirements,
        {{ dbt.safe_cast("preferred_skills", dbt.type_string()) }} as preferred_skills,
        
        -- Employment details
        {{ dbt.safe_cast("full_time_part_time_indicator", dbt.type_string()) }} as employment_type,
        {{ dbt.safe_cast("career_level", dbt.type_string()) }} as career_level,
        
        -- Metadata
        current_timestamp as dbt_loaded_at,
        current_date as dbt_valid_from
        
    from source_data
    where job_id is not null  -- Basic data quality filter
),

add_calculated_fields as (
    select 
        *,
        
        -- Calculate salary midpoint
        case 
            when salary_range_from is not null and salary_range_to is not null
            then (salary_range_from + salary_range_to) / 2.0
            else null
        end as salary_midpoint,
        
        -- Calculate posting duration in days
        case 
            when posting_date is not null and application_deadline is not null
            then extract(day from application_deadline - posting_date)
            else null
        end as posting_duration_days,
        
        -- Standardize agency names
        case 
            when upper(agency_name) like '%POLICE%' then 'Police Department'
            when upper(agency_name) like '%FIRE%' then 'Fire Department'
            when upper(agency_name) like '%EDUCATION%' or upper(agency_name) like '%DOE%' then 'Department of Education'
            when upper(agency_name) like '%HEALTH%' then 'Department of Health'
            else agency_name
        end as agency_name_standardized
        
    from cleaned_data
)

select * from add_calculated_fields
