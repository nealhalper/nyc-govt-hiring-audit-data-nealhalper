-- Staging model for NYC payroll data  
-- Cleans and standardizes citywide payroll information

{{ config(materialized='view') }}

with source_data as (
    select * from {{ ref('raw_payroll_data') }}
),

cleaned_data as (
    select
        -- Employee identifiers (anonymized)
        {{ dbt.safe_cast("payroll_number", dbt.type_string()) }} as payroll_number,
        
        -- Agency and department
        {{ dbt.safe_cast("agency_name", dbt.type_string()) }} as agency_name,
        {{ dbt.safe_cast("title_description", dbt.type_string()) }} as title_description,
        
        -- Salary information
        case 
            when base_salary is not null and base_salary > 0 
            then base_salary::numeric 
            else null 
        end as base_salary,
        
        case 
            when regular_hours is not null and regular_hours >= 0 
            then regular_hours::numeric 
            else null 
        end as regular_hours,
        
        case 
            when regular_gross_paid is not null and regular_gross_paid >= 0 
            then regular_gross_paid::numeric 
            else null 
        end as regular_gross_paid,
        
        case 
            when total_ot_paid is not null and total_ot_paid >= 0 
            then total_ot_paid::numeric 
            else 0 
        end as total_ot_paid,
        
        case 
            when total_other_pay is not null and total_other_pay >= 0 
            then total_other_pay::numeric 
            else 0 
        end as total_other_pay,
        
        -- Employment details
        {{ dbt.safe_cast("leave_status_as_of_june_30", dbt.type_string()) }} as leave_status,
        
        -- Time period
        fiscal_year::integer as fiscal_year,
        
        -- Metadata
        current_timestamp as dbt_loaded_at,
        current_date as dbt_valid_from
        
    from source_data
    where payroll_number is not null
      and agency_name is not null
      and title_description is not null
      and fiscal_year is not null
),

add_calculated_fields as (
    select 
        *,
        
        -- Calculate total compensation
        coalesce(base_salary, 0) + 
        coalesce(total_ot_paid, 0) + 
        coalesce(total_other_pay, 0) as total_compensation,
        
        -- Calculate hourly rate approximation
        case 
            when regular_hours is not null and regular_hours > 0 and base_salary is not null
            then base_salary / regular_hours
            else null
        end as estimated_hourly_rate,
        
        -- Standardize agency names (matching job postings)
        case 
            when upper(agency_name) like '%POLICE%' then 'Police Department'
            when upper(agency_name) like '%FIRE%' then 'Fire Department'
            when upper(agency_name) like '%EDUCATION%' or upper(agency_name) like '%DOE%' then 'Department of Education'
            when upper(agency_name) like '%HEALTH%' then 'Department of Health'
            else agency_name
        end as agency_name_standardized,
        
        -- Employment status flags
        case 
            when upper(leave_status) in ('ACTIVE', 'A') then true
            else false
        end as is_active_employee
        
    from cleaned_data
)

select * from add_calculated_fields
