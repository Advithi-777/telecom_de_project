{{ config(materialized='view') }}

SELECT
    customer_id,
    full_name,
    phone_number,
    email,
    region,
    age,
    plan,
    contract_type,
    tenure_months,
    join_date,
    monthly_charge,
    is_churned,
    data_usage_gb,
    num_complaints,
    tenure_years,
    risk_segment,
    plan_value,
    ingestion_timestamp
FROM {{ source('bronze', 'customers') }}
WHERE customer_id IS NOT NULL