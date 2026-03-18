{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.full_name,
    c.region,
    c.plan,
    c.contract_type,
    c.tenure_months,
    c.tenure_years,
    c.monthly_charge,
    c.is_churned,
    c.risk_segment,
    c.num_complaints,
    c.data_usage_gb,
    -- Billing aggregations
    COUNT(b.bill_id)                        AS total_bills,
    SUM(b.total_charge)                     AS total_revenue,
    AVG(b.total_charge)                     AS avg_monthly_charge,
    SUM(b.is_late_payment)                  AS total_late_payments,
    SUM(b.has_overage)                      AS total_overage_months,
    SUM(b.roaming_charge)                   AS total_roaming_charges,
    -- CDR aggregations
    COUNT(cdr.cdr_id)                       AS total_calls,
    AVG(cdr.call_duration_min)              AS avg_call_duration_min,
    SUM(cdr.call_cost)                      AS total_call_cost,
    SUM(cdr.is_dropped)                     AS total_dropped_calls,
    SUM(cdr.roaming_flag)                   AS total_roaming_calls
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_billing') }} b
    ON c.customer_id = b.customer_id
LEFT JOIN {{ ref('stg_cdr') }} cdr
    ON c.customer_id = cdr.caller_id
GROUP BY
    c.customer_id,
    c.full_name,
    c.region,
    c.plan,
    c.contract_type,
    c.tenure_months,
    c.tenure_years,
    c.monthly_charge,
    c.is_churned,
    c.risk_segment,
    c.num_complaints,
    c.data_usage_gb