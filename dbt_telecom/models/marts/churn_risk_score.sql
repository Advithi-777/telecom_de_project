{{ config(materialized='table') }}

SELECT
    customer_id,
    full_name,
    region,
    plan,
    tenure_months,
    monthly_charge,
    is_churned,
    risk_segment,
    num_complaints,
    total_calls,
    total_late_payments,
    total_dropped_calls,
    total_revenue,
    avg_call_duration_min,
    -- Churn risk score (0-100)
    ROUND(
        (
            -- Short tenure increases risk (max 30 points)
            CASE
                WHEN tenure_months <= 3  THEN 30
                WHEN tenure_months <= 6  THEN 20
                WHEN tenure_months <= 12 THEN 10
                ELSE 0
            END
            +
            -- Complaints increase risk (max 30 points)
            CASE
                WHEN num_complaints >= 5 THEN 30
                WHEN num_complaints >= 3 THEN 20
                WHEN num_complaints >= 1 THEN 10
                ELSE 0
            END
            +
            -- Late payments increase risk (max 20 points)
            CASE
                WHEN total_late_payments >= 3 THEN 20
                WHEN total_late_payments >= 2 THEN 15
                WHEN total_late_payments >= 1 THEN 10
                ELSE 0
            END
            +
            -- Low call activity increases risk (max 20 points)
            CASE
                WHEN total_calls <= 5  THEN 20
                WHEN total_calls <= 10 THEN 10
                ELSE 0
            END
        ), 2
    ) AS churn_risk_score,
    -- Churn risk label
    CASE
        WHEN num_complaints >= 5
          OR tenure_months <= 3 THEN 'Critical'
        WHEN num_complaints >= 3
          OR tenure_months <= 6 THEN 'High'
        WHEN num_complaints >= 1
          OR total_late_payments >= 2 THEN 'Medium'
        ELSE 'Low'
    END AS churn_risk_label,
    CURRENT_TIMESTAMP AS model_run_timestamp
FROM {{ ref('customer_monthly_summary') }}