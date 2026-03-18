{{ config(materialized='view') }}

SELECT
    bill_id,
    customer_id,
    bill_month,
    base_charge,
    data_overage_charge,
    roaming_charge,
    total_charge,
    is_late_payment,
    payment_method,
    invoice_date,
    has_overage,
    has_roaming,
    charge_category,
    ingestion_timestamp
FROM {{ source('bronze', 'billing') }}
WHERE bill_id IS NOT NULL
AND total_charge > 0