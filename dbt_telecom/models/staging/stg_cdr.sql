{{ config(materialized='view') }}

SELECT
    cdr_id,
    caller_id,
    receiver_number,
    call_start_time,
    call_duration_sec,
    call_type,
    tower_id,
    network_type,
    is_dropped,
    roaming_flag,
    charge_per_min,
    call_duration_min,
    call_hour,
    call_day_of_week,
    call_date,
    time_of_day,
    call_cost,
    ingestion_timestamp
FROM {{ source('bronze', 'cdr') }}
WHERE cdr_id IS NOT NULL
AND call_duration_sec >= 0