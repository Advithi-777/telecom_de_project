{{ config(materialized='view') }}

SELECT
    kpi_id,
    tower_id,
    region,
    timestamp,
    network_type,
    signal_strength_dbm,
    download_speed_mbps,
    upload_speed_mbps,
    latency_ms,
    active_connections,
    dropped_call_rate_pct,
    tower_status,
    utilisation_pct,
    hour,
    health_score,
    is_degraded,
    speed_ratio,
    ingestion_timestamp
FROM {{ source('bronze', 'network_kpi') }}
WHERE kpi_id IS NOT NULL
AND download_speed_mbps > 0