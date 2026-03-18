{{ config(materialized='table') }}

SELECT
    tower_id,
    region,
    network_type,
    COUNT(kpi_id)                           AS total_readings,
    AVG(signal_strength_dbm)                AS avg_signal_strength,
    AVG(download_speed_mbps)                AS avg_download_speed,
    AVG(upload_speed_mbps)                  AS avg_upload_speed,
    AVG(latency_ms)                         AS avg_latency,
    AVG(utilisation_pct)                    AS avg_utilisation,
    AVG(dropped_call_rate_pct)              AS avg_dropped_call_rate,
    MAX(dropped_call_rate_pct)              AS max_dropped_call_rate,
    SUM(is_degraded)                        AS total_degraded_readings,
    ROUND(
        SUM(is_degraded) / COUNT(kpi_id) * 100
    , 2)                                    AS degraded_pct,
    -- Tower health rating
    CASE
        WHEN AVG(dropped_call_rate_pct) < 2
         AND AVG(download_speed_mbps) > 40  THEN 'Excellent'
        WHEN AVG(dropped_call_rate_pct) < 5
         AND AVG(download_speed_mbps) > 20  THEN 'Good'
        WHEN AVG(dropped_call_rate_pct) < 10 THEN 'Fair'
        ELSE 'Poor'
    END                                     AS tower_health_rating,
    CURRENT_TIMESTAMP                       AS model_run_timestamp
FROM {{ ref('stg_network_kpi') }}
GROUP BY
    tower_id,
    region,
    network_type