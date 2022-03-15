SELECT
    submission_date,
    COUNTIF(active_hours_sum > 0 AND uri_count > 0) AS qcdou
FROM telemetry.unified_metrics
WHERE normalized_app_name = 'Firefox Desktop'
AND submission_date >= DATE(2022,1,1)
GROUP BY 1
ORDER BY 1