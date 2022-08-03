SELECT
    submission_date,
    COUNTIF(normalized_app_name = 'Firefox Desktop' AND active_hours_sum > 0 AND uri_count > 0) AS QDAU,
FROM telemetry.unified_metrics
WHERE `mozfun`.bits28.active_in_range(days_seen_bits, 0, 1)
AND submission_date >= DATE(2018,1,1)
GROUP BY 1
ORDER BY 1
