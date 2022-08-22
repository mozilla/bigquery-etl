-- Use unified_metrics as the source of truth for KPIs
-- DAU is aggregated to calculate CDOU. 
-- Forecasting procedure for CDOU: Forecast DAU then aggregate to CDOU. CDOU should not be forecasted directly.
SELECT
    submission_date,
    COUNTIF(normalized_app_name != 'Firefox Desktop') AS DAU
FROM telemetry.unified_metrics
WHERE `mozfun`.bits28.active_in_range(days_seen_bits, 0, 1)
AND submission_date >= DATE(2018,1,1)
GROUP BY 1
ORDER BY 1
