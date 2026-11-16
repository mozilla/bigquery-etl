CREATE OR REPLACE TABLE
  telemetry_derived.looker_mobile_forecasts_cache_v1
CLUSTER BY
  key
AS
SELECT
  CAST(NULL AS STRING) AS key,
  CAST(NULL AS TIMESTAMP) AS submission_date,
  CAST(NULL AS STRING) AS app_name,
  CAST(NULL AS FLOAT64) AS dau_forecast,
  CAST(NULL AS FLOAT64) AS dau_target,
  CAST(NULL AS FLOAT64) AS cdou_forecast,
  CAST(NULL AS FLOAT64) AS cdou_target,
  CAST(NULL AS FLOAT64) AS dau_forecast_lower,
  CAST(NULL AS FLOAT64) AS dau_forecast_upper
