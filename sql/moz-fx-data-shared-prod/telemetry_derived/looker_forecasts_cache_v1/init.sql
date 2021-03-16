CREATE OR REPLACE TABLE
  telemetry_derived.looker_forecasts_cache_v1
CLUSTER BY
  key
AS
SELECT
  CAST(NULL AS STRING) AS key,
  CAST(NULL AS TIMESTAMP) AS submission_date,
        -- DAU
  CAST(NULL AS FLOAT64) AS dau_forecast,
  CAST(NULL AS FLOAT64) * 1.05 AS dau_target,
  CAST(NULL AS FLOAT64) AS cdou_forecast,
  CAST(NULL AS FLOAT64) AS cdou_target,
  CAST(NULL AS FLOAT64) AS dau_forecast_lower,
  CAST(NULL AS FLOAT64) AS dau_forecast_upper,
        -- New Profiles
  CAST(NULL AS FLOAT64) AS new_profiles_forecast,
  CAST(NULL AS FLOAT64) * 1.05 AS new_profiles_target,
  CAST(NULL AS FLOAT64) AS cum_new_profiles_forecast,
  CAST(NULL AS FLOAT64) AS cum_new_profiles_target,
  CAST(NULL AS FLOAT64) AS new_profiles_forecast_lower,
  CAST(NULL AS FLOAT64) AS new_profiles_forecast_upper,
