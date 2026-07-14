CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.looker_mobile_forecasts_cache`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.looker_mobile_forecasts_cache_v1`
UNION ALL
SELECT
  -- Key for the cache is the filters used. When no filters are used,
  -- we want to retrieve the official forecast, hence the empty string
  '' AS key,
  -- Looker is better suited to deal with timestamp types, since some of
  -- the filters are timestamp comparisons which fail on dates
  CAST(date AS TIMESTAMP) AS submission_date,
  app_name,
  yhat AS dau_forecast,
  yhat * (1 + target_lift) AS dau_target,
  yhat_cumulative AS cdou_forecast,
  target_pace AS cdou_target,
  yhat_p10 AS dau_forecast_lower,
  yhat_p90 AS dau_forecast_upper
FROM
  `moz-fx-data-shared-prod.static.mobile_forecasts_official_2021`
