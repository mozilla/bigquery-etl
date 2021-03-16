CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.looker_forecasts_cache`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.looker_forecasts_cache_v1`
UNION ALL
SELECT
  -- Key for the cache is the filters used. When no filters are used,
  -- we want to retrieve the official forecast, hence the empty string
  '' AS key,
  -- Looker is better suited to deal with timestamp types, since some of
  -- the filters are timestamp comparisons which fail on dates
  CAST(ds AS TIMESTAMP) AS submission_date,
  -- DAU
  dau_forecast.dau_forecast,
  dau_forecast.dau_forecast * 1.05 AS dau_target,
  SUM(dau_forecast.dau_forecast) OVER (
    PARTITION BY
      date_trunc(ds, YEAR)
    ORDER BY
      ds ASC
  ) AS cdou_forecast,
  SUM(dau_forecast.dau_forecast) OVER (
    PARTITION BY
      date_trunc(ds, YEAR)
    ORDER BY
      ds ASC
  ) * 1.05 AS cdou_target,
  dau_forecast.dau_forecast_lower,
  dau_forecast.dau_forecast_upper,
  -- New Profiles
  np_forecast.yhat AS new_profiles_forecast,
  np_forecast.yhat * 1.05 AS new_profiles_target,
  SUM(np_forecast.yhat) OVER (
    PARTITION BY
      date_trunc(ds, YEAR)
    ORDER BY
      ds ASC
  ) AS cum_new_profiles_forecast,
  SUM(np_forecast.yhat) OVER (
    PARTITION BY
      date_trunc(ds, YEAR)
    ORDER BY
      ds ASC
  ) * 1.05 AS cum_new_profiles_target,
  np_forecast.yhat_lower AS new_profiles_forecast_lower,
  np_forecast.yhat_upper AS new_profiles_forecast_upper,
FROM
  `mozdata.analysis.loines_desktop_dau_forecast_2021-01-19` AS dau_forecast
JOIN
  `mozdata.analysis.loines_desktop_new_profiles_forecast_2021-01-19` AS np_forecast
ON
  dau_forecast.ds = DATE(np_forecast.date);
