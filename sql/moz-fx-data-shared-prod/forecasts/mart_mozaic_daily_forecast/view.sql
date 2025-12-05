CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.forecasts.mart_mozaic_daily_forecast`
AS
SELECT
  forecast_start_date,
  target_date,
  source,
  country,
  app_name,
  app_category,
  COALESCE(JSON_VALUE(segment, "$.os"), "N/A") AS os,
  dau,
  new_profiles,
  existing_engagement_dau,
  existing_engagement_mau
FROM
  `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v1`
