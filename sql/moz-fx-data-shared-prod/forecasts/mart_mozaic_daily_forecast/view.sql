CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.forecasts.mart_mozaic_daily_forecast`
AS
SELECT
  forecast_start_date,
  data_source,
  target_date,
  data_type,
  country,
  app_name,
  COALESCE(JSON_VALUE(segment, "$.os"), "N/A") AS os,
  dau,
  new_profiles,
  existing_engagement_dau,
  existing_engagement_mau
FROM
  `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2`
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      forecast_start_date,
      data_source,
      target_date,
      data_type,
      country,
      app_name,
      segment
    ORDER BY
      forecast_run_timestamp DESC
  ) = 1;
