CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_funnel_forecasts`
AS
SELECT
  *
FROM
  -- this will be switched over to point at a permanent table in shared-prod shortly
  `mozdata.revenue_cat3_analysis.search_revenue_forecast_stage`
