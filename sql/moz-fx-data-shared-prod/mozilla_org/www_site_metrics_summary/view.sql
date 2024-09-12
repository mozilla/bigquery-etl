CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.www_site_metrics_summary`
AS
SELECT
  *,
  `moz-fx-data-shared-prod`.udf.funnel_derived_ga_metrics(
    device_category,
    browser,
    operating_system
  ) AS funnel_derived,
  `moz-fx-data-shared-prod`.udf.distribution_model_ga_metrics() AS distribution_model,
  `moz-fx-data-shared-prod`.udf.partner_org_ga_metrics() AS partner_org
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.www_site_metrics_summary_v2`
WHERE
  `date` >= '2023-10-01' --filter out data earlier since downloads not fully set up before this date
