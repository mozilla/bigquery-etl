-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  adjust_info.*,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(
    adjust_info.adjust_network
  ) AS paid_vs_organic,
  is_suspicious_device_client,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.attribution_clients_v1`
