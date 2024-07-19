-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients`
AS
SELECT
  client_id,
  sample_id,
  adjust_info.* EXCEPT (submission_timestamp),
  adjust_info.submission_timestamp AS adjust_attribution_timestamp,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients_v1`
