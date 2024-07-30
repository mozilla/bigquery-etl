-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.attribution_clients`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  install_source,
  adjust_info.*,
  play_store_info.*,
  meta_info.*,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(
    adjust_info.adjust_network
  ) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix_derived.attribution_clients_v1`
