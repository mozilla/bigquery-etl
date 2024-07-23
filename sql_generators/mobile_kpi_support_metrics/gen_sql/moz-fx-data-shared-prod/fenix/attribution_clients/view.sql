-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.attribution_clients`
AS
SELECT
  client_id,
  sample_id,
  install_source,
  adjust_info.* EXCEPT (submission_timestamp),
  adjust_info.submission_timestamp AS adjust_attribution_timestamp,
  play_store_info.* EXCEPT (submission_timestamp),
  play_store_info.submission_timestamp AS play_store_attribution_timestamp,
  meta_info.* EXCEPT (submission_timestamp),
  meta_info.submission_timestamp AS meta_attribution_timestamp,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix.attribution_clients_v1`
