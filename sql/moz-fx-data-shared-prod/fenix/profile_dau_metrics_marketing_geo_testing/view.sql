CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.profile_dau_metrics_marketing_geo_testing`
AS
SELECT
  *,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix_derived.profile_dau_metrics_marketing_geo_testing`
