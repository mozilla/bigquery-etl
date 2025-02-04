CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.fenix.activation_metrics_geo_agg
AS
SELECT
  *,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix_derived.activation_metrics_geo_agg_v1`