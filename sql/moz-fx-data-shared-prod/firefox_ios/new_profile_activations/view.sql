-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profile_activations`
AS
SELECT
  *,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.new_profile_activations_v1`
