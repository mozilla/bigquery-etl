CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.fenix.placeholder_name
AS
SELECT
  *,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix_derived.placeholder_name_v1`
