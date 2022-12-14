CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.fenix.marketing_attributable_metrics
AS
SELECT
  * EXCEPT (searches),
  1 AS dau,
  IF(is_new_profile, 1, 0) AS new_profiles,
  searches AS search_count,
FROM
  `moz-fx-data-shared-prod.fenix.attributable_clients`
