-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_ios.new_profiles`
AS
SELECT
  *,
  "Organic" AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.klar_ios_derived.new_profiles_v1`