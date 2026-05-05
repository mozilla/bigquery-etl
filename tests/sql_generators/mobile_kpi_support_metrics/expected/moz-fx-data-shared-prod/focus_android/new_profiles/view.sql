-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.new_profiles`
AS
SELECT
  *,
  "Organic" AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.focus_android_derived.new_profiles_v1`
