-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.new_profile_activations`
AS
SELECT
  *,
  "Organic" AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.klar_android_derived.new_profile_activations_v1`
