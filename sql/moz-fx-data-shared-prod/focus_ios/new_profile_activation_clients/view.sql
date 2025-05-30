-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_ios.new_profile_activation_clients`
AS
SELECT
  *,
  "Organic" AS paid_vs_organic,
  CAST(NULL AS STRING) AS paid_vs_organic_via_gclid_attribution,
  -- Checking if the client was seen more than once in the first 2 - 7 days
  -- and if they had more than 0 searches within the time window (3 days).
  IF(num_days_seen_day_2_7 > 1 AND search_count > 0, TRUE, FALSE) AS is_activated,
  -- This is based on the more recent DAU definition that takes duration into consideration.
  IF(num_days_active_day_2_7 > 1 AND search_count > 0, TRUE, FALSE) AS is_early_engagement,
FROM
  `moz-fx-data-shared-prod.focus_ios_derived.new_profile_activation_clients_v1`
