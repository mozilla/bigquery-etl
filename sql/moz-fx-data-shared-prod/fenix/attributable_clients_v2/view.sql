CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.fenix.attributable_clients_v2
AS
WITH unfiltered_activations AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.fenix.new_profile_activation
  WHERE
    submission_date > "2012-06-10"
)
SELECT
  attributable_clients_v2.submission_date,
  firefox_android_clients.first_seen_date AS cohort_date,
  firefox_android_clients.first_seen_date,
  attributable_clients_v2.sample_id,
  attributable_clients_v2.client_id,
  -- Metrics
  attributable_clients_v2.activations_count,
  attributable_clients_v2.active_day_count,
  attributable_clients_v2.searches,
  attributable_clients_v2.searches_with_ads,
  attributable_clients_v2.ad_clicks,
  -- Dimensions
  firefox_android_clients.first_reported_country,
  firefox_android_clients.adjust_network,
  firefox_android_clients.adjust_ad_group,
  firefox_android_clients.adjust_campaign,
  firefox_android_clients.adjust_creative,
  firefox_android_clients.first_seen_date = attributable_clients_v2.submission_date
  AND firefox_android_clients.metadata.reported_first_session_ping AS is_new_install,
  firefox_android_clients.first_seen_date = attributable_clients_v2.submission_date AS is_new_profile,
  COALESCE(unfiltered_activations.activated = 1, FALSE) AS is_activated,
  attributable_clients_v2.metadata,
  firefox_android_clients.metadata AS firefox_android_clients_metadata,
FROM
  `moz-fx-data-shared-prod`.fenix_derived.attributable_clients_v2
JOIN
  `moz-fx-data-shared-prod`.fenix.firefox_android_clients
USING
  (client_id)
JOIN
  unfiltered_activations
USING
  (client_id)
