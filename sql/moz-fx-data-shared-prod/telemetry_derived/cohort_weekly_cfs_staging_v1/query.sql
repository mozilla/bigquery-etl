SELECT
  normalized_app_name,
  normalized_channel,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  country,
  device_model,
  distribution_id,
  is_default_browser,
  locale,
  normalized_os,
  normalized_os_version,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_content,
  play_store_attribution_term,
  play_store_attribution_install_referrer_response,
  DATE_TRUNC(cohort_date, WEEK) AS cohort_date_week,
  client_id
FROM
  `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
WHERE
  cohort_date >= DATE_TRUNC(
    DATE_SUB(@submission_date, INTERVAL 768 day),
    WEEK
  ) --start of week for date 768 days ago
  AND cohort_date <= DATE_SUB(
    DATE_TRUNC(@submission_date, WEEK),
    INTERVAL 1 DAY
  ) --end of last completed week
  AND LOWER(normalized_app_name) NOT LIKE '%browserstack'
  AND LOWER(normalized_app_name) NOT LIKE '%mozillaonline'
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY cohort_date ASC) = 1;
