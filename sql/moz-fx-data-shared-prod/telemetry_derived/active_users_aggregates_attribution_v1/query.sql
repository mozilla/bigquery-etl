-- Query for telemetry_derived.active_users_aggregates_attribution_v1
CREATE TEMP FUNCTION normalize_adjust_network(key STRING) AS (
  (
    CASE
    WHEN
      key NOT IN (
        '',
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      AND key IS NOT NULL
    THEN
      'Other'
    ELSE
      key
    END
  )
);

CREATE TEMP FUNCTION normalize_install_source(key STRING) AS (
  (CASE WHEN key NOT IN ('com.android.vending') AND key IS NOT NULL THEN 'Other' ELSE key END)
);

WITH baseline AS (
  SELECT
    client_id,
    attribution_medium IS NOT NULL
    OR attribution_source IS NOT NULL AS attributed,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_variation,
    city,
    country,
    first_seen_date,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    is_default_browser,
    normalized_app_name,
    submission_date,
    days_since_seen,
    ad_click,
    organic_search_count,
    search_count,
    search_with_ads,
    uri_count,
    active_hours_sum
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
  WHERE
    submission_date = @submission_date
),
fenix_first_date_attributions AS (
  -- Firefox Preview beta
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] AS install_source
  FROM
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    1
  UNION ALL
  -- Firefox Preview nightly
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] AS install_source
  FROM
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    1
  UNION ALL
  -- Fenix nightly
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] AS install_source
  FROM
    org_mozilla_fennec_aurora.metrics AS org_mozilla_fennec_aurora_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    1
  UNION ALL
  -- Fenix beta
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] AS install_source
  FROM
    org_mozilla_firefox_beta.metrics AS org_mozilla_firefox_beta_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    1
  UNION ALL
  -- Fenix release
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] AS install_source
  FROM
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    1
),
fenix_unique_attribution AS (
  SELECT
    client_id,
    DATE(min_submission_timestamp) AS attribution_date,
    ANY_VALUE(adjust_network) AS adjust_network,
    ANY_VALUE(install_source) AS install_source
  FROM
    fenix_first_date_attributions
  GROUP BY
    1,
    2
),
all_with_first_attribution AS (
  SELECT
    base.client_id,
    base.attributed,
    base.attribution_campaign,
    base.attribution_content,
    base.attribution_experiment,
    base.attribution_medium,
    base.attribution_source,
    base.attribution_variation,
    normalize_adjust_network(att.adjust_network) AS adjust_network,
    normalize_install_source(att.install_source) AS install_source,
    base.city,
    base.country,
    base.first_seen_date,
    base.first_seen_year,
    base.is_default_browser,
    base.normalized_app_name,
    base.submission_date,
    base.days_since_seen,
    base.ad_click,
    base.organic_search_count,
    base.search_count,
    base.search_with_ads,
    base.uri_count,
    base.active_hours_sum
  FROM
    baseline AS base
  LEFT JOIN
    fenix_unique_attribution AS att
  ON
    att.client_id = base.client_id
)
SELECT
  attributed,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  adjust_network,
  install_source,
  city,
  country,
  first_seen_year,
  is_default_browser,
  normalized_app_name AS app_name,
  submission_date,
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau,
  COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
  COUNT(DISTINCT client_id) AS mau,
  COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
  SUM(ad_click) AS ad_clicks,
  SUM(organic_search_count) AS organic_search_count,
  SUM(search_count) AS search_count,
  SUM(search_with_ads) AS search_with_ads,
  SUM(uri_count) AS uri_count,
  SUM(active_hours_sum) AS active_hours
FROM
  all_with_first_attribution
GROUP BY
  attributed,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  adjust_network,
  install_source,
  city,
  country,
  first_seen_year,
  is_default_browser,
  app_name,
  submission_date
