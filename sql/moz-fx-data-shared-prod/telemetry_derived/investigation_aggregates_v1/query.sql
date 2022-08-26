-- Query for aggregated dimensions and metrics for investigation
-- of incidents
WITH fenix_baseline AS (
  SELECT
    submission_date,
    client_id,
    first_seen_date,
    days_since_seen,
    country,
    normalized_channel,
    search_count,
    ad_click
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
  WHERE
    submission_date = @submission_date
    AND normalized_app_name = 'Fenix'
),
fenix_first_attribution AS (
  -- Firefox Preview beta
  SELECT
    client_info.client_id,
    MIN(DATE(submission_timestamp)) AS submission_date,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] NOT IN (
        '',
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      AND ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)]
    END
    AS adjust_network,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] NOT IN (
        'com.android.vending'
      )
      AND ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)]
    END
    AS install_source
  FROM
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    1
  UNION ALL
  -- Firefox Preview nightly
  SELECT
    client_info.client_id,
    MIN(DATE(submission_timestamp)) AS submission_date,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] NOT IN (
        '',
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      AND ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)]
    END
    AS adjust_network,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] NOT IN (
        'com.android.vending'
      )
      AND ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)]
    END
    AS install_source
  FROM
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    1
  UNION ALL
  -- Fenix nightly
  SELECT
    client_info.client_id,
    MIN(DATE(submission_timestamp)) AS submission_date,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] NOT IN (
        '',
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      AND ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)]
    END
    AS adjust_network,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] NOT IN (
        'com.android.vending'
      )
      AND ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)]
    END
    AS install_source
  FROM
    org_mozilla_fennec_aurora.metrics AS org_mozilla_fennec_aurora_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    1
  UNION ALL
  -- Fenix beta
  SELECT
    client_info.client_id,
    MIN(DATE(submission_timestamp)) AS submission_date,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] NOT IN (
        '',
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      AND ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)]
    END
    AS adjust_network,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] NOT IN (
        'com.android.vending'
      )
      AND ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)]
    END
    AS install_source
  FROM
    org_mozilla_firefox_beta.metrics AS org_mozilla_firefox_beta_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    1
  UNION ALL
  -- Fenix release
  SELECT
    client_info.client_id,
    MIN(DATE(submission_timestamp)) AS submission_date,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] NOT IN (
        '',
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      AND ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_adjust_network)[SAFE_OFFSET(0)]
    END
    AS adjust_network,
    CASE
    WHEN
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] NOT IN (
        'com.android.vending'
      )
      AND ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)] IS NOT NULL
    THEN
      'Other'
    ELSE
      ARRAY_AGG(metrics.string.metrics_install_source)[SAFE_OFFSET(0)]
    END
    AS install_source
  FROM
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    1
),
fenix AS (
  SELECT
    base.client_id,
    base.submission_date,
    base.first_seen_date,
    base.days_since_seen,
    base.country,
    normalized_channel AS channel,
    att.adjust_network,
    att.install_source,
    base.search_count,
    base.ad_click
  FROM
    fenix_baseline AS base
  LEFT JOIN
    fenix_first_attribution AS att
  ON
    att.client_id = base.client_id
)
SELECT
  submission_date,
  country,
  channel,
  adjust_network,
  install_source,
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau,
  COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
  SUM(search_count) AS search_count,
  SUM(ad_click) AS ad_clicks
FROM
  fenix
GROUP BY
  1,
  2,
  3,
  4,
  5
