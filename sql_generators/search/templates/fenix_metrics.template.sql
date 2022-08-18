-- baseline for {{ app_name }} {{ channel }}
baseline_{{ namespace }} AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    {{ namespace }}.baseline
),
-- metrics for {{ app_name }} {{ channel }}
metrics_{{ namespace }} AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    '{{ app_name }}' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    '{{ channel }}' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    CASE
      WHEN metrics.string.metrics_adjust_network NOT IN ('', 'Organic', 'Google Organic Search', 'Untrusted Devices', 'Product Marketing', 'Google Ads ACI') AND metrics.string.metrics_adjust_network IS NOT NULL THEN 'Other'
      ELSE metrics.string.metrics_adjust_network
    END AS adjust_network,
    CASE
      WHEN metrics.string.metrics_install_source NOT IN ('com.android.vending') AND metrics.string.metrics_install_source IS NOT NULL THEN 'Other'
      ELSE metrics.string.metrics_install_source
    END AS install_source,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.metrics_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.events_total_uri_count AS total_uri_count,
  FROM
    {{ namespace }}.metrics AS {{ namespace }}_metrics
),
