-- metrics for Firefox iOS {{ channel }}
metrics_{{ namespace }} AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    '{{ app_name }}' AS app_name,
    -- Fennec is used to be consistent with core pings
    'Fennec' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    '{{ channel }}' AS channel,
    normalized_os AS os,
    client_info.os_version AS os_version,
    CAST(NULL AS STRING) AS adjust_network,
    CAST(NULL AS STRING) AS install_source,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.search_counts AS search_count,
    extract_ios_provider(metrics.labeled_counter.browser_search_ad_clicks) as search_ad_clicks,
    metrics.labeled_counter.search_in_content AS search_in_content,
    extract_ios_provider(metrics.labeled_counter.browser_search_with_ads) as search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    NULL AS total_uri_count,
    client_info.locale,
  FROM
    {{ namespace }}.metrics
  AS
    {{ namespace }}_metrics
  WHERE
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') >= 28
),
