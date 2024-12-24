-- metrics for Firefox iOS {{ channel }}
baseline_{{ namespace }} AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    '{{ app_name }}' AS app_name,
    -- Fennec is used to be consistent with core pings
    'Fennec' AS normalized_app_name,
    client_info.locale,
    client_info.app_display_version AS app_version,
    '{{ channel }}' AS channel,
    normalized_os AS os,
    normalized_os_version AS os_version,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.search_counts AS search_count,
    extract_ios_provider(metrics.labeled_counter.browser_search_ad_clicks) AS search_ad_clicks,
    metrics.labeled_counter.search_in_content AS search_in_content,
    extract_ios_provider(metrics.labeled_counter.browser_search_with_ads) AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.tabs_normal_and_private_uri_count AS total_uri_count,
    CAST(NULL AS STRING) AS distribution_id,
  FROM
    {{ namespace }}.baseline
  AS
    {{ namespace }}_baseline
  WHERE
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') >= 28
),
