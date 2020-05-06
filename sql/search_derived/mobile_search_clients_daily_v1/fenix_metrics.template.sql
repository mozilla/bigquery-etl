-- baseline for {namespace} ({app_name} {channel})
baseline_{namespace} AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    {namespace}.baseline
),
-- baseline for {namespace} ({app_name} {channel})
metrics_{namespace} AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code,
    '{app_name}' AS app_name,
    client_info.app_display_version,
    '{channel}' AS channel,
    normalized_os,
    client_info.android_sdk_version,
    metrics.string.search_default_engine_code,
    metrics.string.search_default_engine_submission_url,
    sample_id,
    CAST(NULL AS STRING) AS distribution_id,
    metrics.labeled_counter.metrics_search_count,
    client_info.first_run_date,
    ping_info.end_time
  FROM
    {namespace}.metrics AS {namespace}_metrics
),
