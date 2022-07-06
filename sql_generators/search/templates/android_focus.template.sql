-- metrics for {{ app_name }} {{ channel }}
metrics_{{ namespace }} AS (
    SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        normalized_country_code AS country,
        '{{ app_name }}' AS app_name,
        'Focus' AS normalized_app_name,
        client_info.app_display_version AS app_version,
        'Release' AS channel,
        normalized_os AS os,
        client_info.android_sdk_version AS os_version,
        metrics.string.browser_default_search_engine AS default_search_engine,
        CAST(NULL AS STRING) AS default_search_engine_submission_url,
        sample_id,
        metrics.labeled_counter.browser_search_search_count AS search_count,
        metrics.labeled_counter.browser_search_ad_clicks as search_ad_clicks,
        metrics.labeled_counter.browser_search_in_content AS search_in_content,
        metrics.labeled_counter.browser_search_with_ads as search_with_ads,
        client_info.first_run_date,
        ping_info.end_time,
        ping_info.experiments,
        metrics.counter.browser_total_uri_count,
        client_info.locale,
    FROM
        {{ namespace }}.metrics AS {{ namespace }}_metrics
),