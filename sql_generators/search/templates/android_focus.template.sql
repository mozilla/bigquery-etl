-- metrics for Focus Android {{ channel }}
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
        ARRAY<STRUCT<key STRING, value INT64>>[] as search_count,
        -- metrics.labeled_counter.search_counts AS search_count, -- TODO: Make sure the key for this looks like Fenix keys
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
    WHERE
        mozfun.norm.truncate_version(client_info.android_sdk_version, 'minor') >= 98  -- TODO: Update this with the actual version
),