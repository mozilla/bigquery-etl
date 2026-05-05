-- metrics for {{ app_name }} {{ channel }}
baseline_{{ namespace }} AS (
    SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        normalized_country_code AS country,
        '{{ app_name }}' AS app_name,
        'Klar' AS normalized_app_name,
        client_info.app_display_version AS app_version,
        '{{ channel }}' AS channel,
        normalized_os AS os,
        client_info.locale,
        normalized_os_version AS os_version,
        metrics.string.browser_default_search_engine AS default_search_engine,
        CAST(NULL AS STRING) AS default_search_engine_submission_url,
        sample_id,
        metrics.labeled_counter.browser_search_search_count AS search_count,
        metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
        metrics.labeled_counter.browser_search_in_content AS search_in_content,
        ARRAY<STRUCT<key STRING, value INT64>>[STRUCT(NULL AS key, NULL AS value)] AS search_with_ads,
        client_info.first_run_date,
        ping_info.end_time,
        ping_info.experiments,
        metrics.counter.browser_total_uri_count AS total_uri_count,
        CAST(NULL AS STRING) AS distribution_id,
    FROM
        {{ namespace }}.baseline AS {{ namespace }}_baseline
),