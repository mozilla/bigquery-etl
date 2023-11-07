CREATE TEMP TABLE
    `{{ app_name }}_active_users_aggregates_base`
AS
    WITH attribution_data AS (
    SELECT
      client_id,
      adjust_network,
      install_source
    FROM
      fenix.firefox_android_clients
    UNION ALL
    SELECT
      client_id,
      adjust_network,
      CAST(NULL AS STRING) install_source
    FROM
      firefox_ios.firefox_ios_clients
    ),
    baseline AS (
    SELECT
      submission_date,
      normalized_channel,
      client_id,
      days_since_seen,
      days_seen_bits,
      days_created_profile_bits,
      durations,
      normalized_os,
      normalized_os_version,
      locale,
      city,
      country,
      app_display_version,
      device_model,
      first_seen_date,
      submission_date = first_seen_date AS is_new_profile,
      uri_count,
      is_default_browser,
      CAST(NULL AS string) AS distribution_id,
      isp,
      IF(isp = 'BrowserStack', CONCAT('Klar iOS', ' BrowserStack'), 'Klar iOS') AS app_name
    FROM
      `{{ project_id }}.{{ app_name }}.clients_last_seen_joined`
    WHERE
      submission_date = @submission_date
    ),
    search_clients AS (
    SELECT
      client_id,
      submission_date,
      ad_click,
      organic,
      search_count,
      search_with_ads
    FROM
      `{{ project_id }}.search_derived.mobile_search_clients_daily_v1`
    WHERE
      submission_date = @submission_date
    ),
    search_metrics AS (
    SELECT
      baseline.client_id,
      baseline.submission_date,
      SUM(ad_click) AS ad_clicks,
      SUM(organic) AS organic_search_count,
      SUM(search_count) AS search_count,
      SUM(search_with_ads) AS search_with_ads
    FROM
      baseline
    LEFT JOIN
      search_clients s
    ON
      baseline.client_id = s.client_id
      AND baseline.submission_date = s.submission_date
    GROUP BY
      client_id,
      submission_date
    ),
    baseline_with_searches AS (
    SELECT
      baseline.client_id,
      CASE
        WHEN BIT_COUNT(days_seen_bits)
          BETWEEN 1
          AND 6
          THEN 'infrequent_user'
        WHEN BIT_COUNT(days_seen_bits)
          BETWEEN 7
          AND 13
          THEN 'casual_user'
        WHEN BIT_COUNT(days_seen_bits)
          BETWEEN 14
          AND 20
          THEN 'regular_user'
        WHEN BIT_COUNT(days_seen_bits) >= 21
          THEN 'core_user'
        ELSE 'other'
      END AS activity_segment,
      baseline.app_name,
      baseline.app_display_version AS app_version,
      baseline.normalized_channel,
      IFNULL(country, '??') country,
      baseline.city,
      baseline.days_seen_bits,
      baseline.days_created_profile_bits,
      DATE_DIFF(baseline.submission_date, baseline.first_seen_date, DAY) AS days_since_first_seen,
      baseline.device_model,
      baseline.isp,
      baseline.is_new_profile,
      baseline.locale,
      baseline.first_seen_date,
      baseline.days_since_seen,
      baseline.normalized_os,
      baseline.normalized_os_version,
      COALESCE(
        SAFE_CAST(NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
        0
      ) AS os_version_major,
      COALESCE(
        SAFE_CAST(NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
        0
      ) AS os_version_minor,
      COALESCE(
        SAFE_CAST(NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
        0
      ) AS os_version_patch,
      baseline.durations AS durations,
      baseline.submission_date,
      baseline.uri_count,
      baseline.is_default_browser,
      baseline.distribution_id,
      CAST(NULL AS string) AS attribution_content,
      CAST(NULL AS string) AS attribution_source,
      CAST(NULL AS string) AS attribution_medium,
      CAST(NULL AS string) AS attribution_campaign,
      CAST(NULL AS string) AS attribution_experiment,
      CAST(NULL AS string) AS attribution_variation,
      search.ad_clicks,
      search.organic_search_count,
      search.search_count,
      search.search_with_ads,
      NULL AS active_hours_sum
    FROM
      baseline
    LEFT JOIN
      search_metrics search
    ON
      search.client_id = baseline.client_id
      AND search.submission_date = baseline.submission_date
    ),
    baseline_with_searches_and_attribution AS (
    SELECT
      baseline.*,
      attribution_data.install_source,
      attribution_data.adjust_network
    FROM
      baseline_with_searches baseline
    LEFT JOIN
      attribution_data
    USING
      (client_id)
    ),
    todays_metrics AS (
    SELECT
      activity_segment AS segment,
      app_version,
      attribution_medium,
      attribution_source,
      attribution_medium IS NOT NULL
      OR attribution_source IS NOT NULL AS attributed,
      city,
      country,
      distribution_id,
      EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
      is_default_browser,
      COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
      app_name AS app_name,
      normalized_channel AS channel,
      normalized_os AS os,
      normalized_os_version AS os_version,
      os_version_major,
      os_version_minor,
      submission_date,
      days_since_seen,
      client_id,
      first_seen_date,
      ad_clicks,
      organic_search_count,
      search_count,
      search_with_ads,
      uri_count,
      active_hours_sum,
      adjust_network,
      install_source,
      durations
    FROM
      baseline_with_searches_and_attribution
    ),
    todays_metrics_enriched AS (
    SELECT
      todays_metrics.* EXCEPT (locale),
      CASE
        WHEN locale IS NOT NULL
          AND languages.language_name IS NULL
          THEN 'Other'
        ELSE languages.language_name
      END AS language_name,
    FROM
      todays_metrics
    LEFT JOIN
      `mozdata.static.csa_gblmkt_languages` AS languages
    ON
      todays_metrics.locale = languages.code
    )
    SELECT
    *
    FROM
    todays_metrics_enriched;

SELECT
    segment,
    app_version,
    attribution_medium,
    attribution_source,
    attributed,
    city,
    country,
    distribution_id,
    first_seen_year,
    is_default_browser,
    app_name,
    channel,
    os,
    os_version,
    os_version_major,
    os_version_minor,
    submission_date,
      language_name,
    COUNT(DISTINCT IF(days_since_seen = 0 AND durations > 0, client_id, NULL)) AS dau,
    COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
    COUNT(DISTINCT client_id) AS mau,
    COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
    SUM(ad_clicks) AS ad_clicks,
    SUM(organic_search_count) AS organic_search_count,
    SUM(search_count) AS search_count,
    SUM(search_with_ads) AS search_with_ads,
    SUM(uri_count) AS uri_count,
    SUM(active_hours_sum) AS active_hours,
    adjust_network,
    install_source,
    DATE(CURRENT_DATE()) AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    IF(
      submission_date >= CURRENT_DATE(),
      TRUE,
      FALSE
    ) AS is_active
FROM
    `{{ app_name }}_active_users_aggregates_base` AS base
GROUP BY
    app_version,
    attribution_medium,
    attribution_source,
    attributed,
    city,
    country,
    distribution_id,
    first_seen_year,
    is_default_browser,
    language_name,
    app_name,
    channel,
    os,
    os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    segment,
    adjust_network,
    install_source
UNION ALL
    SELECT
        * EXCEPT (is_active, valid_to), DATE(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) AS valid_to, is_active
    FROM
        `{{ project_id }}.{{ app_name }}.active_users_aggregates` --TODO: check if we should use the table or the view here.
    WHERE
        submission_date = @submission_date;