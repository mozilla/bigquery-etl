-- Values that cannot be determined are set as UNDETERMINED to differentiate from values not collected, set as ??.
-- Next is the daily query for the aggregates.
WITH _base AS (
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
      IF(
        isp = 'BrowserStack',
        CONCAT('{{ app_value }}', ' BrowserStack'),
        '{{ app_value }}'
      ) AS app_name,
      device_manufacturer --New column
    FROM
      `{{ project_id }}.{{ app_name }}.clients_last_seen_joined`
    WHERE
      {{ if_init }}
      submission_date >= '2021-01-01'
      {{ else_init }}
      submission_date = @submission_date
      {{ end_init }}
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
      {{ if_init }}
      submission_date >= '2021-01-01'
      {{ else_init }}
      submission_date = @submission_date
      {{ end_init }}
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
        SAFE_CAST(
          NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER
        ),
        0
      ) AS os_version_major,
      COALESCE(
        SAFE_CAST(
          NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER
        ),
        0
      ) AS os_version_minor,
      COALESCE(
        SAFE_CAST(
          NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER
        ),
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
      device_manufacturer, -- New column,
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
      durations,
      device_manufacturer -- New column
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
    segment,
    attribution_medium,
    attribution_source,
    attributed,
    city,
    country,
    distribution_id,
    first_seen_year,
    is_default_browser,
    channel,
    os,
    os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    language_name,
    adjust_network,
    install_source,
    app_name,
    app_version,
    device_manufacturer,
    COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau
  FROM
    todays_metrics_enriched
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
    install_source,
    device_manufacturer
)
{{ if_init }}
-- Aggregate the query results before adding the new column
,
base_agg AS (
  SELECT
    segment,
    attribution_medium,
    attribution_source,
    attributed,
    city,
    country,
    distribution_id,
    first_seen_year,
    is_default_browser,
    channel,
    os,
    os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    language_name,
    adjust_network,
    install_source,
    app_name,
    app_version,
    SUM(dau) AS dau
  FROM
    _base
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
),
_previous AS (
  SELECT
    segment,
    attribution_medium,
    attribution_source,
    attributed,
    city,
    country,
    distribution_id,
    first_seen_year,
    is_default_browser,
    channel,
    os,
    os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    language_name,
    adjust_network,
    install_source,
    app_name,
    app_version,
    'UNDETERMINED' AS device_manufacturer,
    dau
  FROM
    `{{ project_id }}.{{ app_name }}.active_users_aggregates`
  WHERE
    submission_date >= '2021-01-01'
),
-- Get the difference in metrics for all dimensions
_changed AS (
  SELECT
    _previous.* EXCEPT (dau),
    _previous.dau - base_agg.dau AS dau
  FROM
    _previous
  INNER JOIN
    base_agg
  ON
    _previous.submission_date = base_agg.submission_date
    AND _previous.country IS NOT DISTINCT FROM base_agg.country
    AND _previous.segment IS NOT DISTINCT FROM base_agg.segment
    AND _previous.attribution_medium IS NOT DISTINCT FROM base_agg.attribution_medium
    AND _previous.attribution_source IS NOT DISTINCT FROM base_agg.attribution_source
    AND _previous.attributed IS NOT DISTINCT FROM base_agg.attributed
    AND _previous.city IS NOT DISTINCT FROM base_agg.city
    AND _previous.country IS NOT DISTINCT FROM base_agg.country
    AND _previous.distribution_id IS NOT DISTINCT FROM base_agg.distribution_id
    AND _previous.first_seen_year IS NOT DISTINCT FROM base_agg.first_seen_year
    AND _previous.is_default_browser IS NOT DISTINCT FROM base_agg.is_default_browser
    AND _previous.channel IS NOT DISTINCT FROM base_agg.channel
    AND _previous.os IS NOT DISTINCT FROM base_agg.os
    AND _previous.os_version IS NOT DISTINCT FROM base_agg.os_version
    AND _previous.os_version_major IS NOT DISTINCT FROM base_agg.os_version_major
    AND _previous.os_version_minor IS NOT DISTINCT FROM base_agg.os_version_minor
    AND _previous.language_name IS NOT DISTINCT FROM base_agg.language_name
    AND _previous.adjust_network IS NOT DISTINCT FROM base_agg.adjust_network
    AND _previous.install_source IS NOT DISTINCT FROM base_agg.install_source
    AND _previous.app_name IS NOT DISTINCT FROM base_agg.app_name
    AND _previous.app_version IS NOT DISTINCT FROM base_agg.app_version
),
-- Get from the existing aggregates the dimensions that are not returned by the query
_removed AS (
  SELECT
    _previous.* EXCEPT (dau),
    _previous.dau - base_agg.dau AS dau
  FROM
    _previous
  LEFT JOIN
    base_agg
  ON
    _previous.submission_date = base_agg.submission_date
    AND _previous.country IS NOT DISTINCT FROM base_agg.country
    AND _previous.segment IS NOT DISTINCT FROM base_agg.segment
    AND _previous.attribution_medium IS NOT DISTINCT FROM base_agg.attribution_medium
    AND _previous.attribution_source IS NOT DISTINCT FROM base_agg.attribution_source
    AND _previous.attributed IS NOT DISTINCT FROM base_agg.attributed
    AND _previous.city IS NOT DISTINCT FROM base_agg.city
    AND _previous.country IS NOT DISTINCT FROM base_agg.country
    AND _previous.distribution_id IS NOT DISTINCT FROM base_agg.distribution_id
    AND _previous.first_seen_year IS NOT DISTINCT FROM base_agg.first_seen_year
    AND _previous.is_default_browser IS NOT DISTINCT FROM base_agg.is_default_browser
    AND _previous.channel IS NOT DISTINCT FROM base_agg.channel
    AND _previous.os IS NOT DISTINCT FROM base_agg.os
    AND _previous.os_version IS NOT DISTINCT FROM base_agg.os_version
    AND _previous.os_version_major IS NOT DISTINCT FROM base_agg.os_version_major
    AND _previous.os_version_minor IS NOT DISTINCT FROM base_agg.os_version_minor
    AND _previous.language_name IS NOT DISTINCT FROM base_agg.language_name
    AND _previous.adjust_network IS NOT DISTINCT FROM base_agg.adjust_network
    AND _previous.install_source IS NOT DISTINCT FROM base_agg.install_source
    AND _previous.app_name IS NOT DISTINCT FROM base_agg.app_name
    AND _previous.app_version IS NOT DISTINCT FROM base_agg.app_version
  WHERE
    base_agg.submission_date IS NULL
)
SELECT
  *
FROM
  _changed
UNION ALL
SELECT
  *
FROM
  _removed
UNION ALL
{{ end_init }}
SELECT
  *
FROM
  _base
