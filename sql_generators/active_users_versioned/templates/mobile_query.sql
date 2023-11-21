DECLARE device_manufacturer STRING DEFAULT "??";

DECLARE _current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

WITH _current_base AS (
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
      device_manufacturer --TODO: New column
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
      device_manufacturer, --TODO: New column,
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
      device_manufacturer ----TODO: New column
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
    _current_timestamp AS last_updated_timestamp,
    COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau,
    COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
    COUNT(DISTINCT client_id) AS mau,
    COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
    SUM(ad_clicks) AS ad_clicks,
    SUM(organic_search_count) AS organic_search_count,
    SUM(search_count) AS search_count,
    SUM(search_with_ads) AS search_with_ads,
    SUM(uri_count) AS uri_count,
    SUM(active_hours_sum) AS active_hours
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
    device_manufacturer,
    last_updated_timestamp
),
_current AS (
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
    SUM(dau) OVER dimensions AS dau,
    SUM(wau) OVER dimensions AS wau,
    SUM(mau) OVER dimensions AS mau,
    SUM(new_profiles) OVER dimensions AS new_profiles,
    SUM(ad_clicks) OVER dimensions AS ad_clicks,
    SUM(organic_search_count) OVER dimensions AS organic_search_count,
    SUM(search_count) OVER dimensions AS search_count,
    SUM(search_with_ads) OVER dimensions AS search_with_ads,
    SUM(uri_count) OVER dimensions AS uri_count,
    SUM(active_hours) OVER dimensions AS active_hours,
    adjust_network,
    install_source,
    app_name,
    app_version,
    device_manufacturer,
    last_updated_timestamp
  FROM
    _current_base
  WINDOW
    dimensions AS (
      PARTITION BY
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
        -- TODO: New column device_manufacturer not added here, until... next backfill??
      ORDER BY
        last_updated_timestamp
    )
),
_previous AS (
  SELECT
    * EXCEPT (
      app_version_major,
      app_version_minor,
      app_version_patch_revision,
      app_version_is_major_release,
      os_grouped
    ),
    device_manufacturer, --TODO: New column. Needs to be in a DECLARE statement so this query doesn't break becuase the column doesn;t exist yet, and continues working after the backfill once the column exists.
    DATE_SUB(
      CURRENT_TIMESTAMP,
      INTERVAL 365 DAY
    ) AS last_updated_timestamp --TODO: This column is expected in the view. Adding it manually to simulate previous backfills.
  FROM
    `{{ project_id }}.{{ app_name }}.active_users_aggregates` -- TODO: Query the view (to retrive records HAVING MAX(last_updated_timestamp))
  WHERE
    submission_date = @submission_date
),
unioned AS (
  SELECT
    * EXCEPT (device_manufacturer)
  FROM
    _previous
  UNION ALL
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
    MAX(dau) AS dau,
    MAX(wau) AS wau,
    MAX(mau) AS mau,
    MAX(new_profiles) AS new_profiles,
    MAX(ad_clicks) AS ad_clicks,
    MAX(organic_search_count) AS organic_search_count,
    MAX(search_count) AS search_count,
    MAX(search_with_ads) AS search_with_ads,
    MAX(uri_count) AS uri_count,
    MAX(active_hours) AS active_hours,
    adjust_network,
    install_source,
    app_name,
    app_version,
    last_updated_timestamp
    -- TODO: New column device_manufacturer not added here, until... next backfill??
  FROM
    _current
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
    last_updated_timestamp
),
backfill_delta AS (
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
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(dau) OVER dimensions - dau) > 0,
      LAG(dau) OVER dimensions - dau,
      NULL
    ) AS dau,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(wau) OVER dimensions - wau) > 0,
      LAG(wau) OVER dimensions - wau,
      NULL
    ) AS wau,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(mau) OVER dimensions - mau) > 0,
      LAG(mau) OVER dimensions - mau,
      NULL
    ) AS mau,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(new_profiles) OVER dimensions - new_profiles) > 0,
      LAG(new_profiles) OVER dimensions - new_profiles,
      NULL
    ) AS new_profiles,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(ad_clicks) OVER dimensions - ad_clicks) > 0,
      LAG(ad_clicks) OVER dimensions - ad_clicks,
      NULL
    ) AS ad_clicks,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(organic_search_count) OVER dimensions - organic_search_count) > 0,
      LAG(organic_search_count) OVER dimensions - organic_search_count,
      NULL
    ) AS organic_search_count,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(search_count) OVER dimensions - search_count) > 0,
      LAG(search_count) OVER dimensions - search_count,
      NULL
    ) AS search_count,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(search_with_ads) OVER dimensions - search_with_ads) > 0,
      LAG(search_with_ads) OVER dimensions - search_with_ads,
      NULL
    ) AS search_with_ads,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(uri_count) OVER dimensions - uri_count) > 0,
      LAG(uri_count) OVER dimensions - uri_count,
      NULL
    ) AS uri_count,
    IF(
      last_updated_timestamp = _current_timestamp
      AND (LAG(active_hours) OVER dimensions - active_hours) > 0,
      LAG(active_hours) OVER dimensions - active_hours,
      NULL
    ) AS active_hours,
    adjust_network,
    install_source,
    app_name,
    app_version,
    '??' AS device_manufacturer, --TODO: New column
    last_updated_timestamp
  FROM
    unioned
  WINDOW
    dimensions AS (
      PARTITION BY
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
        -- TODO: New column device_manufacturer not added here because it doesn't need to match, it's set as '??'. To be updated in next backfill (?)
      ORDER BY
        last_updated_timestamp
    )
),
only_previous AS (
  SELECT
    _previous.*
  FROM
    _previous
  LEFT OUTER JOIN
    _current
  ON
    _previous.submission_date = _current.submission_date
    AND IFNULL(_previous.attribution_medium, 'NULL') = IFNULL(_current.attribution_medium, 'NULL')
    AND IFNULL(_previous.attribution_source, 'NULL') = IFNULL(_current.attribution_source, 'NULL')
    AND IFNULL(_previous.attributed, FALSE) = IFNULL(_current.attributed, FALSE)
    AND IFNULL(_previous.app_version, 'NULL') = IFNULL(_current.app_version, 'NULL')
    AND IFNULL(_previous.attribution_medium, 'NULL') = IFNULL(_current.attribution_medium, 'NULL')
    AND IFNULL(_previous.attribution_source, 'NULL') = IFNULL(_current.attribution_source, 'NULL')
    AND IFNULL(_previous.city, 'NULL') = IFNULL(_current.city, 'NULL')
    AND IFNULL(_previous.country, 'NULL') = IFNULL(_current.country, 'NULL')
    AND IFNULL(_previous.distribution_id, 'NULL') = IFNULL(_current.distribution_id, 'NULL')
    AND IFNULL(_previous.first_seen_year, 0) = IFNULL(_current.first_seen_year, 0)
    AND IFNULL(_previous.is_default_browser, FALSE) = IFNULL(_current.is_default_browser, FALSE)
    AND IFNULL(_previous.language_name, 'NULL') = IFNULL(_current.language_name, 'NULL')
    AND IFNULL(_previous.app_name, 'NULL') = IFNULL(_current.app_name, 'NULL')
    AND IFNULL(_previous.channel, 'NULL') = IFNULL(_current.channel, 'NULL')
    AND IFNULL(_previous.os, 'NULL') = IFNULL(_current.os, 'NULL')
    AND IFNULL(_previous.os_version, 'NULL') = IFNULL(_current.os_version, 'NULL')
    AND IFNULL(_previous.os_version_major, 0) = IFNULL(_current.os_version_major, 0)
    AND IFNULL(_previous.os_version_minor, 0) = IFNULL(_current.os_version_minor, 0)
    AND IFNULL(_previous.segment, 'NULL') = IFNULL(_current.segment, 'NULL')
    AND IFNULL(_previous.adjust_network, 'NULL') = IFNULL(_current.adjust_network, 'NULL')
    AND IFNULL(_previous.install_source, 'NULL') = IFNULL(_current.install_source, 'NULL')
    -- TODO: New column device_manufacturer not added here because it doesn't need to match, it's set as '??'. To be updated in next backfill (?)
  WHERE
    _current.last_updated_timestamp IS NULL
),
_all AS (
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
    SUM(dau) AS dau,
    SUM(wau) AS wau,
    SUM(mau) AS mau,
    SUM(new_profiles) AS new_profiles,
    SUM(ad_clicks) AS ad_clicks,
    SUM(organic_search_count) AS organic_search_count,
    SUM(search_count) AS search_count,
    SUM(search_with_ads) AS search_with_ads,
    SUM(uri_count) AS uri_count,
    SUM(active_hours) AS active_hours,
    adjust_network,
    install_source,
    app_name,
    app_version,
    device_manufacturer,
    last_updated_timestamp
  FROM
    _current_base
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
    device_manufacturer, --TODO: New column
    last_updated_timestamp
  UNION ALL
  SELECT
    *
  FROM
    backfill_delta
  WHERE
    last_updated_timestamp = _current_timestamp
    AND dau IS NOT NULL
  UNION ALL
  SELECT
    *
  FROM
    only_previous
  ORDER BY
    1,
    2,
    3,
    4,
    5
)
SELECT
  *
FROM
  _all
