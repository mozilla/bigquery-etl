------ DESKTOP SPONSORED TILES
WITH newtab_unnested AS (
  SELECT AS STRUCT
    t.client_info.client_id,
    date(t.submission_timestamp) AS submission_date,
    t.sample_id,
    s.name,
    s.category,
    s.extra
  FROM
    `mozdata.firefox_desktop.newtab` t
  CROSS JOIN
    UNNEST(t.events) s
  WHERE
    date(t.submission_timestamp) = @submission_date
),
desktop_events_1 AS (
  -- desktop tiles clicks and impressions
  SELECT
    client_id,
    submission_date,
    COUNTIF(
      name = "click"
      AND category = "topsites"
      AND mozfun.map.get_key(extra, "is_sponsored") = "true"
    ) AS sponsored_tiles_click_count,
    COUNTIF(
      name = "impression"
      AND category = "topsites"
      AND mozfun.map.get_key(extra, "is_sponsored") = "true"
    ) AS sponsored_tiles_impression_count,
  FROM
    newtab_unnested
  WHERE
    submission_date = @submission_date
  GROUP BY
    1,
    2
),
desktop_events_2 AS (
-- desktop Sponsored Tile Dismissals and Disables
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
    COUNTIF(
      event = 'BLOCK'
      AND value LIKE '%spoc%'
      AND source = 'TOP_SITES'
    ) AS sponsored_tiles_dismissal_count,
    COUNTIF(
      event = 'PREF_CHANGED'
      AND source = 'SPONSORED_TOP_SITES'
      AND value LIKE '%false%'
    ) AS sponsored_tiles_disable_count
  FROM
    `mozdata.activity_stream.events`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
desktop_clients AS (
  SELECT
    os,
    submission_date,
    client_id,
    browser_version_info,
    experiments,
    country,
    locale,
    normalized_channel,
    normalized_os_version,
    profile_age_in_days,
    sample_id
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date = @submission_date
  -- Desktop Sponsored Tiles is only available for the following clients:
    AND country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"])
    AND browser_version_info.major_version >= 92
    AND browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0')
),
------ iOS SPONSORED TILES METRICS
ios_events AS (
  -- iOS clicks and impressions
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    COUNTIF(
      event_category LIKE 'top_site%'
      AND event_name = 'contile_click'
    ) AS sponsored_tiles_click_count,
    COUNTIF(
      event_category LIKE 'top_site%'
      AND event_name = 'contile_impression'
    ) AS sponsored_tiles_impression_count,
    COUNTIF(
      event_category = 'preferences'
      AND event_name = "changed"
      AND `mozfun.map.get_key`(event_extra, 'preference') = 'sponsoredTiles'
      AND `mozfun.map.get_key`(event_extra, 'changed_to') = 'false'
    ) AS sponsored_tiles_disables_count
  FROM
    `mozdata.firefox_ios.events_unnested` events
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
ios_clients AS (
  SELECT
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS submission_date,
    client_info.client_id AS client_id,
    ARRAY_AGG(client_info.app_display_version ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS browser_version_info,
    mozfun.map.mode_last(
      ARRAY_CONCAT_AGG(
        mozfun.glean.legacy_compatible_experiments(ping_info.experiments)
        ORDER BY
          submission_timestamp
      )
    ) AS experiments,
    ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp DESC)[OFFSET(0)] AS country,
    ARRAY_AGG(client_info.locale ORDER BY submission_timestamp DESC)[OFFSET(0)] AS locale,
    ARRAY_AGG(normalized_channel ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(normalized_os_version ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS normalized_os_version,
    ARRAY_AGG(sample_id ORDER BY submission_timestamp DESC)[OFFSET(0)] AS sample_id
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
  -- iOS Sponsored Tiles is only available for the following clients:
    AND normalized_country_code IN UNNEST(["US"])
    AND `mozfun.norm.browser_version_info`(client_info.app_display_version).major_version >= 101
  GROUP BY
    client_info.client_id
),
android_events AS (
  -- Android clicks and impressions
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    COUNTIF(
      event_category = 'top_sites'
      AND event_name = 'contile_click'
    ) AS sponsored_tiles_click_count,
    COUNTIF(
      event_category = 'top_sites'
      AND event_name = 'contile_impression'
    ) AS sponsored_tiles_impression_count,
    COUNTIF(
      event_category = 'customize_home'
      AND event_name = "preference_toggled"
      AND `mozfun.map.get_key`(event_extra, 'preference_key') = 'contile'
      AND `mozfun.map.get_key`(event_extra, 'enabled') = 'false'
    ) AS sponsored_tiles_disable_count
  FROM
    `mozdata.fenix.events_unnested` events
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
android_metrics AS (
  SELECT
    client_info.client_id,
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS submission_date,
    ARRAY_AGG(metrics.boolean.customize_home_contile ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS sponsored_tiles_enabled_at_startup
  FROM
    `mozdata.fenix.metrics`
  WHERE
    metrics.boolean.customize_home_contile IS NOT NULL
    AND DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_info.client_id
),
android_clients AS (
  SELECT
    ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS submission_date,
    client_info.client_id AS client_id,
    ARRAY_AGG(client_info.app_display_version ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS browser_version_info,
    mozfun.map.mode_last(
      ARRAY_CONCAT_AGG(
        mozfun.glean.legacy_compatible_experiments(ping_info.experiments)
        ORDER BY
          submission_timestamp
      )
    ) AS experiments,
    ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp DESC)[OFFSET(0)] AS country,
    ARRAY_AGG(client_info.locale ORDER BY submission_timestamp DESC)[OFFSET(0)] AS locale,
    ARRAY_AGG(normalized_channel ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(normalized_os_version ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS normalized_os_version,
    ARRAY_AGG(sample_id ORDER BY submission_timestamp DESC)[OFFSET(0)] AS sample_id
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
  -- Android Sponsored Tiles is only available for the following clients:
    AND normalized_country_code IN UNNEST(["US"])
    AND `mozfun.norm.browser_version_info`(client_info.app_display_version).major_version >= 100
  GROUP BY
    client_info.client_id
)
-- merge on measures by client
-- desktop
SELECT
  @submission_date AS submission_date,
  "desktop" AS device,
  os,
  client_id,
  browser_version_info,
  experiments,
  country,
  locale,
  normalized_channel,
  normalized_os_version,
  profile_age_in_days,
  sample_id,
  COALESCE(sponsored_tiles_click_count, 0) AS sponsored_tiles_click_count,
  COALESCE(sponsored_tiles_impression_count, 0) AS sponsored_tiles_impression_count,
  sponsored_tiles_dismissal_count,
  sponsored_tiles_disable_count,
  NULL AS sponsored_tiles_enabled_at_startup
FROM
  desktop_events_1
FULL JOIN
  desktop_events_2
USING
  (submission_date, client_id)
INNER JOIN
  desktop_clients
USING
  (submission_date, client_id)
UNION ALL
-- ios
SELECT
  @submission_date AS submission_date,
  "mobile" AS device,
  "iOS" AS os,
  client_id,
  `mozfun.norm.browser_version_info`(browser_version_info) AS browser_version_info,
  experiments,
  country,
  locale,
  normalized_channel,
  normalized_os_version,
  profile_age_in_days,
  sample_id,
  sponsored_tiles_click_count,
  sponsored_tiles_impression_count,
  NULL AS sponsored_tiles_dismissal_count,
  NULL AS sponsored_tiles_disable_count,
  NULL AS sponsored_tiles_enabled_at_startup
FROM
  ios_events
INNER JOIN
  ios_clients
USING
  (submission_date, client_id)
LEFT JOIN
  (
    SELECT
      submission_date,
      client_id,
      days_since_created_profile AS profile_age_in_days
    FROM
      `moz-fx-data-shared-prod.firefox_ios.clients_last_seen_joined`
    WHERE
      submission_date = @submission_date
  ) profile_age_data
USING
  (submission_date, client_id)
UNION ALL
SELECT
  @submission_date AS submission_date,
  "mobile" AS device,
  "Android" AS os,
  client_id,
  `mozfun.norm.browser_version_info`(browser_version_info) AS browser_version_info,
  experiments,
  country,
  locale,
  normalized_channel,
  normalized_os_version,
  profile_age_in_days,
  sample_id,
  sponsored_tiles_click_count,
  sponsored_tiles_impression_count,
  NULL AS sponsored_tiles_dismissal_count,
  NULL AS sponsored_tiles_disable_count,
  sponsored_tiles_enabled_at_startup
FROM
  android_events
INNER JOIN
  android_clients
USING
  (submission_date, client_id)
LEFT JOIN
  (
    SELECT
      submission_date,
      client_id,
      days_since_created_profile AS profile_age_in_days
    FROM
      `moz-fx-data-shared-prod.fenix.clients_last_seen_joined`
    WHERE
      submission_date = @submission_date
  ) profile_age_data
USING
  (submission_date, client_id)
LEFT JOIN
  android_metrics
USING
  (submission_date, client_id)
