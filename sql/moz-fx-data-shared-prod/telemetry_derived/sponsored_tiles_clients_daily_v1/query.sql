  ------ DESKTOP SPONSORED TILES
WITH clicks_main AS (
  SELECT
    client_id,
    submission_date,
    browser_version_info,
    country,
    locale,
    normalized_channel,
    normalized_os,
    sample_id,
    SUM(sponsored_tiles_click_count) AS sponsored_tiles_click_count
  FROM
    (
      SELECT AS STRUCT
        d.client_id,
        DATE(d.submission_timestamp) AS submission_date,
        d.application.display_version AS browser_version_info,
        d.normalized_country_code AS country,
        d.environment.system.os.locale,
        d.normalized_channel,
        d.normalized_os,
        d.sample_id,
        e.value AS sponsored_tiles_click_count
      FROM
        `mozdata.telemetry.main` d
      CROSS JOIN
        UNNEST(d.payload.processes.parent.keyed_scalars.contextual_services_topsites_click) e
      WHERE
        DATE(d.submission_timestamp) = @submission_date
        AND d.normalized_country_code IN UNNEST(
          ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"]
        )
        AND `mozfun.norm.browser_version_info`(d.application.display_version).major_version >= 92
        AND d.application.display_version NOT IN ('92', '92.', '92.0', '92.0.0')
    ) clicks_main
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
),
impressions_main AS (
  SELECT
    client_id,
    submission_date,
    browser_version_info,
    country,
    locale,
    normalized_channel,
    normalized_os,
    sample_id,
    SUM(sponsored_tiles_impression_count) AS sponsored_tiles_impression_count
  FROM
    (
      SELECT AS STRUCT
        g.client_id,
        DATE(g.submission_timestamp) AS submission_date,
        g.application.display_version AS browser_version_info,
        g.normalized_country_code AS country,
        g.environment.system.os.locale,
        g.normalized_channel,
        g.normalized_os,
        g.sample_id,
        h.value AS sponsored_tiles_impression_count
      FROM
        `mozdata.telemetry.main` g
      CROSS JOIN
        UNNEST(g.payload.processes.parent.keyed_scalars.contextual_services_topsites_impression) h
      WHERE
        DATE(g.submission_timestamp) = @submission_date
        AND g.normalized_country_code IN UNNEST(
          ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"]
        )
        AND `mozfun.norm.browser_version_info`(g.application.display_version).major_version >= 92
        AND g.application.display_version NOT IN ('92', '92.', '92.0', '92.0.0')
    ) impressions_main
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
),
  ------ DESKTOP clicks + impressions
desktop_main_events AS (
  SELECT
    *
  FROM
    clicks_main
  FULL JOIN
    impressions_main
  USING
    (
      client_id,
      submission_date,
      browser_version_info,
      country,
      locale,
      normalized_channel,
      normalized_os,
      sample_id
    )
),
  ------ DESKTOP Dismissals and Disables
desktop_activity_stream_events AS (
  SELECT
    client_id,
    date(submission_timestamp) AS submission_date,
    version AS browser_version_info,
    normalized_country_code AS country,
    locale,
    normalized_channel,
    normalized_os,
    sample_id,
    COUNTIF(
      event = 'BLOCK'
      AND value LIKE '%spoc%'
      AND SOURCE = 'TOP_SITES'
    ) AS sponsored_tiles_dismissal_count,
    COUNTIF(
      event = 'PREF_CHANGED'
      AND SOURCE = 'SPONSORED_TOP_SITES'
      AND value LIKE '%false%'
    ) AS sponsored_tiles_disable_count
  FROM
    `mozdata.activity_stream.events`
  WHERE
    date(submission_timestamp) = @submission_date
    AND normalized_country_code IN UNNEST(
      ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"]
    )
    AND `mozfun.norm.browser_version_info`(metadata.user_agent.version).major_version >= 92
    AND metadata.user_agent.version NOT IN ('92', '92.', '92.0', '92.0.0')
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
),
------ iOS SPONSORED TILES
ios_data AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.app_display_version AS browser_version_info,
    -- mozfun.map.mode_last(mozfun.glean.legacy_compatible_experiments(ping_info.experiments)) as experiments,
    normalized_country_code AS country,
    client_info.locale,
    normalized_channel,
    mozfun.norm.os(client_info.os) AS normalized_os,
    sample_id,
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
    ) AS sponsored_tiles_disable_count
  FROM
    `mozdata.firefox_ios.events_unnested` events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_country_code IN UNNEST(["US"])
    AND `mozfun.norm.browser_version_info`(client_info.app_display_version).major_version >= 101
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
),
--- ANDROID SPONSORED TILES
android_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.app_display_version AS browser_version_info,
    normalized_country_code AS country,
    client_info.locale,
    normalized_channel,
    mozfun.norm.os(client_info.os) AS normalized_os,
    sample_id,
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
    AND normalized_country_code IN UNNEST(["US"])
    AND `mozfun.norm.browser_version_info`(client_info.app_display_version).major_version >= 100
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
),
  -- Android enabled at start
android_metrics AS (
  SELECT
    ARRAY_AGG(date(submission_timestamp) ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS submission_date,
    client_info.client_id,
    ARRAY_AGG(client_info.app_display_version ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS browser_version_info,
    ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp DESC)[OFFSET(0)] AS country,
    ARRAY_AGG(client_info.locale ORDER BY submission_timestamp DESC)[OFFSET(0)] AS locale,
    ARRAY_AGG(normalized_channel ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(mozfun.norm.os(client_info.os) ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS normalized_os,
    ARRAY_AGG(sample_id ORDER BY submission_timestamp DESC)[OFFSET(0)] AS sample_id,
    ARRAY_AGG(metrics.boolean.customize_home_contile ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS sponsored_tiles_enabled_at_startup
  FROM
    `mozdata.fenix.metrics`
  WHERE
    metrics.boolean.customize_home_contile IS NOT NULL
    AND DATE(submission_timestamp) = @submission_date
    AND normalized_country_code IN UNNEST(["US"])
    AND `mozfun.norm.browser_version_info`(client_info.app_display_version).major_version >= 100
  GROUP BY
    client_info.client_id
)
----- CREATE MERGED DATASET
----------------  desktop
SELECT
  @submission_date AS submission_date,
  "desktop" AS device,
  client_id,
  `mozfun.norm.browser_version_info`(browser_version_info) AS browser_version_info,
  experiments,
  country,
  locale,
  normalized_channel,
  normalized_os,
  profile_age_in_days,
  sample_id,
  sponsored_tiles_click_count,
  sponsored_tiles_impression_count,
  sponsored_tiles_dismissal_count,
  sponsored_tiles_disable_count,
  NULL AS sponsored_tiles_enabled_at_startup
FROM
  desktop_main_events
FULL JOIN
  desktop_activity_stream_events
USING
  (
    client_id,
    submission_date,
    browser_version_info,
    country,
    locale,
    normalized_channel,
    normalized_os,
    sample_id
  )
--- add experiments and new profile data
LEFT JOIN
  (
    SELECT
      mozfun.norm.os(os) AS normalized_os,
      submission_date,
      client_id,
      app_display_version AS browser_version_info,
      experiments,
      country,
      locale,
      normalized_channel,
      profile_age_in_days,
      sample_id
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date = @submission_date
  -- Desktop Sponsored Tiles is only available for the following clients:
      AND country IN UNNEST(
        ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"]
      )
      AND browser_version_info.major_version >= 92
      AND browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0')
  )
USING
  (
    normalized_os,
    submission_date,
    client_id,
    browser_version_info,
    country,
    locale,
    normalized_channel,
    sample_id
  )
UNION ALL
---------------- ios
SELECT
  @submission_date AS submission_date,
  "mobile" AS device,
  client_id,
  `mozfun.norm.browser_version_info`(browser_version_info) AS browser_version_info,
  experiments,
  country,
  locale,
  normalized_channel,
  normalized_os,
  profile_age_in_days,
  sample_id,
  sponsored_tiles_click_count,
  sponsored_tiles_impression_count,
  NULL AS sponsored_tiles_dismissal_count,
  sponsored_tiles_disable_count,
  NULL AS sponsored_tiles_enabled_at_startup
FROM
  ios_data
--- add experiments data
LEFT JOIN
  (
    SELECT
      client_info.client_id AS client_id,
      ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp DESC)[
        OFFSET(0)
      ] AS submission_date,
      mozfun.map.mode_last(
        ARRAY_CONCAT_AGG(
          mozfun.glean.legacy_compatible_experiments(ping_info.experiments)
          ORDER BY
            submission_timestamp
        )
      ) AS experiments
    FROM
      `mozdata.firefox_ios.events_unnested`
    WHERE
      DATE(submission_timestamp) = @submission_date
      AND normalized_country_code IN UNNEST(["US"])
      AND `mozfun.norm.browser_version_info`(client_info.app_display_version).major_version >= 101
    GROUP BY
      client_info.client_id
  ) experiments_info
USING
  (submission_date, client_id)
--- add profile age in days
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
---------------- android
SELECT
  @submission_date AS submission_date,
  "mobile" AS device,
  client_id,
  `mozfun.norm.browser_version_info`(browser_version_info) AS browser_version_info,
  experiments,
  country,
  locale,
  normalized_channel,
  normalized_os,
  profile_age_in_days,
  sample_id,
  sponsored_tiles_click_count,
  sponsored_tiles_impression_count,
  NULL AS sponsored_tiles_dismissal_count,
  sponsored_tiles_disable_count,
  sponsored_tiles_enabled_at_startup
FROM
  android_events
--- add metrics data
FULL JOIN
  android_metrics
USING
  (
    submission_date,
    client_id,
    browser_version_info,
    country,
    locale,
    normalized_channel,
    normalized_os,
    sample_id
  )
--- add experiments data
LEFT JOIN
  (
    SELECT
      client_id,
      submission_date,
      mozfun.map.mode_last(ARRAY_CONCAT_AGG(experiments ORDER BY submission_date)) AS experiments
    FROM
      (
        SELECT
          client_info.client_id AS client_id,
          ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp DESC)[
            OFFSET(0)
          ] AS submission_date,
          mozfun.map.mode_last(
            ARRAY_CONCAT_AGG(
              mozfun.glean.legacy_compatible_experiments(ping_info.experiments)
              ORDER BY
                submission_timestamp
            )
          ) AS experiments
        FROM
          `mozdata.fenix.events_unnested`
        WHERE
          DATE(submission_timestamp) = @submission_date
          AND normalized_country_code IN UNNEST(["US"])
          AND `mozfun.norm.browser_version_info`(
            client_info.app_display_version
          ).major_version >= 101
        GROUP BY
          client_info.client_id
        UNION ALL
        SELECT
          client_info.client_id AS client_id,
          ARRAY_AGG(DATE(submission_timestamp) ORDER BY submission_timestamp DESC)[
            OFFSET(0)
          ] AS submission_date,
          mozfun.map.mode_last(
            ARRAY_CONCAT_AGG(
              mozfun.glean.legacy_compatible_experiments(ping_info.experiments)
              ORDER BY
                submission_timestamp
            )
          ) AS experiments
        FROM
          `moz-fx-data-shared-prod.fenix.metrics`
        WHERE
          DATE(submission_timestamp) = @submission_date
          AND normalized_country_code IN UNNEST(["US"])
          AND `mozfun.norm.browser_version_info`(
            client_info.app_display_version
          ).major_version >= 101
        GROUP BY
          client_info.client_id
      )
    GROUP BY
      client_id,
      submission_date
  ) merged_experiments
USING
  (submission_date, client_id)
--- add profile age in days
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
