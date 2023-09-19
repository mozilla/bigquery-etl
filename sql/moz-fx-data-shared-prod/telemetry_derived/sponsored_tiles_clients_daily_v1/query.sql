  ------ DESKTOP SPONSORED TILES
WITH clicks_main AS (
  SELECT
    client_id,
    submission_date,
    SUM(sponsored_tiles_click_count) AS sponsored_tiles_click_count
  FROM
    (
      SELECT AS STRUCT
        d.client_id,
        DATE(d.submission_timestamp) AS submission_date,
        e.value AS sponsored_tiles_click_count
      FROM
        `moz-fx-data-shared-prod.telemetry.main` d
      CROSS JOIN
        UNNEST(d.payload.processes.parent.keyed_scalars.contextual_services_topsites_click) e
      WHERE
        DATE(d.submission_timestamp) = @submission_date
    ) clicks_main
  GROUP BY
    1,
    2
),
impressions_main AS (
  SELECT
    client_id,
    submission_date,
    SUM(sponsored_tiles_impression_count) AS sponsored_tiles_impression_count
  FROM
    (
      SELECT AS STRUCT
        g.client_id,
        DATE(g.submission_timestamp) AS submission_date,
        h.value AS sponsored_tiles_impression_count
      FROM
        `moz-fx-data-shared-prod.telemetry.main` g
      CROSS JOIN
        UNNEST(g.payload.processes.parent.keyed_scalars.contextual_services_topsites_impression) h
      WHERE
        DATE(g.submission_timestamp) = @submission_date
    ) impressions_main
  GROUP BY
    1,
    2
),
  ------ DESKTOP Dismissals and Disables
desktop_activity_stream_events AS (
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
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
    `moz-fx-data-shared-prod.activity_stream.events`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
------ iOS SPONSORED TILES
ios_data AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
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
    `moz-fx-data-shared-prod.firefox_ios.events_unnested` events
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
--- ANDROID SPONSORED TILES
android_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
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
    `moz-fx-data-shared-prod.fenix.events_unnested` events
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
-- ----- CREATE MERGED DATASET
unified_metrics AS (
  SELECT
    submission_date,
    CASE
      WHEN normalized_app_name = "Firefox Desktop"
        THEN "desktop"
      ELSE "mobile"
    END AS device,
    client_id,
    browser_version_info,
    country,
    locale,
    normalized_channel,
    normalized_os,
    is_new_profile,
    sample_id
  FROM
    `moz-fx-data-shared-prod.telemetry.unified_metrics`
  WHERE
    `mozfun`.bits28.active_in_range(days_seen_bits, 0, 1)
    AND submission_date = @submission_date
    AND normalized_app_name IN ("Firefox Desktop", "Fenix", "Firefox iOS")
    AND (
      (
        normalized_app_name = "Firefox Desktop"
        AND country IN UNNEST(
          ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"]
        )
        AND browser_version_info.major_version >= 92
        AND (browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0'))
      )
      OR (
        normalized_app_name = "Firefox iOS"
        AND (country IN UNNEST(["US"]))
        OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
        AND browser_version_info.major_version >= 101
      )
      OR (
        normalized_app_name = "Fenix"
        AND (country IN UNNEST(["US"]))
        OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
        AND browser_version_info.major_version >= 100
      )
    )
)
  --- desktop
SELECT
  @submission_date AS submission_date,
  device,
  client_id,
  browser_version_info,
  country,
  locale,
  normalized_channel,
  normalized_os,
  is_new_profile,
  sample_id,
  experiments,
  COALESCE(sponsored_tiles_click_count, 0) AS sponsored_tiles_click_count,
  COALESCE(sponsored_tiles_impression_count, 0) AS sponsored_tiles_impression_count,
  COALESCE(sponsored_tiles_dismissal_count, 0) AS sponsored_tiles_dismissal_count,
  COALESCE(sponsored_tiles_disable_count, 0) AS sponsored_tiles_disable_count
FROM
  (SELECT * FROM unified_metrics WHERE normalized_os NOT IN ("Android", "iOS")) desktop_unified
LEFT JOIN
  clicks_main
USING
  (client_id, submission_date)
LEFT JOIN
  impressions_main
USING
  (client_id, submission_date)
LEFT JOIN
  desktop_activity_stream_events
USING
  (client_id, submission_date)
-- add experiments data
LEFT JOIN
  (
    SELECT
      submission_date,
      client_id,
      experiments
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date = @submission_date
  )
USING
  (submission_date, client_id)
UNION ALL
  --- iOS
SELECT
  @submission_date AS submission_date,
  device,
  client_id,
  browser_version_info,
  country,
  locale,
  normalized_channel,
  normalized_os,
  is_new_profile,
  sample_id,
  experiments,
  COALESCE(sponsored_tiles_click_count, 0) AS sponsored_tiles_click_count,
  COALESCE(sponsored_tiles_impression_count, 0) AS sponsored_tiles_impression_count,
  NULL AS sponsored_tiles_dismissal_count,
  COALESCE(sponsored_tiles_disable_count, 0) AS sponsored_tiles_disable_count
FROM
  (SELECT * FROM unified_metrics WHERE normalized_os = "iOS") ios_unified
LEFT JOIN
  ios_data
USING
  (submission_date, client_id)
-- add experiments data
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
      `moz-fx-data-shared-prod.firefox_ios.events_unnested`
    WHERE
      DATE(submission_timestamp) = @submission_date
    GROUP BY
      client_info.client_id
  ) experiments_info
USING
  (submission_date, client_id)
UNION ALL
--- Android
SELECT
  @submission_date AS submission_date,
  device,
  client_id,
  browser_version_info,
  country,
  locale,
  normalized_channel,
  normalized_os,
  is_new_profile,
  sample_id,
  experiments,
  COALESCE(sponsored_tiles_click_count, 0) AS sponsored_tiles_click_count,
  COALESCE(sponsored_tiles_impression_count, 0) AS sponsored_tiles_impression_count,
  NULL AS sponsored_tiles_dismissal_count,
  COALESCE(sponsored_tiles_disable_count, 0) AS sponsored_tiles_disable_count
FROM
  (-- note unified_metrics drops known Android bots
    SELECT
      *
    FROM
      unified_metrics
    WHERE
      normalized_os = "Android"
  ) android_unified
LEFT JOIN
  android_events
USING
  (submission_date, client_id)
-- add experiments data
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
      `moz-fx-data-shared-prod.fenix.events_unnested`
    WHERE
      DATE(submission_timestamp) = @submission_date
    GROUP BY
      client_info.client_id
  )
USING
  (submission_date, client_id)
