-- Combine firefox_desktop install stats from both legacy and glean based data sources.
-- Glean source containing clients using app version 148 and above.
WITH unioned_install_stats AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    extras.string.hashed_addon_id,
    extras.string.utm_content,
    extras.string.utm_campaign,
    extras.string.utm_source,
    extras.string.utm_medium,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND app_version_major >= 148
    AND event_category = "addons_manager"
    AND event_name = "install_stats"
  UNION ALL BY NAME
  -- Legacy source containing clients using app version major below 148.
  SELECT
    submission_date,
    client_id,
    event_string_value AS hashed_addon_id,
    mozfun.map.get_key(event_map_values, 'utm_content') AS utm_content,
    mozfun.map.get_key(event_map_values, 'utm_campaign') AS utm_campaign,
    mozfun.map.get_key(event_map_values, 'utm_source') AS utm_source,
    mozfun.map.get_key(event_map_values, 'utm_medium') AS utm_medium,
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    event_category = 'addonsManager'
    AND mozfun.norm.truncate_version(app_version, "major") < 148
    AND event_method = 'install_stats'
    AND submission_date = @submission_date
),
per_source AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS downloads_per_source
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_source AS `key`,
        COUNT(DISTINCT client_id) AS `value`
      FROM
        unioned_install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
per_content AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS downloads_per_content
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_content AS `key`,
        COUNT(DISTINCT client_id) AS `value`
      FROM
        unioned_install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
per_medium AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS downloads_per_medium
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_medium AS `key`,
        COUNT(DISTINCT client_id) AS `value`
      FROM
        unioned_install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
per_campaign AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS downloads_per_campaign
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_campaign AS `key`,
        COUNT(DISTINCT client_id) AS `value`
      FROM
        unioned_install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
--
total_downloads AS (
  SELECT
    hashed_addon_id,
    submission_date,
    COUNT(DISTINCT client_id) AS total_downloads
  FROM
    unioned_install_stats
  GROUP BY
    hashed_addon_id,
    submission_date
)
SELECT
  *
FROM
  total_downloads
LEFT JOIN
  per_source
  USING (submission_date, hashed_addon_id)
LEFT JOIN
  per_content
  USING (submission_date, hashed_addon_id)
LEFT JOIN
  per_medium
  USING (submission_date, hashed_addon_id)
LEFT JOIN
  per_campaign
  USING (submission_date, hashed_addon_id)
