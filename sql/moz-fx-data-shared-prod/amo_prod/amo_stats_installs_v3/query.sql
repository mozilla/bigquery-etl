WITH install_stats AS (
  SELECT
    client_id,
    event_string_value AS hashed_addon_id,
    submission_date,
    mozfun.map.get_key(event_map_values, 'utm_content') AS utm_content,
    mozfun.map.get_key(event_map_values, 'utm_campaign') AS utm_campaign,
    mozfun.map.get_key(event_map_values, 'utm_source') AS utm_source,
    mozfun.map.get_key(event_map_values, 'utm_medium') AS utm_medium,
  FROM
    telemetry.events
  WHERE
    event_category = 'addonsManager'
    AND event_method = 'install_stats'
    AND submission_date = @submission_date
  GROUP BY
    client_id,
    hashed_addon_id,
    submission_date,
    utm_campaign,
    utm_content,
    utm_source,
    utm_medium
),
per_source AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(key, value) ORDER BY value DESC) AS downloads_per_source
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_source AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        key
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
--
per_content AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(key, value) ORDER BY value DESC) AS downloads_per_content
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_content AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        key
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
--
per_medium AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(key, value) ORDER BY value DESC) AS downloads_per_medium
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_medium AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        key
    )
  GROUP BY
    submission_date,
    hashed_addon_id
),
--
per_campaign AS (
  SELECT
    hashed_addon_id,
    submission_date,
    ARRAY_AGG(STRUCT(key, value) ORDER BY value DESC) AS downloads_per_campaign
  FROM
    (
      SELECT
        hashed_addon_id,
        submission_date,
        utm_campaign AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        install_stats
      GROUP BY
        submission_date,
        hashed_addon_id,
        key
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
    install_stats
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
