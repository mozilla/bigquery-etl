CREATE TEMP FUNCTION extract_addon_info(addon_info ANY TYPE)
RETURNS STRUCT<
  hashed_addon_id STRING,
  utm_content STRING,
  utm_campaign STRING,
  utm_source STRING,
  utm_medium STRING
> AS (
  STRUCT(
    mozfun.map.get_key(addon_info, "hashed_addon_id") AS hashed_addon_id,
    mozfun.map.get_key(addon_info, "utm_content") AS utm_content,
    mozfun.map.get_key(addon_info, "utm_campaign") AS utm_campaign,
    mozfun.map.get_key(addon_info, "utm_source") AS utm_source,
    mozfun.map.get_key(addon_info, "utm_medium") AS utm_medium
  )
);

WITH install_stats AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    extract_addon_info(event.extra).*,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events`,
    UNNEST(events) AS `event`
  WHERE
    TIMESTAMP_TRUNC(submission_timestamp, DAY) = @submission_date
    AND event.category = "addons_manager"
    AND event.name = "install_stats"
  GROUP BY
    ALL
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
