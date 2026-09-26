-- combine baseline pings FROM different products
WITH product_union AS (
  -- Firefox for Android
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS `datetime`,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
  UNION ALL
  -- Firefox for iOS
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS `datetime`,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
  UNION ALL
  -- Firefox Focus for Android
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS `datetime`,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
  UNION ALL
  -- Firefox Focus for iOS
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS `datetime`,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
  UNION ALL
  -- Firefox Klar for Android
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.klar_android.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
  UNION ALL
  -- Firefox Klar for iOS
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS `datetime`,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.klar_ios.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
  UNION ALL
  -- Mozilla VPN
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) `datetime`,
    metadata.isp.name AS isp_name,
    ping_info.end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.mozilla_vpn.baseline`
  WHERE
    DATE(submission_timestamp)
    BETWEEN "2024-06-20"
    AND "2024-06-29"
    AND (metadata.geo.country IS NOT NULL OR metadata.geo.country <> "??")
),
update_values AS (
  SELECT
    * REPLACE (
      IF(city = "??" OR city IS NULL, "unknown", city) AS city,
      NULLIF(geo_subdivision1, "??") AS geo_subdivision1,
      NULLIF(geo_subdivision2, "??") AS geo_subdivision2
    ),
    CASE
      WHEN REGEXP_CONTAINS(
          end_time,
          "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-|+]\\d{2}:\\d{2}"
        )
        THEN TIMESTAMP(end_time)
      WHEN REGEXP_CONTAINS(end_time, "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}[-|+]\\d{2}:\\d{2}")
        THEN PARSE_TIMESTAMP("%Y-%m-%dT%H:%M%Ez", end_time)
      ELSE NULL
    END AS parsed_end_time,
  FROM
    product_union
)
SELECT
  country,
  city,
  geo_subdivision1,
  geo_subdivision2,
  isp_name,
  `datetime`,
  COUNT(*) AS ping_count,
  AVG(
    ABS(TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, MINUTE))
  ) AS avg_ping_arrival_delay_in_minutes,
FROM
  update_values
GROUP BY
  country,
  city,
  geo_subdivision1,
  geo_subdivision2,
  isp_name,
  `datetime`
HAVING
  ping_count > 50
