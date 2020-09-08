CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_accounts_derived.aet_desktop_clients_daily_v0`
PARTITION BY
  submission_date
CLUSTER BY
  normalized_channel
AS
WITH daily AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    payload.ecosystem_client_id,
    payload.ecosystem_user_id,
    SUM(
      COALESCE(
        payload.duration,
        SAFE_CAST(json_extract_scalar(additional_properties, '$.payload.duration') AS INT64)
      )
    ) AS duration_sum,
    SUM(
      COALESCE(
        payload.scalars.parent.browser_engagement_active_ticks,
        SAFE_CAST(
          JSON_EXTRACT_SCALAR(
            LOWER(additional_properties),
            "$.payload.scalars.parent['browser.engagement.active_ticks']"
          ) AS INT64
        )
      ) / (3600 / 5)
    ) AS active_hours_sum,
    SUM(
      COALESCE(
        payload.scalars.parent.browser_engagement_total_uri_count,
        SAFE_CAST(
          JSON_EXTRACT_SCALAR(
            LOWER(additional_properties),
            "$.payload.scalars.parent['browser.engagement.total_uri_count']"
          ) AS INT64
        )
      )
    ) AS scalar_parent_browser_engagement_total_uri_count_sum,
    mozfun.stats.mode_last(ARRAY_AGG(normalized_channel)) AS normalized_channel,
    mozfun.stats.mode_last(ARRAY_AGG(normalized_os)) AS normalized_os,
    mozfun.stats.mode_last(ARRAY_AGG(normalized_os_version)) AS normalized_os_version,
    mozfun.stats.mode_last(ARRAY_AGG(metadata.geo.country)) AS country_code,
  FROM
    `moz-fx-data-shared-prod.telemetry.account_ecosystem`
  WHERE
    DATE(submission_timestamp) >= '2020-09-01'
  GROUP BY
    submission_date,
    payload.ecosystem_client_id,
    payload.ecosystem_user_id
)
SELECT
  daily.*,
  cn.name AS country_name
FROM
  daily
LEFT JOIN
  `static.country_codes_v1` AS cn
ON
  daily.country_code = cn.code
