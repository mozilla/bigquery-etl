WITH extracted AS (
  SELECT
    client_info.client_id,
    client_info.app_channel AS channel,
    COALESCE(
      SAFE_CAST(SPLIT(client_info.app_display_version, '.')[OFFSET(0)] AS INT64),
      0
    ) AS app_version,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.baseline_v1`
  WHERE
    DATE(submission_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND client_info.app_channel IS NOT NULL
),
transformed AS (
  SELECT
    channel,
    app_version
  FROM
    extracted
  GROUP BY
    channel,
    app_version
  HAVING
    COUNT(DISTINCT client_id) > 100
  ORDER BY
    channel,
    app_version DESC
)
SELECT
  channel,
  MAX(app_version) AS latest_version
FROM
  transformed
GROUP BY
  channel
