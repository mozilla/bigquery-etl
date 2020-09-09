{{ header }}
WITH extracted AS (
  SELECT
    client_id,
    channel,
    app_version
  FROM
    {{ source_table }}
  WHERE
    submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND channel IS NOT NULL
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
