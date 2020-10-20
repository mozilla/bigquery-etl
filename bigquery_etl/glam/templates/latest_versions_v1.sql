{{ header }}
WITH extracted AS (
  SELECT
    client_id,
    channel,
    app_version
  FROM
    {{ source_table }}
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 28 DAY)
    AND @submission_date
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
    COUNT(DISTINCT client_id) > 5
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
