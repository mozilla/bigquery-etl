WITH extracted AS (
  SELECT
    submission_timestamp,
    ecosystem_user_id,
    JSON_EXTRACT_SCALAR(additional_properties, '$.event') AS event,
    JSON_EXTRACT_SCALAR(additional_properties, '$.country') AS country,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts.account_ecosystem`
),
daily AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    ecosystem_user_id,
    COUNTIF(event = 'oauth.token.created') AS oauth_token_created_count,
    mozfun.stats.mode_last(ARRAY_AGG(country)) AS country_name,
  FROM
    extracted
  WHERE
    DATE(submission_timestamp) = '2020-09-07'
  GROUP BY
    submission_date,
    ecosystem_user_id
)
SELECT
  daily.*,
  cn.code AS country_code
FROM
  daily
LEFT JOIN
  static.country_names_v1 AS cn
ON
  daily.country_name = cn.name
LIMIT
  1000
