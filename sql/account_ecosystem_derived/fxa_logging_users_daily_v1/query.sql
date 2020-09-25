WITH daily AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    ecosystem_user_id,
    oauth_client_id,
    COUNT(*) AS event_count,
    mozfun.stats.mode_last(ARRAY_AGG(country)) AS country_name,
  FROM
    firefox_accounts.account_ecosystem
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    ecosystem_user_id,
    oauth_client_id
)
SELECT
  euil.canonical_id,
  daily.*,
  cn.code AS country_code,
FROM
  daily
LEFT JOIN
  ecosystem_user_id_lookup_v1 AS euil
USING
  (ecosystem_user_id)
LEFT JOIN
  static.country_names_v1 AS cn
ON
  daily.country_name = cn.name
