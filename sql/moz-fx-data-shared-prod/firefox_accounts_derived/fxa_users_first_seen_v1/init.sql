CREATE OR REPLACE TABLE
  `firefox_accounts_derived.fxa_users_first_seen_v1`
PARTITION BY
  first_seen_date
CLUSTER BY
  user_id
AS
SELECT
  user_id,
  DATE(MIN(`timestamp`)) AS first_seen_date,
  ARRAY_AGG(DISTINCT service IGNORE NULLS ORDER BY service) AS services_used,
FROM
  firefox_accounts.fxa_all_events
WHERE
  `timestamp` > '2010-01-01'
  AND fxa_log IN ('auth', 'auth_bounce', 'content', 'oauth')
GROUP BY
  user_id
