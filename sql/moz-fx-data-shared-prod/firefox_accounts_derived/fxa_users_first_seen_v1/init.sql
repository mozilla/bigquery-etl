CREATE OR REPLACE TABLE
  `firefox_accounts_derived.fxa_users_first_seen_v1`
PARTITION BY
  first_seen_date
AS
SELECT
  user_id,
  DATE(MIN(`timestamp`)) AS first_seen_date,
FROM
  firefox_accounts.fxa_all_events
WHERE
  `timestamp` > '2010-01-01'
  AND event_category IN ('auth', 'auth_bounce', 'content', 'oauth')
GROUP BY
  user_id
