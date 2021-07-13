CREATE TABLE
  `firefox_accounts_derived.fxa_users_first_seen_v1`
PARTITION BY
  first_seen_date
CLUSTER BY
  user_id
AS
SELECT
  user_id,
  DATE(MIN(submission_timestamp)) AS first_seen_date,
  ARRAY_AGG(DISTINCT service) AS services_used,
FROM
  firefox_accounts.fxa_all_events
WHERE
  submission_timestamp > '2010-01-01'
GROUP BY
  user_id
