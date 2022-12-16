WITH _current AS (
  SELECT DISTINCT
    user_id,
    -- TOOD: should this should be dropped? We have fxa_users_services_first_seen to answer this kind of question.
    -- ARRAY_AGG(DISTINCT service IGNORE NULLS) AS services_used,ß
  FROM
    -- fxa_users_daily already filters out for the following events:
    -- event_category IN ('auth', 'auth_bounce', 'content', 'oauth')
    `firefox_accounts.fxa_users_daily`
  WHERE
    submission_date = @submission_date
),
_previous AS (
  SELECT DISTINCT
    user_id
  FROM
    `firefox_accounts.fxa_users_first_seen`
  WHERE
    -- In reprocessing scenarios, we must always backfill from the first affected date
    -- all the way to the present; to enforce that, we explicitly drop any data after
    -- the target @submission_date
    first_seen_date < @submission_date
)
SELECT
  user_id,
  DATE(@submission_date) AS first_seen_date,
FROM
  _current
WHERE
  user_id NOT IN (SELECT user_id FROM _previous)
