WITH _current AS (
  SELECT DISTINCT
    user_id,
    -- ARRAY_AGG(DISTINCT service IGNORE NULLS) AS services_used, # TODO: create an alternative agg model
    --   for high-level user aggregated information for quick user overview.
  FROM
    `firefox_accounts.fxa_users_daily`
  WHERE
    submission_date = @submission_date
    -- ensuring we only process rows where user_id is provided
    AND user_id IS NOT NULL
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
    -- it appears we have a user_id will null value exist inside fxa_users_first_seen
    -- excluding it here to avoid it being recreated for each new submission_date
    AND user_id IS NOT NULL
)
SELECT
  user_id,
  DATE(@submission_date) AS first_seen_date,
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  (user_id)
WHERE
  _previous.user_id IS NULL
