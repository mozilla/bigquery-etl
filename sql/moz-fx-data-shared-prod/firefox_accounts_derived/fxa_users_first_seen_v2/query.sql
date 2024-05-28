WITH _current AS (
  SELECT
    user_id,
    country,
    `language`,
    os_name,
    os_version,
    seen_in_tier1_country,
    registered,
  FROM
    firefox_accounts_derived.fxa_users_daily_v2
  WHERE
    submission_date = @submission_date
),
_previous AS (
  SELECT
    user_id
  FROM
    firefox_accounts_derived.fxa_users_first_seen_v2
  WHERE
    -- In reprocessing scenarios, we must always backfill from the first affected date
    -- all the way to the present; to enforce that, we explicitly drop any data after
    -- the target @submission_date
    first_seen_date < @submission_date
)
SELECT
  *,
  @submission_date AS first_seen_date,
FROM
  _current
FULL OUTER JOIN
  _previous
  USING (user_id)
WHERE
  _previous.user_id IS NULL
