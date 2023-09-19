WITH _current AS (
  SELECT
    user_id,
    ARRAY_AGG(DISTINCT service IGNORE NULLS) AS services_used,
  FROM
    firefox_accounts.fxa_all_events
  WHERE
    DATE(`timestamp`) = @submission_date
    AND fxa_log IN ('auth', 'auth_bounce', 'content', 'oauth')
  GROUP BY
    user_id
),
_previous AS (
  SELECT
    *
  FROM
    firefox_accounts_derived.fxa_users_first_seen_v1
  WHERE
    -- In reprocessing scenarios, we must always backfill from the first affected date
    -- all the way to the present; to enforce that, we explicitly drop any data after
    -- the target @submission_date
    first_seen_date < @submission_date
)
SELECT
  user_id,
  COALESCE(_previous.first_seen_date, @submission_date) AS first_seen_date,
  ARRAY(
    SELECT DISTINCT
      service
    FROM
      UNNEST(
        ARRAY_CONCAT(COALESCE(_previous.services_used, []), COALESCE(_current.services_used, []))
      ) AS service
    WHERE
      service IS NOT NULL
    ORDER BY
      service
  ) AS services_used,
FROM
  _previous
FULL OUTER JOIN
  _current
USING
  (user_id)
