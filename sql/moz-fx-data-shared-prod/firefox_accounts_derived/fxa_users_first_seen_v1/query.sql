WITH _current AS (
  SELECT
    user_id,
    ARRAY_AGG(DISTINCT service) AS services_used,
  FROM
    firefox_accounts.fxa_all_events
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    user_id
),
_previous AS (
  SELECT
    *
  FROM
    firefox_accounts_derived.fxa_users_first_seen_v1
)
SELECT
  user_id,
  COALESCE(_previous.first_seen_date, @submission_date) AS first_seen_date,
  ARRAY(
    SELECT DISTINCT service
    FROM UNNEST(COALESCE(_previous.services_used, []) || COALESCE(_current.services_used, [])) AS service
    WHERE service IS NOT NULL
  ) AS services_used,
FROM
  _previous
FULL OUTER JOIN
  _current
USING
  (user_id)
