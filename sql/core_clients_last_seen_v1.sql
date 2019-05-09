WITH
  _current AS (
  SELECT
    * EXCEPT (submission_date)
    -- Record the days since we recieved any core ping at all from this client.
    0 AS days_since_seen,
    -- Record the days since the client was in a "Tier 1" country;
    -- this allows a variant of country-segmented MAU where we can still count
    -- a client that appeared in one of the target countries in the previous
    -- 28 days even if the most recent "country" value is not in this set.
    IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'),
      0,
      NULL) AS days_since_seen_in_tier1_country
  FROM
    core_clients_daily_v1
  WHERE
    submission_date = @submission_date ),
  _previous AS (
  SELECT
    * EXCEPT (submission_date) REPLACE(
      -- We use REPLACE to null out any days_since observations older than 28 days;
      -- this ensures data never bleeds in from outside the target 28 day window.
      IF(days_since_seen_in_tier1_country < 27,
        days_since_seen_in_tier1_country,
        NULL) AS days_since_seen_in_tier1_country)
  FROM
    core_clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND days_since_seen < 27 )
SELECT
  @submission_date AS submission_date,
  IF(_current.user_id IS NOT NULL,
    _current,
    _previous).* EXCEPT (days_since_seen,
      days_since_seen_in_tier1_country),
  COALESCE(_current.days_since_seen,
    _previous.days_since_seen + 1) AS days_since_seen,
  COALESCE(_current.days_since_seen_in_tier1_country,
    _previous.days_since_seen_in_tier1_country + 1) AS days_since_seen_in_tier1_country
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
