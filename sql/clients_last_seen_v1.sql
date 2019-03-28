WITH
  _current AS (
  SELECT
    * EXCEPT (submission_date_s3),
    0 AS days_since_seen,
    -- For measuring Active MAU, where this is the day since this
    -- client_id was an Active User as defined by
    -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
    IF(scalar_parent_browser_engagement_total_uri_count_sum >= 5,
      0,
      NULL) AS days_since_visited_5_uri
  FROM
    clients_daily_v6
  WHERE
    submission_date_s3 = @submission_date ),
  _previous AS (
  SELECT
    * EXCEPT (submission_date) REPLACE(
      -- omit values outside 28 day window
      IF(days_since_visited_5_uri < 27,
        days_since_visited_5_uri,
        NULL) AS days_since_visited_5_uri)
  FROM
    clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND clients_last_seen_v1.days_since_seen < 27 )
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL,
    _current,
    _previous).* EXCEPT (days_since_seen,
      days_since_visited_5_uri),
  COALESCE(_current.days_since_seen,
    _previous.days_since_seen + 1) AS days_since_seen,
  COALESCE(_current.days_since_visited_5_uri,
    _previous.days_since_visited_5_uri + 1) AS days_since_visited_5_uri
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
