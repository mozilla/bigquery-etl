WITH history AS (
  SELECT
    *
  FROM
    mozdata.analysis.gclids_v1
),
new_data AS (
  SELECT
    trafficSource.adwordsClickInfo.gclId AS gclid,
    -- We take the first ga_client_id
    MIN_BY(clientId, visitStartTime) AS ga_client_id,
    MIN(visitStartTime) AS min_start_time,
    MIN(PARSE_DATE('%Y%m%d', date)) AS first_seen_date,
    MAX(PARSE_DATE('%Y%m%d', date)) AS last_seen_date,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
  WHERE
    -- Re-process yesterday, to account for late-arriving data
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@session_date, INTERVAL 1 DAY))
    AND FORMAT_DATE('%Y%m%d', @session_date)
    AND trafficSource.adwordsClickInfo.gclId IS NOT NULL
  GROUP BY
    gclid
)
SELECT
  gclid,
  -- We try and find the earliest ga_client_id
  CASE
    WHEN _current.ga_client_id IS NULL
      OR _current.min_start_time IS NULL
      THEN _previous.ga_client_id
    WHEN _previous.ga_client_id IS NULL
      OR _previous.min_start_time IS NULL
      THEN _current.ga_client_id
    WHEN _previous.min_start_time <= _current.min_start_time
      THEN _previous.ga_client_id
    WHEN _current.min_start_time < _previous.min_start_time
      THEN _current.ga_client_id
    ELSE NULL
  END AS ga_client_id,
  -- Least and greatest return NULL if any input is NULL, so we coalesce each value first
  LEAST(
    COALESCE(_previous.min_start_time, _current.min_start_time),
    COALESCE(_current.min_start_time, _previous.min_start_time)
  ) AS min_start_time,
  LEAST(
    COALESCE(_previous.first_seen_date, _current.first_seen_date),
    COALESCE(_current.first_seen_date, _previous.first_seen_date)
  ) AS first_seen_date,
  GREATEST(
    COALESCE(_previous.last_seen_date, _current.last_seen_date),
    COALESCE(_current.last_seen_date, _previous.last_seen_date)
  ) AS last_seen_date,
FROM
  history AS _previous
FULL OUTER JOIN
  new_data AS _current
USING
  (gclid)
