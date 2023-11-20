WITH historical_clients AS (
  SELECT
    *
  FROM
    mozilla_org_derived.ga_clients_v1
),
new_clients AS (
  SELECT
    ga_client_id,
    MIN(session_date) AS first_seen_date,
    MAX(session_date) AS last_seen_date,
    LOGICAL_OR(had_download_event) AS had_download_event,
    STRUCT(
      /* Geos */
      MIN_BY(country, session_number) AS country,
      MIN_BY(region, session_number) AS region,
      MIN_BY(city, session_number) AS city,
      /* Attribution */
      MIN_BY(campaign_id, session_number) AS campaign_id,
      MIN_BY(campaign, session_number) AS campaign,
      MIN_BY(source, session_number) AS source,
      MIN_BY(medium, session_number) AS medium,
      MIN_BY(term, session_number) AS term,
      MIN_BY(content, session_number) AS content,
      /* Device */
      MIN_BY(device_category, session_number) AS device_category,
      MIN_BY(mobile_device_model, session_number) AS mobile_device_model,
      MIN_BY(mobile_device_string, session_number) AS mobile_device_string,
      MIN_BY(os, session_number) AS os,
      MIN_BY(os_version, session_number) AS os_version,
      MIN_BY(LANGUAGE, session_number) AS language,
      MIN_BY(browser, session_number) AS browser,
      MIN_BY(browser_version, session_number) AS browser_version,
      /* Session */
      MIN_BY(ga_session_id, session_number) AS ga_session_id
    ) AS first_reported,
  FROM
    mozilla_org_derived.ga_sessions_v1
  WHERE
    ga_client_id IS NOT NULL
    -- Re-process three days, to account for late-arriving data
    AND session_date
    BETWEEN DATE_SUB(@session_date, INTERVAL 3 DAY)
    AND @session_date
  GROUP BY
    ga_client_id
)
SELECT
  ga_client_id,
  -- Least and greatest return NULL if any input is NULL, so we coalesce each value first
  LEAST(
    COALESCE(_previous.first_seen_date, _current.first_seen_date),
    COALESCE(_current.first_seen_date, _previous.first_seen_date)
  ) AS first_seen_date,
  GREATEST(
    COALESCE(_previous.last_seen_date, _current.last_seen_date),
    COALESCE(_current.last_seen_date, _previous.last_seen_date)
  ) AS last_seen_date,
  -- OR treats NULL differently for True/False. If any input is True, it will return True; otherwise it returns NULL.
  COALESCE(
    _previous.had_download_event
    OR _current.had_download_event,
    FALSE
  ) AS had_download_event,
  -- We take the current data for first_reported only if we don't already have first_reported data, or if we're backfilling
  -- If equal, prefer new data
  IF(
    _previous.ga_client_id IS NULL
    OR _current.first_seen_date <= _previous.first_seen_date,
    _current.first_reported,
    _previous.first_reported
  ) AS first_reported,
FROM
  historical_clients AS _previous
FULL OUTER JOIN
  new_clients AS _current
USING
  (ga_client_id)
