#warn
-- check if we're receiving the same number of events in backend `accounts_events` and `events` pings
-- this is a temporary check that will be removed once migration is completed (https://mozilla-hub.atlassian.net/browse/DENG-2407)
WITH events_new AS (
  SELECT
    DATE(e.submission_timestamp) AS day,
    CONCAT(event.category, "_", event.name) AS event_name,
    COUNT(*) AS count_new
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.events_v1` AS e
  CROSS JOIN
    UNNEST(e.events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    DATE(e.submission_timestamp),
    CONCAT(event.category, "_", event.name)
),
events_old AS (
  SELECT
    DATE(submission_timestamp) AS day,
    metrics.string.event_name AS event_name,
    COUNT(*) AS count_old
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.accounts_events_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    DATE(submission_timestamp),
    metrics.string.event_name
),
check_results AS (
  SELECT
    *,
    events_new.count_new - events_old.count_old AS count_diff
  FROM
    events_new
  FULL OUTER JOIN
    events_old
    USING (day, event_name)
  WHERE
    events_new.count_new IS NULL
    OR events_old.count_old IS NULL
    OR (
      (event_name NOT LIKE 'access_token_%' AND events_new.count_new - events_old.count_old > 1)
    -- access_token_checked is sent frequently, 300M per day and due to small time differences some events might end up in a different day's parition
      OR (event_name LIKE 'access_token_%' AND events_new.count_new - events_old.count_old > 50)
    )
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR('Events count mismatch between backend accounts_events and events pings'),
    NULL
  )
FROM
  check_results;

#warn
-- check if we're receiving the same number of events in frontend `accounts_events` and `events` pings
WITH events_new AS (
  SELECT
    DATE(e.submission_timestamp) AS day,
    CONCAT(event.category, "_", event.name) AS event_name,
    COUNT(*) AS count_new
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.events_v1` AS e
  CROSS JOIN
    UNNEST(e.events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    DATE(e.submission_timestamp),
    CONCAT(event.category, "_", event.name)
),
events_old AS (
  SELECT
    DATE(submission_timestamp) AS day,
    metrics.string.event_name AS event_name,
    COUNT(*) AS count_old
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.accounts_events_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    DATE(submission_timestamp),
    metrics.string.event_name
),
check_results AS (
  SELECT
    *,
    events_new.count_new - events_old.count_old AS count_diff,
    ABS(events_new.count_new - events_old.count_old) / LEAST(
      events_new.count_new,
      events_old.count_old
    ) AS diff_ratio
  FROM
    events_new
  FULL OUTER JOIN
    events_old
    USING (day, event_name)
  WHERE
    event_name IS NOT NULL
  -- these were recently added in https://mozilla-hub.atlassian.net/browse/FXA-9978
  -- will be removed from here when this lands in production
    AND event_name NOT IN (
      'login_backup_code_submit',
      'login_backup_code_success_view',
      'login_backup_code_view'
    )
    AND (
      (events_new.count_new IS NULL AND events_old.count_old > 1) -- ignore erroneous event names
      OR (events_old.count_old IS NULL AND events_new.count_new > 1)
      OR ABS(events_new.count_new - events_old.count_old) / LEAST(
        events_new.count_new,
        events_old.count_old
      ) > 0.01
    )
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR('Events count mismatch between frontend accounts_events and events pings'),
    NULL
  )
FROM
  check_results;
