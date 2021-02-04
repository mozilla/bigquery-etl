-- creates a pre-filtered main ping dataset for monitoring
-- and ad-hoc analyses on fission experiment
WITH main_pings AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.telemetry.main
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_channel = 'nightly'
    AND mozfun.map.get_key(
      environment.experiments,
      "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
    ) IS NOT NULL
),
first_sessions AS (
    -- these are the sessions where clients were enrolled in the experiment
    -- since fission is enabled after restart, we want to filter them out from the main pings
  SELECT
    session_id
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    event_category = 'normandy'
    AND normalized_channel = 'nightly'
    AND event_method = 'enroll'
    AND submission_date = @submission_date
    AND event_string_value = 'bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100'
)
SELECT
  m.*
FROM
  main_pings m
LEFT JOIN
  first_sessions e
ON
  m.payload.info.session_id = e.session_id
WHERE
  e.session_id IS NULL
