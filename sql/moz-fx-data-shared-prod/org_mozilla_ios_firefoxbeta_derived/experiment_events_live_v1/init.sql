-- Generated via ./bqetl generate experiment_monitoring
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS org_mozilla_ios_firefoxbeta_derived.experiment_events_live_v1
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  -- Glean apps use Nimbus for experimentation
  WITH all_events AS (
    SELECT
      submission_timestamp,
      events
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_live.events_v1`
  ),
  experiment_events AS (
    SELECT
      submission_timestamp AS `timestamp`,
      event.category AS `type`,
      CAST(event.extra[SAFE_OFFSET(i)].value AS STRING) AS branch,
      CAST(event.extra[SAFE_OFFSET(j)].value AS STRING) AS experiment,
      event.name AS event_method
    FROM
      all_events,
      UNNEST(events) AS event,
      -- Workaround for https://issuetracker.google.com/issues/182829918
      -- To prevent having the branch name set to the experiment slug,
      -- the number of generated array indices needs to be different.
      UNNEST(GENERATE_ARRAY(0, 50)) AS i,
      UNNEST(GENERATE_ARRAY(0, 51)) AS j
    WHERE
      event.category = 'nimbus_events'
      AND CAST(event.extra[SAFE_OFFSET(i)].key AS STRING) = 'branch'
      AND CAST(event.extra[SAFE_OFFSET(j)].key AS STRING) = 'experiment'
  )
  SELECT
    DATE(`timestamp`) AS submission_date,
    `type`,
    experiment,
    branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(`timestamp`, HOUR),
    -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(`timestamp`, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
    ) AS window_end,
    COUNTIF(event_method = 'enroll' OR event_method = 'enrollment') AS enroll_count,
    COUNTIF(event_method = 'unenroll' OR event_method = 'unenrollment') AS unenroll_count,
    COUNTIF(event_method = 'graduate') AS graduate_count,
    COUNTIF(event_method = 'update') AS update_count,
    COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
    COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
    COUNTIF(event_method = 'updateFailed') AS update_failed_count,
    COUNTIF(event_method = 'disqualification') AS disqualification_count,
    COUNTIF(event_method = 'expose' OR event_method = 'exposure') AS exposure_count,
    COUNTIF(event_method = 'validationFailed') AS validation_failed_count
  FROM
    experiment_events
  WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
    timestamp > TIMESTAMP('2023-10-10')
  GROUP BY
    submission_date,
    `type`,
    experiment,
    branch,
    window_start,
    window_end
