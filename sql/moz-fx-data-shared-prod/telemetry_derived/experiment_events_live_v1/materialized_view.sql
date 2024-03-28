-- Generated via ./bqetl generate experiment_monitoring
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS telemetry_derived.experiment_events_live_v1
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  -- Non-Glean apps might use Normandy and Nimbus for experimentation
  WITH experiment_events AS (
    SELECT
      submission_timestamp AS timestamp,
      event.f2_ AS event_method,
      event.f3_ AS `type`,
      event.f4_ AS experiment,
      IF(event_map_value.key = 'branch', event_map_value.value, NULL) AS branch
    FROM
      `moz-fx-data-shared-prod.telemetry_live.event_v4`
    CROSS JOIN
      UNNEST(
        ARRAY_CONCAT(
          payload.events.parent,
          payload.events.content,
          payload.events.dynamic,
          payload.events.extension,
          payload.events.gpu
        )
      ) AS event
    CROSS JOIN
      UNNEST(event.f5_) AS event_map_value
    WHERE
      event.f1_ = 'normandy'
      AND (
        (event_map_value.key = 'branch' AND event.f3_ = 'preference_study')
        OR (
          (event.f3_ = 'addon_study' OR event.f3_ = 'preference_rollout')
          AND event_map_value.key = 'enrollmentId'
        )
        OR event.f3_ = 'nimbus_experiment'
        OR event.f2_ = 'enrollFailed'
        OR event.f2_ = 'unenrollFailed'
      )
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
