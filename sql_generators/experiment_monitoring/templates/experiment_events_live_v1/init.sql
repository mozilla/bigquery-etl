-- Generated via ./bqetl generate experiment_monitoring
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS {{ dataset }}_derived.{{ destination_table }}
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  {% if "_cirrus" in dataset %}
  -- Cirrus apps uses specialized Glean structure per events
  WITH all_events AS (
    SELECT
      submission_timestamp,
      events
    FROM
       `moz-fx-data-shared-prod.{{ dataset }}_stable.enrollment_v1`
  ),
  experiment_events AS (
    SELECT
      submission_timestamp AS `timestamp`,
      event.category AS `type`,
      CAST(event.extra[safe_offset(i)].value AS STRING) AS branch,
      CAST(event.extra[safe_offset(j)].value AS STRING) AS experiment,
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
      event.category = 'cirrus_events'
      AND CAST(event.extra[safe_offset(i)].key AS STRING) = 'branch'
      AND CAST(event.extra[safe_offset(j)].key AS STRING) = 'experiment'
  )
  {% elif dataset != "telemetry" %}
  -- Glean apps use Nimbus for experimentation
  WITH all_events AS (
    SELECT
      submission_timestamp,
      events
    FROM
      `moz-fx-data-shared-prod.{{ dataset }}_live.events_v1`
  ),
  experiment_events AS (
    SELECT
      submission_timestamp AS `timestamp`,
      event.category AS `type`,
      CAST(event.extra[safe_offset(i)].value AS STRING) AS branch,
      CAST(event.extra[safe_offset(j)].value AS STRING) AS experiment,
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
      AND CAST(event.extra[safe_offset(i)].key AS STRING) = 'branch'
      AND CAST(event.extra[safe_offset(j)].key AS STRING) = 'experiment'
  )

  {% else %}
  -- Non-Glean apps might use Normandy and Nimbus for experimentation
  WITH experiment_events AS (
    SELECT
      submission_timestamp AS timestamp,
      event.f2_ AS event_method,
      event.f3_ AS `type`,
      event.f4_ AS experiment,
      IF(event_map_value.key = 'branch', event_map_value.value, NULL) AS branch
    FROM
      `moz-fx-data-shared-prod.{{ dataset }}_live.event_v4`
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
  {% endif %}
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
    timestamp > TIMESTAMP('{{ start_date }}')
  GROUP BY
    submission_date,
    `type`,
    experiment,
    branch,
    window_start,
    window_end
