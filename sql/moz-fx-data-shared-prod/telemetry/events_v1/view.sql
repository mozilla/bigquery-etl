-- Creates a union view to provide continuity between legacy parquet events view and new
-- all-gcp events tables
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.events_v1`
AS
WITH parquet_events AS (
  SELECT
    * EXCEPT (
      -- These are old fields that were removed in the parquet job but resurfaced in the BQ import
      e10s_enabled,
      e10s_cohort,
      subsession_start_date,
      subsession_length,
      sync_configured,
      sync_count_desktop,
      sync_count_mobile,
      active_experiment_id,
      active_experiment_branch
    ) REPLACE(
      event_map_values.key_value AS event_map_values,
      -- experiments struct in new events includes enrollment_id and type subfields
      (
        SELECT
          ARRAY_AGG(
            STRUCT(
              e.key AS key,
              STRUCT(
                e.value AS branch,
                CAST(NULL AS string) AS enrollment_id,
                CAST(NULL AS string) AS `type`
              ) AS value
            )
          )
        FROM
          UNNEST(experiments.key_value) e
      ) AS experiments,
      -- Bug 1525620: fix inconsistencies in timestamp resolution for main vs event events
      CASE
      WHEN
        doc_type = 'main'
      THEN
        SAFE.TIMESTAMP_MICROS(CAST(`timestamp` / 1000 AS INT64))
      ELSE
        SAFE.TIMESTAMP_SECONDS(`timestamp`)
      END
      AS `timestamp`,
      SAFE.TIMESTAMP_MILLIS(session_start_time) AS session_start_time
    ),
    CAST(NULL AS STRING) AS build_id
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.events_v1`
  WHERE
    submission_date < '2019-10-31'
),
main_events AS (
  SELECT
    submission_date,
    'main' AS doc_type,
    document_id,
    client_id,
    normalized_channel,
    country,
    locale,
    app_name,
    app_version,
    os,
    os_version,
    `timestamp`,
    sample_id,
    event_timestamp,
    event_category,
    event_method,
    event_object,
    event_string_value,
    event_map_values,
    experiments,
    event_process,
    subsession_id,
    session_start_time,
    session_id,
    build_id
  FROM
    `moz-fx-data-shared-prod`.telemetry_derived.main_events_v1
  WHERE
    submission_date >= '2019-10-31'
),
event_events AS (
  SELECT
    submission_date,
    'event' AS doc_type,
    document_id,
    client_id,
    normalized_channel,
    country,
    locale,
    app_name,
    app_version,
    os,
    os_version,
    `timestamp`,
    sample_id,
    event_timestamp,
    event_category,
    event_method,
    event_object,
    event_string_value,
    event_map_values,
    experiments,
    event_process,
    subsession_id,
    session_start_time,
    session_id,
    build_id
  FROM
    `moz-fx-data-shared-prod`.telemetry_derived.event_events_v1
  WHERE
    submission_date >= '2019-10-31'
)
SELECT
  *
FROM
  parquet_events
UNION ALL
SELECT
  *
FROM
  main_events
UNION ALL
SELECT
  *
FROM
  event_events
