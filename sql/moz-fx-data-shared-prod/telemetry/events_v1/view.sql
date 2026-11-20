-- Creates a union view to combine events from the main and event pings
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.events_v1`
AS
WITH main_events AS (
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
    build_id,
    `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
    profile_group_id
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
    build_id,
    `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
    profile_group_id
  FROM
    `moz-fx-data-shared-prod`.telemetry_derived.event_events_v1
  WHERE
    submission_date >= '2019-10-31'
)
SELECT
  *
FROM
  main_events
UNION ALL
SELECT
  *
FROM
  event_events
