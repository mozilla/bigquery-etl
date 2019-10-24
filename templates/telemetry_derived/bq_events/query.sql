-- Creates a backwards-compatible flattened events dataset created from gcp-ingested pings
-- Designed to provide continuity for the `events` dataset via a user-facing union view
WITH event_events AS (
  SELECT
    *,
    e,
    "content" AS event_process
  FROM
    telemetry.event
  CROSS JOIN
    UNNEST(payload.events.content) AS e
  UNION ALL
  SELECT
    *,
    e,
    "dynamic" AS event_process
  FROM
    telemetry.event
  CROSS JOIN
    UNNEST(payload.events.dynamic) AS e
  UNION ALL
  SELECT
    *,
    e,
    "extension" AS event_process
  FROM
    telemetry.event
  CROSS JOIN
    UNNEST(payload.events.extension) AS e
  UNION ALL
  SELECT
    *,
    e,
    "gpu" AS event_process
  FROM
    telemetry.event
  CROSS JOIN
    UNNEST(payload.events.gpu) AS e
  UNION ALL
  SELECT
    *,
    e,
    "parent" AS event_process
  FROM
    telemetry.event
  CROSS JOIN
    UNNEST(payload.events.parent) AS e
),
main_events AS (
  -- While there are more "events" fields under other process in the main ping schema,
  -- events were moved out to the event ping before those other processes were added. This is
  -- an exhaustive list of processes in which we'd expect to see events in main pings
  SELECT
    *,
    e,
    "content" AS event_process
  FROM
    telemetry.main
  CROSS JOIN
    UNNEST(payload.processes.content.events) AS e
  UNION ALL
  SELECT
    *,
    e,
    "dynamic" AS event_process
  FROM
    telemetry.main
  CROSS JOIN
    UNNEST(payload.processes.dynamic.events) AS e
  UNION ALL
  SELECT
    *,
    e,
    "gpu" AS event_process
  FROM
    telemetry.main
  CROSS JOIN
    UNNEST(payload.processes.gpu.events) AS e
  UNION ALL
  SELECT
    *,
    e,
    "parent" AS event_process
  FROM
    telemetry.main
  CROSS JOIN
    UNNEST(payload.processes.parent.events) AS e
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  document_id,
  client_id,
  normalized_channel,
  normalized_country_code AS country,
  environment.settings.locale AS locale,
  normalized_app_name AS app_name,
  metadata.uri.app_version AS app_version,
  normalized_os AS os,
  normalized_os_version AS os_version,
  environment.experiments AS experiments,
  sample_id,
  payload.session_id AS session_id,
  SAFE.TIMESTAMP_MILLIS(payload.process_start_timestamp) AS session_start_time,
  payload.subsession_id AS subsession_id,
  submission_timestamp AS timestamp,
  udf.deanonymize_event(e).*,
  event_process,
  "event" AS doc_type
FROM
  event_events
WHERE
  submission_date = @submission_date
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  document_id,
  client_id,
  normalized_channel,
  normalized_country_code AS country,
  environment.settings.locale AS locale,
  normalized_app_name AS app_name,
  metadata.uri.app_version AS app_version,
  normalized_os AS os,
  normalized_os_version AS os_version,
  environment.experiments AS experiments,
  sample_id,
  payload.info.session_id AS session_id,
  SAFE.PARSE_TIMESTAMP(
    "%FT%H:%M:%S.0%Ez",
    payload.info.session_start_date
  ) AS session_start_time,
  payload.info.subsession_id AS subsession_id,
  submission_timestamp AS timestamp,
  udf.deanonymize_event(e).*,
  event_process "main" AS doc_type
FROM
  main_events
WHERE
  submission_date = @submission_date
