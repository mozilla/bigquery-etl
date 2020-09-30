CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.events_live`
AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    document_id,
    client_id,
    normalized_channel,
    normalized_country_code AS country,
    environment.settings.locale AS locale,
    normalized_app_name AS app_name,
    metadata.uri.app_version AS app_version,
    environment.build.build_id AS build_id,
    normalized_os AS os,
    normalized_os_version AS os_version,
    environment.experiments AS experiments,
    sample_id,
    payload.session_id AS session_id,
    SAFE.TIMESTAMP_MILLIS(payload.process_start_timestamp) AS session_start_time,
    payload.subsession_id AS subsession_id,
    submission_timestamp AS `timestamp`,
    `moz-fx-data-shared-prod`.udf.deanonymize_event(e).*,
    event_process
  FROM
    `moz-fx-data-shared-prod.telemetry_live.event_v4`
  CROSS JOIN
    UNNEST(
      [
        STRUCT(
          "content" AS event_process,
          payload.events.content AS events
        ),
        ("dynamic", payload.events.dynamic),
        ("extension", payload.events.extension),
        ("gpu", payload.events.gpu),
        ("parent", payload.events.parent)
      ]
    )
  CROSS JOIN
    UNNEST(events) AS e
)
