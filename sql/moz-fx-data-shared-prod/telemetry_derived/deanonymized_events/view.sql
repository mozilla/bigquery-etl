CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.telemetry_derived.deanonymized_events
AS
WITH events AS (
  SELECT
    `moz-fx-data-shared-prod`.udf.deanonymize_event(e).*,
    *
  FROM
    `moz-fx-data-shared-prod`.telemetry_stable.event_v4
  LEFT JOIN
    UNNEST(
      [
        STRUCT("content" AS process, payload.events.content AS events),
        ("dynamic", payload.events.dynamic),
        ("extension", payload.events.extension),
        ("gpu", payload.events.gpu),
        ("parent", payload.events.parent)
      ]
    )
  LEFT JOIN
    UNNEST(events) AS e
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  event_category AS category,
  CONCAT(event_method, '.', event_object) AS event,
  ARRAY_CONCAT(
    event_map_values,
    IF(
      event_string_value IS NOT NULL,
      [STRUCT('event_value' AS key, event_string_value AS value)],
      []
    )
  ) AS extra,
  SAFE.TIMESTAMP_ADD(
    SAFE.TIMESTAMP_MILLIS(payload.process_start_timestamp),
    INTERVAL event_timestamp MILLISECOND
  ) AS timestamp,
  (
    SELECT
      ARRAY_AGG(STRUCT(key, value.branch AS value))
    FROM
      UNNEST(environment.experiments)
  ) AS experiments,
  *
FROM
  events
