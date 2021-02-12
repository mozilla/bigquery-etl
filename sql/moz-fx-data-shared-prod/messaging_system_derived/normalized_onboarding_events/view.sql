CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.messaging_system_derived.normalized_onboarding_events`
AS
SELECT
  DATE(submission_timestamp) AS submission_date,
  submission_timestamp AS timestamp,
  version AS app_version,
  message_id AS category,
  ARRAY(
    SELECT AS STRUCT key, value
    FROM UNNEST([
      STRUCT('page' AS key, JSON_EXTRACT_SCALAR(event_context, '$.page') AS value),
      STRUCT('source' AS key, JSON_EXTRACT_SCALAR(event_context, '$.source') AS value),
      STRUCT('domState' AS key, JSON_EXTRACT_SCALAR(event_context, '$.domState') AS value),
      STRUCT('reason' AS key, JSON_EXTRACT_SCALAR(event_context, '$.reason') AS value),
      STRUCT('display' AS key, JSON_EXTRACT_SCALAR(event_context, '$.display') AS value)
    ])
    WHERE value IS NOT NULL
  ) AS extra,
  (
    SELECT
      ARRAY_AGG(STRUCT(key, value.branch AS value))
    FROM
      UNNEST(experiments)
  ) AS experiments,
  * EXCEPT (experiments)
FROM
  `moz-fx-data-shared-prod.messaging_system_stable.onboarding_v1`
