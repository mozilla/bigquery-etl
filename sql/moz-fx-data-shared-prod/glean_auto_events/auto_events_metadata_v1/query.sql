WITH events AS (
  SELECT
    DATE(submission_timestamp) as submission_date,
    event,
    JSON_VALUE(event_extra.id) AS element_id,
    COUNT(*) AS count
  FROM moz-fx-data-shared-prod.accounts_frontend.events_stream as e
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'glean.element_click'
  GROUP BY submission_date, event, element_id
)
SELECT
  submission_date,
  "accounts_frontend" AS app,
  CONCAT(event, '.', events.element_id) name,
  count
FROM events
