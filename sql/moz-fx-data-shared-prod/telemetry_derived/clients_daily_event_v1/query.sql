WITH firefox_suggest_events AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    ARRAY_AGG(event_object ORDER BY session_start_time DESC)[OFFSET(0)] AS firefoxsuggest_optin,
  FROM
    telemetry.events
  WHERE
    submission_date = @submission_date
  AND 
    event_category = 'contextservices.quicksuggest'
  AND 
    event_method = 'opt_in_dialog'
  GROUP BY
    1,
    2,
    3
),
telemetry_events AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    COUNT(*) AS n_logged_event,
    COUNTIF(
      event_category = 'pictureinpicture'
      AND event_method = 'create'
    ) AS n_created_pictureinpicture,
    COUNTIF(
      event_category = 'security.ui.protections'
      AND event_object = 'protection_report'
    ) AS n_viewed_protection_report,
  FROM
    telemetry.events
  WHERE
    submission_date = @submission_date
  GROUP BY
    1,
    2,
    3
)
SELECT
  submission_date,
  sample_id,
  client_id,
  n_logged_event,
  n_created_pictureinpicture,
  n_viewed_protection_report,
  firefoxsuggest_optin,
FROM
  telemetry_events
LEFT JOIN 
  firefox_suggest_events
USING
  ( submission_date, sample_id, client_id )
