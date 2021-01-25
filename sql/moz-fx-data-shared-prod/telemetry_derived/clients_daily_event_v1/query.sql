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
  submission_date,
  sample_id,
  client_id
