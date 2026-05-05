--exclude significantly over-active clients
WITH overactive AS (
  SELECT
    client_id,
    COUNT(1) AS nbr_events
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
  HAVING
    COUNT(1) > 10000000
)
SELECT
  e.submission_date,
  e.sample_id,
  e.client_id,
  COUNT(*) AS n_logged_event,
  COUNTIF(
    e.event_category = 'pictureinpicture'
    AND e.event_method = 'create'
  ) AS n_created_pictureinpicture,
  COUNTIF(
    e.event_category = 'security.ui.protections'
    AND e.event_object = 'protection_report'
  ) AS n_viewed_protection_report,
  mozfun.stats.mode_last(ARRAY_AGG(e.profile_group_id ORDER BY `timestamp`)) AS profile_group_id,
FROM
  `moz-fx-data-shared-prod.telemetry.events` e
LEFT JOIN
  overactive o
  ON e.client_id = o.client_id
WHERE
  e.submission_date = @submission_date
  AND o.client_id IS NULL
GROUP BY
  e.submission_date,
  e.sample_id,
  e.client_id
