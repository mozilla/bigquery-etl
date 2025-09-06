SELECT
  * REPLACE ("*" AS event_extra_key, MAX(total_events) AS total_events)
FROM
  `moz-fx-data-shared-prod.monitoring_derived.event_monitoring_aggregates_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  channel,
  version,
  experiment,
  experiment_branch
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.event_monitoring_aggregates_v1`
WHERE
  submission_date = @submission_date
